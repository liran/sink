package service

import (
	"context"
	"sync"
	"time"

	sinkmetrics "github.com/liran/sink/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type batchResult[Response any] struct {
	response Response
	err      error
}

type batchCall[Request any, Response any] struct {
	ctx            context.Context
	request        Request
	operationCount int
	encodedBytes   int
	enqueuedAt     time.Time
	result         chan batchResult[Response]
}

type requestBatcherOptions[Request any, Response any] struct {
	Method              string
	MaxWait             time.Duration
	MaxOperations       int
	MaxBytes            int
	MaxQueuedOperations int
	MaxQueuedBytes      int
	Execute             func(context.Context, []*batchCall[Request, Response])
	Metrics             *sinkmetrics.Metrics
}

type requestBatcher[Request any, Response any] struct {
	method              string
	maxWait             time.Duration
	maxOperations       int
	maxBytes            int
	maxQueuedOperations int
	maxQueuedBytes      int
	execute             func(context.Context, []*batchCall[Request, Response])
	metrics             *sinkmetrics.Metrics
	ctx                 context.Context
	cancel              context.CancelFunc
	input               chan *batchCall[Request, Response]
	waitGroup           sync.WaitGroup
	queueMu             sync.Mutex
	queuedCalls         int
	queuedOperations    int
	queuedBytes         int
}

func newRequestBatcher[Request any, Response any](opts requestBatcherOptions[Request, Response]) *requestBatcher[Request, Response] {
	ctx, cancel := context.WithCancel(context.Background())
	batcher := &requestBatcher[Request, Response]{
		method:              opts.Method,
		maxWait:             opts.MaxWait,
		maxOperations:       opts.MaxOperations,
		maxBytes:            opts.MaxBytes,
		maxQueuedOperations: opts.MaxQueuedOperations,
		maxQueuedBytes:      opts.MaxQueuedBytes,
		execute:             opts.Execute,
		metrics:             opts.Metrics,
		ctx:                 ctx,
		cancel:              cancel,
		input:               make(chan *batchCall[Request, Response], opts.MaxQueuedOperations),
	}
	batcher.waitGroup.Add(1)
	go batcher.run()
	return batcher
}

func (b *requestBatcher[Request, Response]) Submit(
	ctx context.Context,
	request Request,
	operationCount int,
	encodedBytes int,
) (Response, error) {
	var empty Response
	if err := contextError(ctx); err != nil {
		return empty, err
	}
	if err := b.reserve(operationCount, encodedBytes); err != nil {
		reason := "queue_full"
		if status.Code(err) == codes.Unavailable {
			reason = "shutdown"
		}
		b.metrics.ObserveBatchRejected(b.method, reason)
		return empty, err
	}

	call := &batchCall[Request, Response]{
		ctx:            ctx,
		request:        request,
		operationCount: operationCount,
		encodedBytes:   encodedBytes,
		enqueuedAt:     time.Now(),
		result:         make(chan batchResult[Response], 1),
	}
	select {
	case <-b.ctx.Done():
		b.release(call)
		return empty, status.Error(codes.Unavailable, "synchronous batcher is shutting down")
	default:
	}
	select {
	case b.input <- call:
	case <-ctx.Done():
		b.release(call)
		return empty, contextError(ctx)
	case <-b.ctx.Done():
		b.release(call)
		return empty, status.Error(codes.Unavailable, "synchronous batcher is shutting down")
	}

	select {
	case result := <-call.result:
		return result.response, result.err
	case <-ctx.Done():
		return empty, contextError(ctx)
	case <-b.ctx.Done():
		return empty, status.Error(codes.Unavailable, "synchronous batcher is shutting down")
	}
}

func (b *requestBatcher[Request, Response]) Close() {
	b.stop()
	b.wait()
}

func (b *requestBatcher[Request, Response]) stop() {
	b.cancel()
}

func (b *requestBatcher[Request, Response]) wait() {
	b.waitGroup.Wait()
}

func (b *requestBatcher[Request, Response]) reserve(operationCount int, encodedBytes int) error {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	if b.ctx.Err() != nil {
		return status.Error(codes.Unavailable, "synchronous batcher is shutting down")
	}
	if operationCount > b.maxQueuedOperations ||
		b.queuedOperations > b.maxQueuedOperations-operationCount ||
		encodedBytes > b.maxQueuedBytes || b.queuedBytes > b.maxQueuedBytes-encodedBytes {
		return status.Error(codes.ResourceExhausted, "synchronous batch queue is full")
	}
	b.queuedCalls++
	b.queuedOperations += operationCount
	b.queuedBytes += encodedBytes
	b.metrics.AdjustBatchQueue(b.method, operationCount, encodedBytes)
	return nil
}

func (b *requestBatcher[Request, Response]) release(call *batchCall[Request, Response]) {
	b.queueMu.Lock()
	b.queuedCalls--
	b.queuedOperations -= call.operationCount
	b.queuedBytes -= call.encodedBytes
	b.queueMu.Unlock()
	b.metrics.AdjustBatchQueue(b.method, -call.operationCount, -call.encodedBytes)
}

func (b *requestBatcher[Request, Response]) run() {
	defer b.waitGroup.Done()
	var carry *batchCall[Request, Response]
	for {
		first, ok := b.firstCall(carry)
		carry = nil
		if !ok {
			b.drain()
			return
		}
		if b.ctx.Err() != nil {
			b.failCalls([]*batchCall[Request, Response]{first})
			b.drain()
			return
		}
		if err := contextError(first.ctx); err != nil {
			b.release(first)
			completeCall(first, emptyResponse[Response](), err)
			continue
		}

		calls, next, reason, collecting := b.collect(first)
		if !collecting {
			b.failCalls(calls)
			if next != nil {
				b.failCalls([]*batchCall[Request, Response]{next})
			}
			b.drain()
			return
		}
		live := b.liveCalls(calls)
		if len(live) > 0 {
			b.executeBatch(live, reason)
		}
		carry = next
	}
}

func (b *requestBatcher[Request, Response]) firstCall(
	carry *batchCall[Request, Response],
) (*batchCall[Request, Response], bool) {
	if carry != nil {
		return carry, true
	}
	select {
	case call := <-b.input:
		return call, true
	case <-b.ctx.Done():
		return nil, false
	}
}

func (b *requestBatcher[Request, Response]) collect(
	first *batchCall[Request, Response],
) ([]*batchCall[Request, Response], *batchCall[Request, Response], string, bool) {
	calls := []*batchCall[Request, Response]{first}
	operations := first.operationCount
	encodedBytes := first.encodedBytes
	var timer *time.Timer
	defer func() {
		if timer != nil {
			stopTimer(timer)
		}
	}()
	for {
		if b.ctx.Err() != nil {
			return calls, nil, "shutdown", false
		}
		if operations >= b.maxOperations {
			return calls, nil, "max_operations", true
		}
		if encodedBytes >= b.maxBytes {
			return calls, nil, "max_bytes", true
		}
		if timer == nil {
			// Drain backlog before checking the wait deadline. Requests can age while
			// the previous batch executes and must still be coalesced instead of
			// degenerating into one storage call per queued RPC.
			select {
			case next := <-b.input:
				if err := contextError(next.ctx); err != nil {
					b.release(next)
					completeCall(next, emptyResponse[Response](), err)
					continue
				}
				if next.operationCount > b.maxOperations-operations {
					return calls, next, "max_operations", true
				}
				if next.encodedBytes > b.maxBytes-encodedBytes {
					return calls, next, "max_bytes", true
				}
				calls = append(calls, next)
				operations += next.operationCount
				encodedBytes += next.encodedBytes
				continue
			default:
			}
			remaining := b.maxWait - time.Since(first.enqueuedAt)
			if remaining <= 0 {
				return calls, nil, "max_wait", true
			}
			timer = time.NewTimer(remaining)
		}
		select {
		case next := <-b.input:
			if err := contextError(next.ctx); err != nil {
				b.release(next)
				completeCall(next, emptyResponse[Response](), err)
				continue
			}
			if next.operationCount > b.maxOperations-operations {
				return calls, next, "max_operations", true
			}
			if next.encodedBytes > b.maxBytes-encodedBytes {
				return calls, next, "max_bytes", true
			}
			calls = append(calls, next)
			operations += next.operationCount
			encodedBytes += next.encodedBytes
		case <-timer.C:
			return calls, nil, "max_wait", true
		case <-b.ctx.Done():
			return calls, nil, "shutdown", false
		}
	}
}

func (b *requestBatcher[Request, Response]) liveCalls(
	calls []*batchCall[Request, Response],
) []*batchCall[Request, Response] {
	live := make([]*batchCall[Request, Response], 0, len(calls))
	for _, call := range calls {
		b.release(call)
		if err := contextError(call.ctx); err != nil {
			completeCall(call, emptyResponse[Response](), err)
			continue
		}
		live = append(live, call)
	}
	return live
}

func (b *requestBatcher[Request, Response]) executeBatch(
	calls []*batchCall[Request, Response],
	reason string,
) {
	operationCount := 0
	encodedBytes := 0
	oldest := calls[0].enqueuedAt
	for _, call := range calls {
		operationCount += call.operationCount
		encodedBytes += call.encodedBytes
		if call.enqueuedAt.Before(oldest) {
			oldest = call.enqueuedAt
		}
	}
	executionContext, cancel := b.executionContext(calls)
	started := time.Now()
	b.execute(executionContext, calls)
	cancel()
	observation := sinkmetrics.BatchObservation{
		Method:            b.method,
		Reason:            reason,
		Operations:        operationCount,
		Bytes:             encodedBytes,
		QueueDuration:     started.Sub(oldest),
		ExecutionDuration: time.Since(started),
	}
	b.metrics.ObserveBatch(observation)
}

func (b *requestBatcher[Request, Response]) executionContext(
	calls []*batchCall[Request, Response],
) (context.Context, context.CancelFunc) {
	// One caller timing out must not cancel work still required by another
	// caller in the same batch. The latest deadline bounds shared execution;
	// any caller without a deadline leaves it bounded only by shutdown.
	var latest time.Time
	for _, call := range calls {
		deadline, ok := call.ctx.Deadline()
		if !ok {
			return context.WithCancel(b.ctx)
		}
		if deadline.After(latest) {
			latest = deadline
		}
	}
	return context.WithDeadline(b.ctx, latest)
}

func (b *requestBatcher[Request, Response]) failCalls(calls []*batchCall[Request, Response]) {
	err := status.Error(codes.Unavailable, "synchronous batcher is shutting down")
	for _, call := range calls {
		b.release(call)
		completeCall(call, emptyResponse[Response](), err)
	}
}

func (b *requestBatcher[Request, Response]) drain() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for b.queuedCallCount() > 0 {
		select {
		case call := <-b.input:
			b.failCalls([]*batchCall[Request, Response]{call})
		case <-ticker.C:
		}
	}
}

func (b *requestBatcher[Request, Response]) queuedCallCount() int {
	b.queueMu.Lock()
	count := b.queuedCalls
	b.queueMu.Unlock()
	return count
}

func completeCall[Request any, Response any](
	call *batchCall[Request, Response],
	response Response,
	err error,
) {
	result := batchResult[Response]{response: response, err: err}
	call.result <- result
}

func emptyResponse[Response any]() Response {
	var empty Response
	return empty
}

func contextError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	return status.FromContextError(err).Err()
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
