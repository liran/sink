package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type batcherSubmission struct {
	request  int
	response int
	err      error
}

func TestRequestBatcherAggregatesConcurrentSubmissions(t *testing.T) {
	const submissionCount = 32
	var executions atomic.Int64
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		executions.Add(1)
		for _, call := range calls {
			completeCall(call, call.request*2, nil)
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Second,
		MaxOperations:       submissionCount,
		MaxBytes:            1 << 20,
		MaxQueuedOperations: submissionCount,
		MaxQueuedBytes:      1 << 20,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)
	defer batcher.Close()

	start := make(chan struct{})
	results := make(chan batcherSubmission, submissionCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(submissionCount)
	for request := range submissionCount {
		go func() {
			defer waitGroup.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := batcher.Submit(ctx, request, 1, 1)
			result := batcherSubmission{request: request, response: response, err: err}
			results <- result
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			t.Fatalf("Submit(%d) error = %v", result.request, result.err)
		}
		if result.response != result.request*2 {
			t.Fatalf("Submit(%d) response = %d", result.request, result.response)
		}
	}
	if executions.Load() != 1 {
		t.Fatalf("batch executions = %d, want 1", executions.Load())
	}
}

func TestRequestBatcherRejectsWhenQueueIsFull(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var executions atomic.Int64
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		if executions.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Second,
		MaxOperations:       1,
		MaxBytes:            1024,
		MaxQueuedOperations: 1,
		MaxQueuedBytes:      1024,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)
	defer batcher.Close()

	results := make(chan batcherSubmission, 2)
	go submitBatcherTestRequest(batcher, 1, results)
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first batch did not start")
	}
	go submitBatcherTestRequest(batcher, 2, results)
	waitForQueuedCalls(t, batcher, 1)

	_, err := batcher.Submit(t.Context(), 3, 1, 1)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Submit() error = %v, want ResourceExhausted", err)
	}
	close(releaseFirst)
	for range 2 {
		result := <-results
		if result.err != nil || result.response != result.request {
			t.Fatalf("queued Submit() = %+v", result)
		}
	}
}

func TestRequestBatcherEnforcesQueuedByteLimit(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var executions atomic.Int64
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		if executions.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Second,
		MaxOperations:       1,
		MaxBytes:            2,
		MaxQueuedOperations: 10,
		MaxQueuedBytes:      2,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)
	defer batcher.Close()

	results := make(chan batcherSubmission, 2)
	go submitBatcherTestRequestWithSize(batcher, 1, 1, results)
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first batch did not start")
	}
	go submitBatcherTestRequestWithSize(batcher, 2, 2, results)
	waitForQueuedCalls(t, batcher, 1)

	_, err := batcher.Submit(t.Context(), 3, 1, 1)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Submit() error = %v, want ResourceExhausted", err)
	}
	close(releaseFirst)
	for range 2 {
		result := <-results
		if result.err != nil || result.response != result.request {
			t.Fatalf("queued Submit() = %+v", result)
		}
	}
}

func TestRequestBatcherSkipsCanceledCallsAndClosesPromptly(t *testing.T) {
	var executions atomic.Int64
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		executions.Add(1)
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Hour,
		MaxOperations:       2,
		MaxBytes:            1024,
		MaxQueuedOperations: 2,
		MaxQueuedBytes:      1024,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := batcher.Submit(ctx, 1, 1, 1)
		result <- err
	}()
	waitForQueuedCalls(t, batcher, 1)
	cancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("Submit() error = %v, want Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Submit() did not return")
	}

	closed := make(chan struct{})
	go func() {
		batcher.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() waited for the batch timer")
	}
	if executions.Load() != 0 {
		t.Fatalf("batch executions = %d, want 0", executions.Load())
	}
	_, err := batcher.Submit(t.Context(), 2, 1, 1)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Submit() after Close error = %v, want Unavailable", err)
	}
}

func TestRequestBatcherCallerCancellationDoesNotCancelOtherCalls(t *testing.T) {
	started := make(chan struct{})
	execute := func(ctx context.Context, calls []*batchCall[int, int]) {
		close(started)
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			for _, call := range calls {
				completeCall(call, call.request, nil)
			}
		case <-ctx.Done():
			for _, call := range calls {
				completeCall(call, 0, contextError(ctx))
			}
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Second,
		MaxOperations:       2,
		MaxBytes:            1024,
		MaxQueuedOperations: 2,
		MaxQueuedBytes:      1024,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)
	defer batcher.Close()

	shortContext, cancelShort := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelShort()
	longContext, cancelLong := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelLong()
	results := make(chan batcherSubmission, 2)
	go func() {
		response, err := batcher.Submit(shortContext, 1, 1, 1)
		result := batcherSubmission{request: 1, response: response, err: err}
		results <- result
	}()
	go func() {
		response, err := batcher.Submit(longContext, 2, 1, 1)
		result := batcherSubmission{request: 2, response: response, err: err}
		results <- result
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not start")
	}

	seen := make(map[int]batcherSubmission, 2)
	for range 2 {
		result := <-results
		seen[result.request] = result
	}
	if status.Code(seen[1].err) != codes.DeadlineExceeded {
		t.Fatalf("short Submit() = %+v, want DeadlineExceeded", seen[1])
	}
	if seen[2].err != nil || seen[2].response != 2 {
		t.Fatalf("long Submit() = %+v, want success", seen[2])
	}
}

func TestRequestBatcherConcurrentSubmitCancelAndClose(t *testing.T) {
	const submissionCount = 500
	executionStarted := make(chan struct{})
	releaseExecution := make(chan struct{})
	var startOnce sync.Once
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		startOnce.Do(func() { close(executionStarted) })
		<-releaseExecution
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	options := requestBatcherOptions[int, int]{
		Method:              "test",
		MaxWait:             time.Second,
		MaxOperations:       32,
		MaxBytes:            1 << 20,
		MaxQueuedOperations: submissionCount,
		MaxQueuedBytes:      1 << 20,
		Execute:             execute,
	}
	batcher := newRequestBatcher(options)

	start := make(chan struct{})
	results := make(chan error, submissionCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(submissionCount)
	for request := range submissionCount {
		go func() {
			defer waitGroup.Done()
			<-start
			ctx := context.Background()
			var cancel context.CancelFunc
			if request%3 == 0 {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if request%5 == 0 {
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
			}
			if cancel != nil {
				defer cancel()
			}
			_, err := batcher.Submit(ctx, request, 1, 1)
			results <- err
		}()
	}
	close(start)
	select {
	case <-executionStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("batch execution did not start")
	}
	waitForAnyQueuedCall(t, batcher)
	closed := make(chan struct{})
	go func() {
		batcher.Close()
		close(closed)
	}()
	close(releaseExecution)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not finish")
	}
	waitGroup.Wait()
	close(results)

	for err := range results {
		switch status.Code(err) {
		case codes.OK, codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		default:
			t.Fatalf("Submit() error = %v", err)
		}
	}
	if batcher.queuedCallCount() != 0 {
		t.Fatalf("queued calls after Close = %d", batcher.queuedCallCount())
	}
}

func submitBatcherTestRequest(
	batcher *requestBatcher[int, int],
	request int,
	results chan<- batcherSubmission,
) {
	submitBatcherTestRequestWithSize(batcher, request, 1, results)
}

func submitBatcherTestRequestWithSize(
	batcher *requestBatcher[int, int],
	request int,
	encodedBytes int,
	results chan<- batcherSubmission,
) {
	response, err := batcher.Submit(context.Background(), request, 1, encodedBytes)
	result := batcherSubmission{request: request, response: response, err: err}
	results <- result
}

func waitForQueuedCalls(t *testing.T, batcher *requestBatcher[int, int], wanted int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if batcher.queuedCallCount() == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued calls = %d, want %d", batcher.queuedCallCount(), wanted)
}

func waitForAnyQueuedCall(t *testing.T, batcher *requestBatcher[int, int]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if batcher.queuedCallCount() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no calls were queued")
}
