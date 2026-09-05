package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxPollRecords    = 500
	defaultMaxRetryAttempts  = 10
	defaultRetryBackoff      = 100 * time.Millisecond
	defaultMaxBackoff        = 10 * time.Second
	maxDeadLetterErrorBytes  = 4096
	defaultProcessingTimeout = 20 * time.Second
	defaultMaxRecordBytes    = 900 << 10
	kafkaRecordOverhead      = 16 << 10
)

type Handler interface {
	HandleBatch(ctx context.Context, mutations []queue.Mutation) []error
}

type WorkerOptions struct {
	Topics            *TopicManager
	Brokers           []string
	Store             string
	Topic             string
	GroupID           string
	DeadLetterTopic   string
	Handler           Handler
	MaxPollRecords    int
	MaxRetryAttempts  int
	RetryBackoff      time.Duration
	MaxRetryBackoff   time.Duration
	ClientOptions     []kgo.Opt
	Metrics           *sinkmetrics.Metrics
	ProcessingTimeout time.Duration
	MaxRecordBytes    int
}

type Worker struct {
	topics            *TopicManager
	client            *kgo.Client
	handler           Handler
	store             string
	deadLetterTopic   string
	maxPollRecords    int
	maxRetryAttempts  int
	retryBackoff      time.Duration
	maxRetryBackoff   time.Duration
	metrics           *sinkmetrics.Metrics
	processingTimeout time.Duration
	processingMu      sync.Mutex
	processingCancel  context.CancelFunc
	running           bool
	lastError         error
	offsetGap         bool
}

func NewWorker(opts WorkerOptions) (*Worker, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka worker: brokers are required")
	}
	if opts.Store == "" || opts.Topic == "" || opts.GroupID == "" || opts.DeadLetterTopic == "" {
		return nil, errors.New("create Kafka worker: store, topic, group ID, and dead-letter topic are required")
	}
	if opts.Handler == nil {
		return nil, errors.New("create Kafka worker: handler is required")
	}
	if opts.MaxPollRecords < 0 || opts.MaxRetryAttempts < 0 || opts.RetryBackoff < 0 || opts.MaxRetryBackoff < 0 {
		return nil, errors.New("create Kafka worker: limits cannot be negative")
	}

	maxPollRecords := opts.MaxPollRecords
	if maxPollRecords == 0 {
		maxPollRecords = defaultMaxPollRecords
	}
	maxRetryAttempts := opts.MaxRetryAttempts
	if maxRetryAttempts == 0 {
		maxRetryAttempts = defaultMaxRetryAttempts
	}
	retryBackoff := opts.RetryBackoff
	if retryBackoff == 0 {
		retryBackoff = defaultRetryBackoff
	}
	maxRetryBackoff := opts.MaxRetryBackoff
	if maxRetryBackoff == 0 {
		maxRetryBackoff = defaultMaxBackoff
	}
	if maxRetryBackoff < retryBackoff {
		return nil, errors.New("create Kafka worker: max retry backoff is less than retry backoff")
	}
	if opts.ProcessingTimeout < 0 || opts.ProcessingTimeout > 20*time.Second {
		return nil, errors.New("create Kafka worker: processing timeout must not exceed 20 seconds")
	}
	if opts.ProcessingTimeout == 0 {
		opts.ProcessingTimeout = defaultProcessingTimeout
	}
	if opts.MaxRecordBytes < 0 || opts.MaxRecordBytes > 64<<20 {
		return nil, errors.New("create Kafka worker: record byte limit must be between 1 and 64 MiB")
	}
	if opts.MaxRecordBytes == 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	worker := &Worker{
		topics:            opts.Topics,
		handler:           opts.Handler,
		store:             opts.Store,
		deadLetterTopic:   opts.DeadLetterTopic,
		maxPollRecords:    maxPollRecords,
		maxRetryAttempts:  maxRetryAttempts,
		retryBackoff:      retryBackoff,
		maxRetryBackoff:   maxRetryBackoff,
		metrics:           opts.Metrics,
		processingTimeout: opts.ProcessingTimeout,
	}

	clientOptions := append([]kgo.Opt(nil), opts.ClientOptions...)
	requiredOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumeTopics(opts.Topic),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()),
		kgo.RebalanceTimeout(60 * time.Second),
		kgo.OnPartitionsCallbackBlocked(worker.rebalanceRequested),
		kgo.OnPartitionsRevoked(worker.partitionsReleased),
		kgo.OnPartitionsLost(worker.partitionsReleased),
		kgo.MaxBufferedBytes(max(64<<20, opts.MaxRecordBytes+kafkaRecordOverhead)),
		kgo.FetchMaxBytes(16 << 20),
		kgo.FetchMaxPartitionBytes(int32(opts.MaxRecordBytes + kafkaRecordOverhead)),
		kgo.ProducerBatchMaxBytes(int32(opts.MaxRecordBytes + kafkaRecordOverhead)),
		kgo.RecordDeliveryTimeout(15 * time.Second),
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	worker.client = client
	worker.metrics.SetWorkerOffsetGap(worker.store, false)
	return worker, nil
}

func (w *Worker) Run(ctx context.Context) error {
	w.processingMu.Lock()
	w.running = true
	w.processingMu.Unlock()
	defer func() {
		w.processingMu.Lock()
		w.running = false
		w.processingMu.Unlock()
	}()
	if err := w.topics.Wait(ctx); err != nil {
		return nil
	}
	fetchBackoff := w.retryBackoff
	for {
		fetches := w.client.PollRecords(ctx, w.maxPollRecords)
		if ctx.Err() != nil {
			w.client.AllowRebalance()
			return nil
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka worker client is closed")
		}
		hasFetchErrors := len(fetches.Errors()) > 0
		processing, cancel := context.WithTimeout(ctx, w.processingTimeout)
		w.processingMu.Lock()
		w.processingCancel = cancel
		w.processingMu.Unlock()
		retained, err := w.handleFetches(processing, fetches)
		cancel()
		w.processingMu.Lock()
		w.processingCancel = nil
		w.lastError = err
		if len(fetches.Errors()) > 0 {
			w.lastError = fetches.Errors()[0].Err
			for _, failure := range fetches.Errors() {
				if errors.Is(failure.Err, kerr.OffsetOutOfRange) {
					w.offsetGap = true
					w.metrics.SetWorkerOffsetGap(w.store, true)
				}
			}
		}
		w.processingMu.Unlock()
		if err != nil {
			// Keep the entire uncommitted fetch replayable. SetOffsets is only
			// called while rebalance callbacks are blocked, after commits finish.
			w.rewind(retained)
			w.metrics.ObserveWorkerRecovery(w.store)
			slog.Warn("Kafka batch retained for retry", "store", w.store, "error", err)
		}
		w.client.AllowRebalance()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
		}
		if !hasFetchErrors && err == nil {
			fetchBackoff = w.retryBackoff
			continue
		}
		timer := time.NewTimer(jitteredBackoff(fetchBackoff))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		fetchBackoff = min(fetchBackoff*2, w.maxRetryBackoff)
	}
}

func (w *Worker) rebalanceRequested(_ context.Context, _ *kgo.Client) {
	w.processingMu.Lock()
	defer w.processingMu.Unlock()
	if w.processingCancel != nil {
		w.processingCancel()
	}
}

func (w *Worker) partitionsReleased(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
	// The previous fetch snapshot no longer represents current ownership.
	// Kafka group lag remains the authority for work transferred to another member.
	w.processingMu.Lock()
	w.lastError = nil
	w.processingMu.Unlock()
	var cleared time.Time
	w.metrics.SetWorkerPending(w.store, cleared, 0)
}

func (w *Worker) rewind(records []*kgo.Record) {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for _, record := range records {
		partitions := offsets[record.Topic]
		if partitions == nil {
			partitions = make(map[int32]kgo.EpochOffset)
			offsets[record.Topic] = partitions
		}
		previous, exists := partitions[record.Partition]
		if !exists || record.Offset < previous.Offset {
			partitions[record.Partition] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
		}
	}
	w.client.SetOffsets(offsets)
}

func (w *Worker) handleFetches(ctx context.Context, fetches kgo.Fetches) ([]*kgo.Record, error) {
	if fetches.IsClientClosed() {
		return nil, errors.New("consume Kafka mutations: client is closed")
	}
	fetchErrors := fetches.Errors()
	for _, fetchError := range fetchErrors {
		slog.Warn(
			"Kafka fetch failed; consumer will continue polling",
			"topic", fetchError.Topic,
			"partition", fetchError.Partition,
			"error", fetchError.Err,
		)
	}
	records := fetches.Records()
	w.metrics.ObserveWorkerPoll(w.store, len(fetchErrors))
	if len(records) == 0 {
		return nil, nil
	}
	oldest := records[0].Timestamp
	for _, record := range records {
		if record.Timestamp.Before(oldest) {
			oldest = record.Timestamp
		}
	}
	w.metrics.SetWorkerPending(w.store, oldest, len(records))
	mutations := make([]queue.Mutation, 0, len(records))
	mutationIndexes := make([]int, 0, len(records))
	finalErrors := make([]error, len(records))
	for index, record := range records {
		mutation, err := queue.UnmarshalMutation(record.Value)
		if err != nil {
			finalErrors[index] = fmt.Errorf("decode Kafka mutation at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			continue
		}
		store, err := queue.MutationStore(mutation)
		if err != nil {
			finalErrors[index] = fmt.Errorf("route Kafka mutation at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			continue
		}
		if store != w.store {
			finalErrors[index] = fmt.Errorf("kafka mutation at %s/%d/%d targets store %q, worker owns store %q", record.Topic, record.Partition, record.Offset, store, w.store)
			continue
		}
		mutations = append(mutations, mutation)
		mutationIndexes = append(mutationIndexes, index)
	}
	if len(mutations) > 0 {
		results := w.handleWithRetry(ctx, mutations)
		for index, result := range results {
			finalErrors[mutationIndexes[index]] = result
		}
	}
	// Commit only the resolved prefix of each partition. A slow suffix must not
	// force an already completed prefix to execute again on every deadline.
	ready, resolved, retained := resolvedPrefixes(records, finalErrors)
	if len(ready) > 0 {
		// Rebalance remains blocked for this bounded settlement window. Do not
		// start any more backend work after the processing context is cancelled.
		settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := w.publishDeadLetters(settlement, ready, resolved); err != nil {
			return records, fmt.Errorf("publish Kafka dead letters: %w", err)
		}
		if err := w.client.CommitRecords(settlement, ready...); err != nil {
			return records, fmt.Errorf("commit Kafka mutation offsets: %w", err)
		}
		for _, result := range resolved {
			outcome := "applied"
			if result != nil {
				outcome = "failed"
			}
			w.metrics.ObserveKafkaWorker(outcome, 1)
		}
		w.metrics.ObserveWorkerCommitted(w.store, oldest)
	}
	if len(retained) > 0 {
		w.metrics.SetWorkerPending(w.store, retained[0].Timestamp, len(retained))
		return retained, errors.New("temporary mutation failure; unresolved offsets remain uncommitted")
	}
	return nil, nil
}

type topicPartition struct {
	topic     string
	partition int32
}

func resolvedPrefixes(records []*kgo.Record, results []error) ([]*kgo.Record, []error, []*kgo.Record) {
	blocked := make(map[topicPartition]bool)
	ready := make([]*kgo.Record, 0, len(records))
	resolved := make([]error, 0, len(records))
	retained := make([]*kgo.Record, 0)
	for index, record := range records {
		partition := topicPartition{topic: record.Topic, partition: record.Partition}
		if retryableProcessingError(results[index]) {
			blocked[partition] = true
		}
		if blocked[partition] {
			retained = append(retained, record)
			continue
		}
		ready = append(ready, record)
		resolved = append(resolved, results[index])
	}
	return ready, resolved, retained
}

type pendingMutation struct {
	index    int
	mutation queue.Mutation
}

func (w *Worker) handleWithRetry(ctx context.Context, mutations []queue.Mutation) []error {
	backoff := w.retryBackoff
	finalResults := make([]error, len(mutations))
	pending := make([]pendingMutation, 0, len(mutations))
	for index, mutation := range mutations {
		work := pendingMutation{index: index, mutation: mutation}
		pending = append(pending, work)
	}
	for attempt := 1; attempt <= w.maxRetryAttempts; attempt++ {
		batch := make([]queue.Mutation, 0, len(pending))
		for _, work := range pending {
			batch = append(batch, work.mutation)
		}
		results := w.handler.HandleBatch(ctx, batch)
		if len(results) != len(batch) {
			err := storage.BackendError(errors.New("kafka mutation handler returned an invalid result count"))
			for _, work := range pending {
				finalResults[work.index] = err
			}
			return finalResults
		}
		next := make([]pendingMutation, 0, len(pending))
		for index, err := range results {
			if err == nil {
				continue
			}
			work := pending[index]
			if !isRetryable(err) || attempt == w.maxRetryAttempts {
				finalResults[work.index] = err
				continue
			}
			next = append(next, work)
		}
		if len(next) == 0 {
			return finalResults
		}
		w.metrics.ObserveKafkaRetry(len(next))
		delay := jitteredBackoff(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			for _, work := range next {
				finalResults[work.index] = ctx.Err()
			}
			return finalResults
		case <-timer.C:
		}
		pending = next
		backoff = min(backoff*2, w.maxRetryBackoff)
	}
	return finalResults
}

func jitteredBackoff(backoff time.Duration) time.Duration {
	if backoff <= 1 {
		return backoff
	}
	half := backoff / 2
	spread := int64(backoff)
	return half + time.Duration(rand.Int64N(spread))
}

func (w *Worker) publishDeadLetters(ctx context.Context, records []*kgo.Record, results []error) error {
	deadLetters := make([]*kgo.Record, 0)
	for index, result := range results {
		if result == nil {
			continue
		}
		original := records[index]
		errorMessage := result.Error()
		if len(errorMessage) > maxDeadLetterErrorBytes {
			errorMessage = errorMessage[:maxDeadLetterErrorBytes]
		}
		record := &kgo.Record{
			Topic: w.deadLetterTopic,
			Key:   append([]byte(nil), original.Key...),
			Value: append([]byte(nil), original.Value...),
			Headers: []kgo.RecordHeader{
				{Key: "sink-source-topic", Value: []byte(original.Topic)},
				{Key: "sink-source-partition", Value: []byte(strconv.Itoa(int(original.Partition)))},
				{Key: "sink-source-offset", Value: []byte(strconv.FormatInt(original.Offset, 10))},
				{Key: "sink-error", Value: []byte(errorMessage)},
			},
		}
		deadLetters = append(deadLetters, record)
	}
	if len(deadLetters) == 0 {
		return nil
	}
	produced := w.client.ProduceSync(ctx, deadLetters...)
	produceErrors := make([]error, 0)
	for _, result := range produced {
		if result.Err != nil {
			produceErrors = append(produceErrors, result.Err)
			continue
		}
		w.metrics.ObserveQuarantined(w.store)
		slog.Error("Kafka mutation quarantined", "store", w.store, "dead_letter_topic", w.deadLetterTopic, "partition", result.Record.Partition, "offset", result.Record.Offset)
	}
	joined := errors.Join(produceErrors...)
	if joined == nil {
		w.metrics.ObserveKafkaDeadLetter(len(deadLetters))
	}
	return joined
}

func (w *Worker) Ping(ctx context.Context) error {
	if err := w.topics.Ping(ctx); err != nil {
		return err
	}
	w.processingMu.Lock()
	running, lastError, offsetGap := w.running, w.lastError, w.offsetGap
	w.processingMu.Unlock()
	if offsetGap {
		return errors.New("kafka source offset is outside retention; explicit recovery and worker restart required")
	}
	if !running {
		return errors.New("kafka worker is not running")
	}
	if lastError != nil {
		return fmt.Errorf("kafka worker is recovering: %w", lastError)
	}
	return w.client.Ping(ctx)
}

func (w *Worker) Close() {
	w.client.AllowRebalance()
	w.client.Close()
}

type retryableError interface {
	Retryable() bool
}

func isRetryable(err error) bool {
	var candidate retryableError
	_, backendRetryable := storage.ErrorDetails(err)
	return backendRetryable || (errors.As(err, &candidate) && candidate.Retryable())
}
