package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxPollRecords   = 500
	defaultMaxRetryAttempts = 10
	defaultRetryBackoff     = 100 * time.Millisecond
	defaultMaxBackoff       = 10 * time.Second
	maxDeadLetterErrorBytes = 4096
)

type Handler interface {
	HandleBatch(ctx context.Context, mutations []queue.Mutation) []error
}

type WorkerOptions struct {
	Brokers          []string
	Topic            string
	GroupID          string
	DeadLetterTopic  string
	Handler          Handler
	MaxPollRecords   int
	MaxRetryAttempts int
	RetryBackoff     time.Duration
	MaxRetryBackoff  time.Duration
	ClientOptions    []kgo.Opt
	Metrics          *sinkmetrics.Metrics
}

type Worker struct {
	client           *kgo.Client
	handler          Handler
	deadLetterTopic  string
	maxPollRecords   int
	maxRetryAttempts int
	retryBackoff     time.Duration
	maxRetryBackoff  time.Duration
	metrics          *sinkmetrics.Metrics
}

func NewWorker(opts WorkerOptions) (*Worker, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka worker: brokers are required")
	}
	if opts.Topic == "" || opts.GroupID == "" || opts.DeadLetterTopic == "" {
		return nil, errors.New("create Kafka worker: topic, group ID, and dead-letter topic are required")
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

	clientOptions := append([]kgo.Opt(nil), opts.ClientOptions...)
	requiredOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumeTopics(opts.Topic),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	worker := &Worker{
		client:           client,
		handler:          opts.Handler,
		deadLetterTopic:  opts.DeadLetterTopic,
		maxPollRecords:   maxPollRecords,
		maxRetryAttempts: maxRetryAttempts,
		retryBackoff:     retryBackoff,
		maxRetryBackoff:  maxRetryBackoff,
		metrics:          opts.Metrics,
	}
	return worker, nil
}

func (w *Worker) Run(ctx context.Context) error {
	fetchBackoff := w.retryBackoff
	for {
		fetches := w.client.PollRecords(ctx, w.maxPollRecords)
		if ctx.Err() != nil {
			w.client.AllowRebalance()
			return nil
		}
		hasFetchErrors := len(fetches.Errors()) > 0
		err := w.handleFetches(ctx, fetches)
		w.client.AllowRebalance()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !hasFetchErrors {
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

func (w *Worker) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	if fetches.IsClientClosed() {
		return errors.New("consume Kafka mutations: client is closed")
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
	if len(records) == 0 {
		return nil
	}
	mutations := make([]queue.Mutation, 0, len(records))
	mutationIndexes := make([]int, 0, len(records))
	finalErrors := make([]error, len(records))
	for index, record := range records {
		mutation, err := queue.UnmarshalMutation(record.Value)
		if err != nil {
			finalErrors[index] = fmt.Errorf("decode Kafka mutation at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
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
	succeeded := 0
	failed := 0
	for _, result := range finalErrors {
		if result == nil {
			succeeded++
			continue
		}
		failed++
	}
	w.metrics.ObserveKafkaWorker("applied", succeeded)
	w.metrics.ObserveKafkaWorker("failed", failed)
	if err := w.publishDeadLetters(ctx, records, finalErrors); err != nil {
		return fmt.Errorf("publish Kafka dead letters: %w", err)
	}
	if err := w.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit Kafka mutation offsets: %w", err)
	}
	return nil
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
			err := errors.New("kafka mutation handler returned an invalid result count")
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
		}
	}
	joined := errors.Join(produceErrors...)
	if joined == nil {
		w.metrics.ObserveKafkaDeadLetter(len(deadLetters))
	}
	return joined
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
	return errors.As(err, &candidate) && candidate.Retryable()
}
