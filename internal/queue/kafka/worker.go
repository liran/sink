package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxPollRecords = 500
	defaultRetryBackoff   = 100 * time.Millisecond
	defaultMaxBackoff     = 10 * time.Second
)

type Handler interface {
	HandleBatch(ctx context.Context, mutations []queue.Mutation) []error
}

type WorkerOptions struct {
	Brokers         []string
	Topic           string
	GroupID         string
	Handler         Handler
	MaxPollRecords  int
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	ClientOptions   []kgo.Opt
}

type Worker struct {
	client          *kgo.Client
	handler         Handler
	maxPollRecords  int
	retryBackoff    time.Duration
	maxRetryBackoff time.Duration
}

func NewWorker(opts WorkerOptions) (*Worker, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka worker: brokers are required")
	}
	if opts.Topic == "" || opts.GroupID == "" {
		return nil, errors.New("create Kafka worker: topic and group ID are required")
	}
	if opts.Handler == nil {
		return nil, errors.New("create Kafka worker: handler is required")
	}
	if opts.MaxPollRecords < 0 || opts.RetryBackoff < 0 || opts.MaxRetryBackoff < 0 {
		return nil, errors.New("create Kafka worker: limits cannot be negative")
	}

	maxPollRecords := opts.MaxPollRecords
	if maxPollRecords == 0 {
		maxPollRecords = defaultMaxPollRecords
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
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	worker := &Worker{
		client:          client,
		handler:         opts.Handler,
		maxPollRecords:  maxPollRecords,
		retryBackoff:    retryBackoff,
		maxRetryBackoff: maxRetryBackoff,
	}
	return worker, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		fetches := w.client.PollRecords(ctx, w.maxPollRecords)
		if ctx.Err() != nil {
			w.client.AllowRebalance()
			return nil
		}
		err := w.handleFetches(ctx, fetches)
		w.client.AllowRebalance()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (w *Worker) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	fetchErrors := fetches.Errors()
	if len(fetchErrors) > 0 {
		errorsList := make([]error, 0, len(fetchErrors))
		for _, fetchError := range fetchErrors {
			errorsList = append(errorsList, fetchError.Err)
		}
		return fmt.Errorf("consume Kafka mutations: %w", errors.Join(errorsList...))
	}
	records := fetches.Records()
	if len(records) == 0 {
		return nil
	}
	mutations := make([]queue.Mutation, 0, len(records))
	for _, record := range records {
		mutation, err := queue.UnmarshalMutation(record.Value)
		if err != nil {
			return fmt.Errorf("decode Kafka mutation at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		mutations = append(mutations, mutation)
	}
	if err := w.handleWithRetry(ctx, mutations); err != nil {
		return fmt.Errorf("apply Kafka mutation batch: %w", err)
	}
	if err := w.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit Kafka mutation offsets: %w", err)
	}
	return nil
}

func (w *Worker) handleWithRetry(ctx context.Context, mutations []queue.Mutation) error {
	backoff := w.retryBackoff
	pending := mutations
	for {
		results := w.handler.HandleBatch(ctx, pending)
		if len(results) != len(pending) {
			return errors.New("kafka mutation handler returned an invalid result count")
		}
		next := make([]queue.Mutation, 0, len(pending))
		for index, err := range results {
			if err == nil {
				continue
			}
			if !isRetryable(err) {
				return err
			}
			next = append(next, pending[index])
		}
		if len(next) == 0 {
			return nil
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		pending = next
		backoff = min(backoff*2, w.maxRetryBackoff)
	}
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
