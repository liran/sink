package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type outageHandler struct {
	failing atomic.Bool
	calls   atomic.Int32
}

func (h *outageHandler) HandleBatch(_ context.Context, mutations []queue.Mutation) []error {
	h.calls.Add(1)
	results := make([]error, len(mutations))
	if h.failing.Load() {
		for index := range results {
			results[index] = storage.BackendError(errors.New("injected backend outage"))
		}
	}
	return results
}

func TestWorkerRecoversBeyondRetryBudgetWithoutDeadLetter(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "recovery", "recovery.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	handler := &outageHandler{}
	handler.failing.Store(true)
	opts := WorkerOptions{
		Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "recovery",
		GroupID: "recovery-worker", DeadLetterTopic: "recovery.dlq", Handler: handler,
		MaxRetryAttempts: 2, RetryBackoff: time.Millisecond, MaxRetryBackoff: 5 * time.Millisecond,
	}
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{}`)
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "recovery", Value: payload}
	if err := w.client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
		w.Close()
	})
	waitRecovery(t, func() bool { return handler.calls.Load() >= 6 })
	admin := kadm.NewClient(w.client)
	ends, err := admin.ListEndOffsets(t.Context(), "recovery.dlq")
	if err != nil {
		t.Fatal(err)
	}
	if ends["recovery.dlq"][0].Offset != 0 {
		t.Fatal("temporary failure was quarantined")
	}
	committed, err := admin.FetchOffsets(t.Context(), "recovery-worker")
	if err != nil {
		t.Fatal(err)
	}
	if committed["recovery"][0].At > 0 {
		t.Fatal("unresolved source record was committed")
	}
	handler.failing.Store(false)
	waitRecovery(t, func() bool {
		committed, err := admin.FetchOffsets(t.Context(), "recovery-worker")
		return err == nil && committed["recovery"][0].At == 1
	})
}

func waitRecovery(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("recovery condition did not become true")
		case <-ticker.C:
		}
	}
}

func TestResolvedPrefixesCannotSkipFailedPartitionOffset(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "t", Partition: 0, Offset: 10}, {Topic: "t", Partition: 0, Offset: 11},
		{Topic: "t", Partition: 0, Offset: 12}, {Topic: "t", Partition: 1, Offset: 20},
	}
	results := []error{nil, context.DeadlineExceeded, nil, errors.New("invalid record")}
	ready, resolved, retained := resolvedPrefixes(records, results)
	if len(ready) != 2 || ready[0].Offset != 10 || ready[1].Offset != 20 || resolved[1] == nil {
		t.Fatalf("incorrect committed prefix: %v, %v", ready, resolved)
	}
	if len(retained) != 2 || retained[0].Offset != 11 || retained[1].Offset != 12 {
		t.Fatalf("incorrect retained suffix: %v", retained)
	}
}

func TestRebalanceCancelsCurrentProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w := &Worker{processingCancel: cancel}
	w.rebalanceRequested(t.Context(), nil)
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("rebalance did not cancel backend work")
	}
}

type heldProcessingHandler struct {
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
}

func (h *heldProcessingHandler) HandleBatch(ctx context.Context, mutations []queue.Mutation) []error {
	h.once.Do(func() { close(h.started) })
	<-ctx.Done()
	select {
	case h.cancelled <- struct{}{}:
	default:
	}
	results := make([]error, len(mutations))
	for index := range results {
		results[index] = ctx.Err()
	}
	return results
}

func TestJoiningWorkerCancelsInFlightBatchBeforeRebalanceTimeout(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "rebalance", "rebalance.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	held := &heldProcessingHandler{started: make(chan struct{}), cancelled: make(chan struct{}, 1)}
	opts := WorkerOptions{Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "rebalance", GroupID: "joining-worker",
		DeadLetterTopic: "rebalance.dlq", Handler: held, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
	first, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var running sync.WaitGroup
	workers := []*Worker{first}
	t.Cleanup(func() {
		cancel()
		running.Wait()
		for _, w := range workers {
			w.Close()
		}
	})
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{}`)
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "rebalance", Value: payload}
	if err := first.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	running.Go(func() {
		if err := first.Run(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-held.started:
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not start")
	}
	opts.Handler = &outageHandler{}
	second, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	workers = append(workers, second)
	running.Go(func() {
		if err := second.Run(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-held.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("new group member did not interrupt the 20-second processing window")
	}
}

func TestExpiredCommittedOffsetStopsInsteadOfSkippingData(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "retention", "retention.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	clientOptions := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}
	seed, err := kgo.NewClient(clientOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	admin := kadm.NewClient(seed)
	handler := &outageHandler{}
	opts := WorkerOptions{Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "retention", GroupID: "expired-offset",
		DeadLetterTopic: "retention.dlq", Handler: handler, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
	firstWorker, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{}`)
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	first := &kgo.Record{Topic: "retention", Value: payload}
	if err := seed.ProduceSync(t.Context(), first).FirstErr(); err != nil {
		t.Fatal(err)
	}
	initialContext, stopInitial := context.WithCancel(t.Context())
	initialDone := make(chan error, 1)
	go func() { initialDone <- firstWorker.Run(initialContext) }()
	t.Cleanup(func() { stopInitial(); firstWorker.Close() })
	waitRecovery(t, func() bool {
		positions, err := admin.FetchOffsets(t.Context(), "expired-offset")
		return err == nil && positions["retention"][0].At == 1
	})
	stopInitial()
	if err := <-initialDone; err != nil {
		t.Fatal(err)
	}
	firstWorker.Close()
	second := &kgo.Record{Topic: "retention", Value: payload}
	third := &kgo.Record{Topic: "retention", Value: payload}
	if err := seed.ProduceSync(t.Context(), second, third).FirstErr(); err != nil {
		t.Fatal(err)
	}
	position := kadm.Offset{Topic: "retention", Partition: 0, At: 2, LeaderEpoch: -1}
	positions := kadm.Offsets{"retention": {0: position}}
	deleted, err := admin.DeleteRecords(t.Context(), positions)
	if err != nil {
		t.Fatal(err)
	}
	if err := deleted.Error(); err != nil {
		t.Fatal(err)
	}
	recoveredHandler := &outageHandler{}
	opts.Handler = recoveredHandler
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done; w.Close() })
	waitRecovery(t, func() bool {
		w.processingMu.Lock()
		defer w.processingMu.Unlock()
		return w.offsetGap
	})
	if recoveredHandler.calls.Load() != 0 {
		t.Fatal("worker silently skipped to retained data")
	}
	if err := w.Ping(t.Context()); err == nil {
		t.Fatal("expired offset was reported healthy")
	}
	remaining, err := admin.FetchOffsets(t.Context(), "expired-offset")
	if err != nil || remaining["retention"][0].At != 1 {
		t.Fatalf("expired offset advanced: %v %v", remaining, err)
	}
}
