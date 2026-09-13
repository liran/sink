package kafka

import (
	"context"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"testing"
	"time"
)

type capacitySplitHandler struct {
	processor *worker.Processor
	calls     chan int
}

func (h *capacitySplitHandler) HandleBatch(ctx context.Context, mutations []queue.Mutation) []error {
	results := h.processor.HandleBatch(ctx, mutations)
	select {
	case h.calls <- len(mutations):
	default:
	}
	return results
}

func TestWorkerSplitsCapacityRejectedPollAndCommits(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "review-capacity", "review-capacity.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	luaOpts := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	coreOpts := service.Options{Storage: store, Lua: engine, MaxInFlightBytes: 1024}
	core, err := service.New(coreOpts)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(core)
	if err != nil {
		t.Fatal(err)
	}
	handler := &capacitySplitHandler{processor: processor, calls: make(chan int, 100)}
	opts := WorkerOptions{Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "review-capacity", GroupID: "review-capacity", DeadLetterTopic: "review-capacity.dlq", Handler: handler, MaxPollRecords: 2, MaxRetryAttempts: 1, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var records []*kgo.Record
	for _, id := range []string{"a", "b"} {
		mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":"`+strings.Repeat("x", 600)+`"}`)
		kind := &sink.RecordKey_StringValue{StringValue: id}
		key := &sink.RecordKey{Kind: kind}
		mutation.Write.Address.Key = key
		payload, err := queue.MarshalMutation(mutation)
		if err != nil {
			t.Fatal(err)
		}
		record := &kgo.Record{Topic: "review-capacity", Value: payload}
		records = append(records, record)
	}
	if err := w.client.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case size := <-handler.calls:
		if size != 2 {
			t.Fatalf("unexpected poll size %d", size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process poll")
	}
	admin := kadm.NewClient(w.client)
	waitRecovery(t, func() bool {
		offsets, err := admin.FetchOffsets(t.Context(), "review-capacity")
		return err == nil && offsets["review-capacity"][0].At == 2
	})
	ends, err := admin.ListEndOffsets(t.Context(), "review-capacity.dlq")
	if err != nil {
		t.Fatal(err)
	}
	if ends["review-capacity.dlq"][0].Offset != 0 {
		t.Fatal("valid records were quarantined")
	}
}
