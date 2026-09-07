package service

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
)

func TestWriteObservationsCountOnlyRetriedDocuments(t *testing.T) {
	observed, err := sinkmetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	backend := newHeldReadStorage(t)
	backend.conflict = true
	backend.unblock()
	core := completionServer(t, backend).server
	core.metrics = observed
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	call := completionWriteCall(t.Context(), mode, completionMerge("fast", 1), completionMerge("slow", 1))
	response, err := core.Write(t.Context(), call.request)
	if err != nil || len(response.GetResults()) != 2 {
		t.Fatalf("write: %v, %v", response, err)
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	wanted := []string{
		`sink_write_phase_duration_seconds_count{phase="storage_read"} 2`,
		`sink_write_phase_duration_seconds_count{phase="storage_write_applied"} 2`,
		`sink_write_phase_duration_seconds_count{phase="lua"} 3`,
		`sink_write_phase_duration_seconds_count{phase="admission"} 1`,
		`sink_write_execution_rounds_sum{phase="storage_read"} 2`,
		`sink_write_execution_rounds_count{phase="storage_write"} 1`,
	}
	for _, line := range wanted {
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
}

func TestBatchQueueObservesEveryRPCIncludingCanceledAndShutdown(t *testing.T) {
	observed, err := sinkmetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	opts := requestBatcherOptions[int, int]{Method: "Read", Metrics: observed, MaxWait: time.Hour, MaxOperations: 2, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	results := make(chan batcherSubmission, 2)
	go submitBatcherTestRequest(batcher, 1, results)
	waitForQueuedCalls(t, batcher, 1)
	go submitBatcherTestRequest(batcher, 2, results)
	for range 2 {
		if result := awaitCompletion(t, results); result.err != nil {
			t.Fatal(result)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := make(chan error, 1)
	go func() { _, err := batcher.Submit(ctx, 3, 1, 1); canceled <- err }()
	waitForQueuedCalls(t, batcher, 1)
	cancel()
	if err := awaitCompletion(t, canceled); err == nil {
		t.Fatal("queued cancellation succeeded")
	}
	waitForQueuedCalls(t, batcher, 0)
	go submitBatcherTestRequest(batcher, 4, results)
	waitForQueuedCalls(t, batcher, 1)
	batcher.Close()
	if result := awaitCompletion(t, results); result.err == nil {
		t.Fatal("queued shutdown succeeded")
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	wanted := []string{
		`sink_batcher_request_queue_duration_seconds_count{method="Read"} 4`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="execute"} 2`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="canceled"} 1`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="shutdown"} 1`,
		`sink_batcher_batches_total{method="Read",reason="max_operations"} 1`,
	}
	for _, line := range wanted {
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
}

func TestWriteObservationsDoNotLabelArbitraryStores(t *testing.T) {
	observed, err := sinkmetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	backend := newHeldReadStorage(t)
	core := completionServer(t, backend).server
	core.metrics = observed
	for index := range 10 {
		op := completionPut("key", index)
		op.Address.Store = fmt.Sprintf("untrusted-client-store-%d", index)
		call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, op)
		observation := core.newWriteObservation(call.request)
		// Exercise the real request-to-store classification without a long sleep.
		observation.phase("storage_write", time.Now().Add(-6*time.Second))
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if strings.Contains(body, "untrusted-client-store") || !strings.Contains(body, `sink_write_slow_phases_total{phase="storage_write_applied",store="_unconfigured"} 10`) {
		t.Fatal("client input escaped bounded metric labels")
	}
}
