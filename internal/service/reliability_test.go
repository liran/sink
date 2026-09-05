package service

import (
	"context"
	"testing"
	"time"
)

func TestOnlyCallerCancellationMustReleaseStoreQueue(t *testing.T) {
	started := make(chan struct{})
	executionStopped := make(chan struct{})
	execute := func(ctx context.Context, calls []*batchCall[int, int]) {
		close(started)
		<-ctx.Done()
		close(executionStopped)
		for _, call := range calls {
			completeCall(call, 0, ctx.Err())
		}
	}
	opts := requestBatcherOptions[int, int]{Method: "cancellation", MaxWait: time.Millisecond, MaxOperations: 1, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Execute: execute}
	batcher := newRequestBatcher(opts)
	defer batcher.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	callerDone := make(chan error, 1)
	go func() { _, err := batcher.Submit(ctx, 1, 1, 1); callerDone <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch did not start")
	}
	cancel()
	<-callerDone
	select {
	case <-executionStopped:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("the only caller was canceled, but backend execution has no deadline and retains the store queue until shutdown or backend recovery")
	}
}
