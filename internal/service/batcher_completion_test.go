package service

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
)

func TestBatcherOldBatchCannotReleaseNewOwnerOfCompletedDocument(t *testing.T) {
	firstGate := make(chan struct{})
	secondGate := make(chan struct{})
	var firstRelease, secondRelease sync.Once
	firstHeld := make(chan struct{})
	secondHeld := make(chan struct{})
	firstDone := make(chan struct{})
	key := recordIdentity{dataset: "products", keyType: "string", keyData: "hot"}
	other := recordIdentity{dataset: "products", keyType: "string", keyData: "other"}
	records := func(request int) []recordIdentity {
		if request == 2 {
			return []recordIdentity{other}
		}
		return []recordIdentity{key}
	}
	execute := func(ctx context.Context, calls []*batchCall[int, int]) {
		for _, call := range calls {
			switch call.request {
			case 2:
				close(firstHeld)
				select {
				case <-firstGate:
				case <-ctx.Done():
				}
				completeCall(call, call.request, nil)
				close(firstDone)
			case 3:
				close(secondHeld)
				select {
				case <-secondGate:
				case <-ctx.Done():
				}
				completeCall(call, call.request, nil)
			default:
				completeCall(call, call.request, nil)
			}
		}
	}
	opts := requestBatcherOptions[int, int]{Method: "Write", MaxWait: 20 * time.Millisecond, MaxConcurrent: 3, MaxOperations: 2, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Records: records, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	t.Cleanup(func() { firstRelease.Do(func() { close(firstGate) }); secondRelease.Do(func() { close(secondGate) }) })
	results := make(chan batcherSubmission, 4)
	go submitBatcherTestRequest(batcher, 1, results)
	waitForQueuedCalls(t, batcher, 1)
	go submitBatcherTestRequest(batcher, 2, results)
	awaitCompletion(t, firstHeld)
	if result := awaitCompletion(t, results); result.request != 1 || result.err != nil {
		t.Fatal(result)
	}
	go submitBatcherTestRequest(batcher, 3, results)
	awaitCompletion(t, secondHeld)
	firstRelease.Do(func() { close(firstGate) })
	awaitCompletion(t, firstDone)
	if result := awaitCompletion(t, results); result.request != 2 || result.err != nil {
		t.Fatal(result)
	}
	go submitBatcherTestRequest(batcher, 4, results)
	waitForQueuedCalls(t, batcher, 1)
	select {
	case result := <-results:
		t.Fatalf("new owner was released by old batch completion: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	secondRelease.Do(func() { close(secondGate) })
	for range 2 {
		if result := awaitCompletion(t, results); result.err != nil {
			t.Fatal(result)
		}
	}
}

func TestBatcherShutdownUnblocksDocumentCompletionNotifications(t *testing.T) {
	started := make(chan struct{})
	records := func(request int) []recordIdentity {
		key := recordIdentity{keyType: "string", keyData: strconv.Itoa(request)}
		return []recordIdentity{key}
	}
	execute := func(ctx context.Context, calls []*batchCall[int, int]) {
		close(started)
		<-ctx.Done()
		for _, call := range calls {
			completeCall(call, 0, ctx.Err())
		}
	}
	const count = 32
	opts := requestBatcherOptions[int, int]{Method: "Write", MaxWait: time.Hour, MaxConcurrent: 1, MaxOperations: count, MaxBytes: 100, MaxQueuedOperations: count, MaxQueuedBytes: 1000, Records: records, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	results := make(chan batcherSubmission, count)
	for index := range count {
		go submitBatcherTestRequest(batcher, index, results)
	}
	awaitCompletion(t, started)
	closed := make(chan struct{})
	go func() { batcher.Close(); close(closed) }()
	awaitCompletion(t, closed)
	for range count {
		if result := awaitCompletion(t, results); result.err == nil {
			t.Fatalf("shutdown request succeeded: %+v", result)
		}
	}
}

func TestBatcherCancellationDoesNotReleaseRunningMutation(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	var release sync.Once
	key := recordIdentity{dataset: "products", keyType: "string", keyData: "hot"}
	records := func(int) []recordIdentity { return []recordIdentity{key} }
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		for _, call := range calls {
			if call.request == 1 {
				close(entered)
				// Model a backend which cannot immediately interrupt an accepted write.
				<-gate
			}
			completeCall(call, call.request, nil)
		}
	}
	opts := requestBatcherOptions[int, int]{Method: "Write", MaxWait: time.Millisecond, MaxConcurrent: 2, MaxOperations: 1, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Records: records, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() { _, err := batcher.Submit(ctx, 1, 1, 1); first <- err }()
	awaitCompletion(t, entered)
	cancel()
	if err := awaitCompletion(t, first); err == nil {
		t.Fatal("canceled caller succeeded")
	}
	results := make(chan batcherSubmission, 1)
	go submitBatcherTestRequest(batcher, 2, results)
	waitForQueuedCalls(t, batcher, 1)
	select {
	case result := <-results:
		t.Fatalf("later mutation overlapped canceled but still running write: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(gate) })
	if result := awaitCompletion(t, results); result.err != nil {
		t.Fatal(result)
	}
}

func TestMutationPartitionsPreserveBlockedPredecessors(t *testing.T) {
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	first := completionWriteCall(t.Context(), mode, completionPut("first", 1))
	second := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionPut("hot", 1))
	third := completionWriteCall(t.Context(), mode, completionPut("hot", 2))
	other := completionWriteCall(t.Context(), mode, completionPut("independent", 1))
	other.request.Operations[0].Address.Dataset = "another-index"
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second, third, other}
	for _, call := range calls {
		call.records = mutationRequestRecords(call.request, identityOf)
		call.partition = mutationRequestPartition[*sink.WriteOperation](call.request)
		call.encodedBytes = 1
	}
	batcher := &requestBatcher[*sink.WriteRequest, *sink.WriteResponse]{maxOperations: 100, maxBytes: 1000}
	active := make(map[recordIdentity]bool)
	selected, pending, _ := batcher.selectReady(calls, active)
	if len(selected) != 1 || selected[0] != first || len(pending) != 3 {
		t.Fatal("partition crossing or same-document predecessor overtaken")
	}
	selected, pending, _ = batcher.selectReady(pending, active)
	if len(selected) != 1 || selected[0] != second || len(pending) != 2 {
		t.Fatal("completion-mode order changed")
	}
}

func TestBatcherWaitsForEveryCallerOwningSharedDocument(t *testing.T) {
	gate := make(chan struct{})
	held := make(chan struct{})
	var release sync.Once
	key := recordIdentity{dataset: "products", keyType: "string", keyData: "hot"}
	records := func(int) []recordIdentity { return []recordIdentity{key} }
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		for _, call := range calls {
			if call.request == 2 {
				close(held)
				<-gate
			}
			completeCall(call, call.request, nil)
		}
	}
	opts := requestBatcherOptions[int, int]{Method: "Write", MaxWait: 100 * time.Millisecond, MaxConcurrent: 2, MaxOperations: 2, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Records: records, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	results := make(chan batcherSubmission, 3)
	go submitBatcherTestRequest(batcher, 1, results)
	waitForQueuedCalls(t, batcher, 1)
	go submitBatcherTestRequest(batcher, 2, results)
	awaitCompletion(t, held)
	if result := awaitCompletion(t, results); result.request != 1 || result.err != nil {
		t.Fatal(result)
	}
	go submitBatcherTestRequest(batcher, 3, results)
	waitForQueuedCalls(t, batcher, 1)
	select {
	case result := <-results:
		t.Fatalf("document released while another selected caller still owns it: %+v", result)
	case <-time.After(150 * time.Millisecond):
	}
	release.Do(func() { close(gate) })
	for range 2 {
		if result := awaitCompletion(t, results); result.err != nil {
			t.Fatal(result)
		}
	}
}
