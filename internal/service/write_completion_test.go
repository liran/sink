package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type heldReadStorage struct {
	storage.Storage
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	mu          sync.Mutex
	reads       int
	writes      map[string]int
	blockAfter  int
	conflict    bool
	failure     error
}

func (s *heldReadStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.mu.Lock()
	s.reads++
	block := s.reads >= max(1, s.blockAfter)
	s.mu.Unlock()
	if block {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			var response storage.ReadResponse
			return response, ctx.Err()
		}
		if s.failure != nil {
			var response storage.ReadResponse
			return response, s.failure
		}
	}
	return s.Storage.Read(ctx, req)
}

func (s *heldReadStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	forward := storage.WriteRequest{WaitUntilVisible: req.WaitUntilVisible}
	var indexes []int
	s.mu.Lock()
	for index, operation := range req.Operations {
		key := string(operation.Address.Key.Data)
		s.writes[key]++
		if s.conflict && key == "slow" {
			s.conflict = false
			response.Results[index].Status = storage.WriteStatusPreconditionFailed
			continue
		}
		forward.Operations = append(forward.Operations, operation)
		indexes = append(indexes, index)
	}
	s.mu.Unlock()
	if len(forward.Operations) > 0 {
		stored, err := s.Storage.Write(ctx, forward)
		if err != nil {
			return response, err
		}
		for index, result := range stored.Results {
			response.Results[indexes[index]] = result
		}
	}
	return response, nil
}

func newHeldReadStorage(t *testing.T) *heldReadStorage {
	t.Helper()
	backend := &heldReadStorage{Storage: memory.New(), entered: make(chan struct{}), release: make(chan struct{}), writes: make(map[string]int)}
	t.Cleanup(backend.unblock)
	return backend
}

func (s *heldReadStorage) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func requireWriteResult(t *testing.T, call *batchCall[*sink.WriteRequest, *sink.WriteResponse]) *sink.WriteResponse {
	t.Helper()
	select {
	case result := <-call.result:
		if result.err != nil {
			t.Fatal(result.err)
		}
		for index, operation := range result.response.Results {
			if operation.OperationIndex != uint32(index) || operation.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("invalid operation result: %v", operation)
			}
		}
		return result.response
	case <-time.After(time.Second):
		t.Fatal("completed RPC is blocked by another document")
		return nil
	}
}

func TestWriteCompletionReturnsPutBeforeUnrelatedMergeRead(t *testing.T) {
	for _, failure := range []error{nil, errors.New("injected read failure")} {
		t.Run(status.Code(failure).String(), func(t *testing.T) {
			backend := newHeldReadStorage(t)
			backend.failure = failure
			server := completionServer(t, backend)
			mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
			fast := completionWriteCall(t.Context(), mode, completionPut("fast", 1))
			slow := completionWriteCall(t.Context(), mode, completionMerge("slow", 1))
			calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{fast, slow}
			done := make(chan struct{})
			go func() { server.executeWrites(t.Context(), calls); close(done) }()
			awaitCompletion(t, backend.entered)
			response := requireWriteResult(t, fast)
			before := response.String()
			// A later backend error must not replace or mutate a delivered result.
			backend.unblock()
			awaitCompletion(t, done)
			if response.String() != before {
				t.Fatal("executor mutated a delivered result")
			}
			if failure == nil {
				requireWriteResult(t, slow)
			} else {
				result := awaitCompletion(t, slow.result)
				if status.Code(result.err) != codes.Unavailable {
					t.Fatalf("pending RPC did not receive backend failure: %v", result.err)
				}
			}
			if len(fast.result) != 0 {
				t.Fatal("completed RPC received a second result")
			}
		})
	}
}

func TestWriteCompletionReturnsSuccessfulMergeBeforeSiblingRetry(t *testing.T) {
	backend := newHeldReadStorage(t)
	backend.blockAfter = 2
	backend.conflict = true
	server := completionServer(t, backend)
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	fast := completionWriteCall(t.Context(), mode, completionMerge("fast", 1))
	slow := completionWriteCall(t.Context(), mode, completionMerge("slow", 1))
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{fast, slow}
	done := make(chan struct{})
	go func() { server.executeWrites(t.Context(), calls); close(done) }()
	awaitCompletion(t, backend.entered)
	requireWriteResult(t, fast)
	select {
	case <-slow.result:
		t.Fatal("conflicting document completed before retry")
	default:
	}
	backend.unblock()
	awaitCompletion(t, done)
	requireWriteResult(t, slow)
	if backend.writes["fast"] != 1 || backend.writes["slow"] != 2 {
		t.Fatalf("successful sibling was replayed: %v", backend.writes)
	}
}

func TestWriteCompletionReleasesDocumentBeforeWholeRPC(t *testing.T) {
	backend := newHeldReadStorage(t)
	core := completionServer(t, backend).server
	opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 2, MaxWait: time.Millisecond}
	server, err := NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	// Unblock execution before Close, including on a failed assertion.
	t.Cleanup(backend.unblock)
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	first := completionWriteCall(t.Context(), mode, completionPut("fast", 1), completionMerge("slow", 1))
	done := make(chan error, 1)
	go func() { _, err := server.Write(t.Context(), first.request); done <- err }()
	awaitCompletion(t, backend.entered)
	select {
	case err := <-done:
		t.Fatalf("multi-operation RPC returned before all its results: %v", err)
	default:
	}
	next := completionWriteCall(t.Context(), mode, completionPut("fast", 2))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	response, err := server.Write(ctx, next.request)
	if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("completed document remains owned by unrelated unfinished operation: %v, %v", response, err)
	}
	core.admissionMu.Lock()
	requests, reserved := core.inFlightRequests, core.inFlightBytes
	core.admissionMu.Unlock()
	if requests < 1 || reserved <= 0 {
		t.Fatal("early completion released capacity still owned by unfinished execution")
	}
	backend.unblock()
	if err := awaitCompletion(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestWriteCompletionKeepsSpeculativeFailuresPrivate(t *testing.T) {
	backend := newHeldReadStorage(t)
	backend.blockAfter = 2
	backend.conflict = true
	server := completionServer(t, backend)
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	create := completionPut("slow", 1)
	create.GetPut().Mode = sink.WriteMode_WRITE_MODE_CREATE
	duplicate := completionPut("slow", 2)
	duplicate.GetPut().Mode = sink.WriteMode_WRITE_MODE_CREATE
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{
		completionWriteCall(t.Context(), mode, create),
		completionWriteCall(t.Context(), mode, duplicate),
		completionWriteCall(t.Context(), mode, completionMerge("slow", 3)),
	}
	done := make(chan struct{})
	go func() { server.executeWrites(t.Context(), calls); close(done) }()
	awaitCompletion(t, backend.entered)
	for _, call := range calls {
		select {
		case result := <-call.result:
			t.Fatalf("uncommitted chain published a speculative result: %+v", result)
		default:
		}
	}
	backend.unblock()
	awaitCompletion(t, done)
	requireWriteResult(t, calls[0])
	requireWriteResult(t, calls[2])
	failed := awaitCompletion(t, calls[1].result)
	if failed.err != nil || failed.response.GetResults()[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED {
		t.Fatalf("committed chain lost conditional failure: %+v", failed)
	}
}

func TestWriteBatchingIsolatesDatasetRefreshWait(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 4), blocked: "product", release: make(chan struct{})}
	core := completionServer(t, backend).server
	opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 2, MaxWait: 100 * time.Millisecond}
	server, err := NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(backend.release) })
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
	slow := completionWriteCall(t.Context(), mode, completionPut("product", 1))
	fast := completionWriteCall(t.Context(), mode, completionPut("fast", 2))
	fast.request.Operations[0].Address.Dataset = "another-index"
	done := make(chan error, 1)
	go func() { _, err := server.Write(t.Context(), slow.request); done <- err }()
	waitForQueuedCalls(t, server.writes["primary"], 1)
	result := make(chan batchResult[*sink.WriteResponse], 1)
	go func() {
		response, err := server.Write(t.Context(), fast.request)
		completed := batchResult[*sink.WriteResponse]{response: response, err: err}
		result <- completed
	}()
	for range 2 {
		event := awaitCompletion(t, backend.events)
		if len(event.keys) != 1 || !event.visible {
			t.Fatalf("different datasets entered a shared storage bulk: %+v", event)
		}
	}
	completed := awaitCompletion(t, result)
	if completed.err != nil || completed.response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("independent dataset blocked by refresh: %+v", completed)
	}
	select {
	case err := <-done:
		t.Fatalf("slow dataset returned before visibility: %v", err)
	default:
	}
}
