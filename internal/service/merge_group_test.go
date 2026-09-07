package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func foldingMerge(key, source, payload string, missing sink.MissingDocumentMode) *sink.WriteOperation {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(payload)}
	program := &sink.LuaProgram{Source: []byte(source)}
	mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: missing}
	action := &sink.WriteOperation_Merge{Merge: mutation}
	operation := &sink.WriteOperation{Address: protoAddress(key), Action: action}
	return operation
}

func TestMergeFoldingAcrossRPCsPreservesResponseBoundaries(t *testing.T) {
	backend := memory.New()
	observed := &countingStorage{backend: backend}
	core := newTestServer(t, observed, nil)
	const callers = 16
	opts := service.BatchingOptions{StoreNames: []string{"primary"}, MaxWait: time.Second, MaxOperations: callers * 2}
	server, err := service.NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	responses := make(chan *sink.WriteResponse, callers)
	errs := make(chan error, callers)
	for caller := range callers {
		go func() {
			hot := foldingMerge("hot", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			cold := foldingMerge(fmt.Sprintf("cold-%d", caller), incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			request := foldingRequest(hot, cold)
			response, err := server.Write(ctx, request)
			responses <- response
			errs <- err
		}()
	}
	var revision []byte
	for range callers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		response := <-responses
		if len(response.Results) != 2 {
			t.Fatalf("response boundary: %v", response)
		}
		for index, result := range response.Results {
			if result.OperationIndex != uint32(index) || result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("result boundary: %v", result)
			}
		}
		if revision == nil {
			revision = response.Results[0].GetRevision().GetData()
		} else if !bytes.Equal(revision, response.Results[0].GetRevision().GetData()) {
			t.Fatal("hot writes were not folded across RPCs")
		}
	}
	if observed.readCalls.Load() != 1 || observed.writeCalls.Load() != 1 || observed.maxWriteOperations.Load() != callers+1 || !observed.writeWaitVisible.Load() {
		t.Fatalf("batch reads=%d writes=%d docs=%d visible=%t", observed.readCalls.Load(), observed.writeCalls.Load(), observed.maxWriteOperations.Load(), observed.writeWaitVisible.Load())
	}
	if foldingValue(t, backend, "hot") != callers {
		t.Fatal("lost hot document updates")
	}
}

func TestMergeFoldingFullAddressIsolation(t *testing.T) {
	backend := memory.New()
	observed := &countingStorage{backend: backend}
	server := newTestServer(t, observed, nil)
	var operations []*sink.WriteOperation
	for _, dataset := range []string{"products", "offers"} {
		for range 2 {
			operation := foldingMerge("same-key", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			operation.Address.Dataset = dataset
			operations = append(operations, operation)
		}
	}
	response, err := server.Write(t.Context(), foldingRequest(operations...))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(result)
		}
	}
	if observed.maxWriteOperations.Load() != 2 || foldingValue(t, backend, "same-key") != 2 {
		t.Fatal("grouping crossed a dataset boundary")
	}
}

func TestMergeFoldingBoundsConflictAttemptsAndOutput(t *testing.T) {
	for _, scenario := range []string{"conflict", "output"} {
		t.Run(scenario, func(t *testing.T) {
			var backend storage.Storage = conflictStorage{}
			if scenario == "output" {
				backend = memory.New()
			}
			observed := &countingStorage{backend: backend}
			luaOptions := merge.LuaOptions{}
			engine, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				t.Fatal(err)
			}
			options := service.Options{Storage: observed, Lua: engine, MaxMergeAttempts: 2, MaxReadBytes: 140}
			server, err := service.New(options)
			if err != nil {
				t.Fatal(err)
			}
			source := incrementLua
			if scenario == "output" {
				source = `return function() return {value="abcdefghijklmnopqrstuvwxyz0123456789"} end`
			}
			first := foldingMerge("counter", source, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			second := foldingMerge("counter", source, `{"value":2}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			response, err := server.Write(t.Context(), foldingRequest(first, second))
			if err != nil {
				t.Fatal(err)
			}
			want := sink.FailureCode_FAILURE_CODE_CONFLICT
			wantWrites := int64(2)
			if scenario == "output" {
				want = sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
				wantWrites = 0
			}
			for _, result := range response.Results {
				if result.GetFailure().GetCode() != want || result.Status == sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatalf("bounded result = %v, want %v", result, want)
				}
			}
			if observed.writeCalls.Load() != wantWrites {
				t.Fatalf("writes = %d, want %d", observed.writeCalls.Load(), wantWrites)
			}
		})
	}
}

func foldingRequest(operations ...*sink.WriteOperation) *sink.WriteRequest {
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: operations}
	return request
}

func foldingValue(t testing.TB, backend storage.Storage, key string) int {
	t.Helper()
	operation := storage.ReadOperation{Address: storageAddress(key)}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
	response, err := backend.Read(context.Background(), request)
	if err != nil || len(response.Results) != 1 || response.Results[0].Status != storage.ReadStatusFound {
		t.Fatalf("read final document: %v, %+v", err, response)
	}
	var document struct{ Value int }
	if err := json.Unmarshal(response.Results[0].Document.Payload, &document); err != nil {
		t.Fatal(err)
	}
	return document.Value
}

func TestMergeFoldingPreservesOrderAndSharesCommitRevision(t *testing.T) {
	backend := memory.New()
	observed := &countingStorage{backend: backend}
	server := newTestServer(t, observed, nil)
	const appendDigit = `return function(current, incoming)
        current = current or {value=0}
        current.value = current.value * 10 + incoming.value
        return current
    end`
	var operations []*sink.WriteOperation
	for _, digit := range []int{1, 2, 3} {
		operation := foldingMerge("ordered", appendDigit, fmt.Sprintf(`{"value":%d}`, digit), sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
		operations = append(operations, operation)
	}
	response, err := server.Write(t.Context(), foldingRequest(operations...))
	if err != nil {
		t.Fatal(err)
	}
	if observed.readCalls.Load() != 1 || observed.writeCalls.Load() != 1 || observed.maxWriteOperations.Load() != 1 {
		t.Fatalf("backend work: reads=%d writes=%d documents=%d", observed.readCalls.Load(), observed.writeCalls.Load(), observed.maxWriteOperations.Load())
	}
	if !observed.writeWaitVisible.Load() {
		t.Fatal("final commit did not wait for visibility")
	}
	for index, result := range response.Results {
		if result.OperationIndex != uint32(index) || result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || result.Failure != nil {
			t.Fatalf("result %d: %v", index, result)
		}
		if len(result.GetRevision().GetData()) == 0 || !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatalf("result %d did not share the final commit revision", index)
		}
	}
	if got := foldingValue(t, backend, "ordered"); got != 123 {
		t.Fatalf("ordered value = %d", got)
	}
}

func TestMergeFoldingPutIsABarrier(t *testing.T) {
	backend := memory.New()
	observed := &countingStorage{backend: backend}
	server := newTestServer(t, observed, nil)
	first := foldingMerge("counter", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
	second := foldingMerge("counter", incrementLua, `{"value":2}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	put := putWriteOperation("counter", "unused")
	put.GetPut().Document.Payload = []byte(`{"value":20}`)
	third := foldingMerge("counter", incrementLua, `{"value":3}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	fourth := foldingMerge("counter", incrementLua, `{"value":4}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	response, err := server.Write(t.Context(), foldingRequest(first, second, put, third, fourth))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(result)
		}
	}
	if got := foldingValue(t, backend, "counter"); got != 27 {
		t.Fatalf("value across put barrier = %d", got)
	}
	if observed.readCalls.Load() != 2 || observed.writeCalls.Load() != 3 {
		t.Fatalf("barrier reads=%d writes=%d", observed.readCalls.Load(), observed.writeCalls.Load())
	}
	if bytes.Equal(response.Results[0].GetRevision().GetData(), response.Results[3].GetRevision().GetData()) {
		t.Fatal("merges across put shared a commit")
	}
}

func TestMergeFoldingRetainsIndividualLuaAndMissingFailures(t *testing.T) {
	backend := memory.New()
	observed := &countingStorage{backend: backend}
	server := newTestServer(t, observed, nil)
	missing := foldingMerge("counter", incrementLua, `{"value":100}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	create := foldingMerge("counter", incrementLua, `{"value":2}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
	bad := foldingMerge("counter", `return function(current) current.value=999; error("rejected") end`, `{}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	last := foldingMerge("counter", incrementLua, `{"value":4}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	response, err := server.Write(t.Context(), foldingRequest(missing, create, bad, last))
	if err != nil {
		t.Fatal(err)
	}
	want := []sink.WriteStatus{sink.WriteStatus_WRITE_STATUS_FAILED, sink.WriteStatus_WRITE_STATUS_APPLIED, sink.WriteStatus_WRITE_STATUS_FAILED, sink.WriteStatus_WRITE_STATUS_APPLIED}
	for index, result := range response.Results {
		if result.Status != want[index] {
			t.Fatalf("result %d = %v", index, result)
		}
	}
	if response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_NOT_FOUND || response.Results[2].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
		t.Fatalf("individual failures = %v", response.Results)
	}
	if observed.writeCalls.Load() != 1 || foldingValue(t, backend, "counter") != 6 {
		t.Fatal("failed Lua changed the working document or split the commit")
	}
}

// Inject failures at the storage boundary while retaining actual revision checks.
type foldingFaultStorage struct {
	storage.Storage
	backend *memory.Store
	fault   string
	writes  int
	reads   int
}

func (s *foldingFaultStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads++
	return s.backend.Read(ctx, req)
}

func (s *foldingFaultStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes++
	if s.fault == "conflict" && s.writes == 1 {
		seed := memory.SeedRequest{Address: req.Operations[0].Address, Document: storageJSONDocument(`{"value":10}`)}
		s.backend.Seed(seed)
	}
	if s.fault == "reject" {
		cause := errors.New("backend unavailable before commit")
		failure := storage.WriteResult{Status: storage.WriteStatusFailed, Err: storage.BackendError(cause)}
		response := storage.WriteResponse{Results: []storage.WriteResult{failure}}
		return response, nil
	}
	response, err := s.backend.Write(ctx, req)
	if s.fault == "lost acknowledgement" && err == nil {
		return response, errors.New("connection lost after commit")
	}
	return response, err
}

func TestMergeFoldingRecomputesWholeChainAfterConflict(t *testing.T) {
	backend := memory.New()
	observed := &foldingFaultStorage{Storage: backend, backend: backend, fault: "conflict"}
	server := newTestServer(t, observed, nil)
	first := foldingMerge("counter", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
	conditional := foldingMerge("counter", `return function(current)
        if current.value < 10 then error("base too small") end
        current.value = current.value + 10
        return current
    end`, `{}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
	response, err := server.Write(t.Context(), foldingRequest(first, conditional))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || result.Failure != nil {
			t.Fatalf("stale attempt result survived recomputation: %v", result)
		}
	}
	if got := foldingValue(t, backend, "counter"); got != 21 {
		t.Fatalf("rebased value = %d", got)
	}
	if observed.reads != 2 || observed.writes != 2 {
		t.Fatalf("conflict reads=%d writes=%d", observed.reads, observed.writes)
	}
}

func TestMergeFoldingDoesNotAcknowledgeOrReplayFailedCommit(t *testing.T) {
	for _, fault := range []string{"reject", "lost acknowledgement"} {
		t.Run(fault, func(t *testing.T) {
			backend := memory.New()
			observed := &foldingFaultStorage{Storage: backend, backend: backend, fault: fault}
			server := newTestServer(t, observed, nil)
			first := foldingMerge("counter", incrementLua, `{"value":2}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			bad := foldingMerge("counter", `return function() error("dependent failure") end`, `{}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
			last := foldingMerge("counter", incrementLua, `{"value":3}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
			response, err := server.Write(t.Context(), foldingRequest(first, bad, last))
			if fault == "reject" {
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range response.Results {
					if result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_UNAVAILABLE || len(result.GetRevision().GetData()) != 0 {
						t.Fatalf("acknowledged an uncommitted outcome: %v", result)
					}
				}
			} else {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("lost acknowledgement error = %v", err)
				}
				if foldingValue(t, backend, "counter") != 5 {
					t.Fatal("ambiguous write was replayed")
				}
			}
			if observed.writes != 1 {
				t.Fatalf("retried uncertain/failed commit %d times", observed.writes)
			}
		})
	}
}

func TestMergeFoldingConcurrentServersDoNotLoseUpdates(t *testing.T) {
	backend := memory.New()
	const writers, perRequest = 4, 8
	var wg sync.WaitGroup
	errorsChannel := make(chan error, writers)
	for range writers {
		server := newTestServer(t, backend, nil)
		wg.Go(func() {
			var operations []*sink.WriteOperation
			for range perRequest {
				operation := foldingMerge("counter", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
				operations = append(operations, operation)
			}
			response, err := server.Write(t.Context(), foldingRequest(operations...))
			if err == nil {
				for _, result := range response.Results {
					if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
						err = fmt.Errorf("merge failed: %v", result)
						break
					}
				}
			}
			errorsChannel <- err
		})
	}
	wg.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := foldingValue(t, backend, "counter"); got != writers*perRequest {
		t.Fatalf("concurrent value = %d", got)
	}
}

type foldingVisibilityStorage struct {
	storage.Storage
	started chan struct{}
	release chan struct{}
}

func (s *foldingVisibilityStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response, err := s.Storage.Write(ctx, req)
	if err != nil || !req.WaitUntilVisible {
		return response, err
	}
	close(s.started)
	select {
	case <-s.release:
		return response, nil
	case <-ctx.Done():
		return response, ctx.Err()
	}
}

func TestMergeFoldingWaitsForVisibilityWhileOneCallerCancels(t *testing.T) {
	backend := memory.New()
	observed := &foldingVisibilityStorage{Storage: backend, started: make(chan struct{}), release: make(chan struct{})}
	core := newTestServer(t, observed, nil)
	opts := service.BatchingOptions{StoreNames: []string{"primary"}, MaxWait: time.Second, MaxOperations: 2}
	server, err := service.NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cancelledCtx, cancelCaller := context.WithCancel(ctx)
	defer cancelCaller()
	callerResults := make(chan error, 1)
	liveResults := make(chan error, 1)
	for _, caller := range []struct {
		ctx    context.Context
		result chan error
	}{{ctx: cancelledCtx, result: callerResults}, {ctx: ctx, result: liveResults}} {
		go func() {
			operation := foldingMerge("counter", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
			response, err := server.Write(caller.ctx, foldingRequest(operation))
			if err == nil && response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				err = fmt.Errorf("unexpected write result: %v", response)
			}
			caller.result <- err
		}()
	}
	select {
	case <-observed.started:
	case <-ctx.Done():
		t.Fatal("commit did not start")
	}
	select {
	case err := <-liveResults:
		t.Fatalf("returned before final state was visible: %v", err)
	default:
	}
	cancelCaller()
	if err := <-callerResults; status.Code(err) != codes.Canceled {
		t.Fatalf("canceled caller result = %v", err)
	}
	close(observed.release)
	if err := <-liveResults; err != nil {
		t.Fatalf("one caller canceled shared work: %v", err)
	}
	if foldingValue(t, backend, "counter") != 2 {
		t.Fatal("lost a dispatched merge intent")
	}
}
