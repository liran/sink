package service_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func returningIncrement(key string) *sink.WriteRequest {
	request := mergeWriteRequest(key, "1")
	request.Operations[0].ReturnDocument = true
	request.Operations[0].GetMerge().MissingDocumentMode = sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE
	return request
}

func returnedCounter(t *testing.T, result *sink.WriteResult) int {
	t.Helper()
	if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || result.GetDocument() == nil {
		t.Fatalf("expected committed document: %v", result)
	}
	var document struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(result.GetDocument().GetPayload(), &document); err != nil {
		t.Fatal(err)
	}
	return document.Value
}

func TestWriteReturningSeparatesSameRecordCommits(t *testing.T) {
	store := memory.New()
	server := newTestServer(t, store, nil)
	first := returningIncrement("quota")
	second := returningIncrement("quota")
	first.Operations = append(first.Operations, second.Operations[0])
	response, err := server.Write(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if returnedCounter(t, response.Results[0]) != 1 || returnedCounter(t, response.Results[1]) != 2 {
		t.Fatalf("return values=%v", response)
	}
	if bytes.Equal(response.Results[0].GetRevision().GetData(), response.Results[1].GetRevision().GetData()) {
		t.Fatal("returning operations shared a commit")
	}
	read, err := server.Read(t.Context(), readRequest("quota"))
	if err != nil || string(read.Results[0].Document.Payload) != `{"value":2}` {
		t.Fatalf("stored=%v err=%v", read, err)
	}
}

func TestBatchedReturningIncrementsHaveDistinctResults(t *testing.T) {
	store := memory.New()
	server := newBatchingTestServer(t, store, nil, 1000)
	defer server.Close()
	const callers = 32
	results := make(chan *sink.WriteResult, callers)
	errors := make(chan error, callers)
	var workers sync.WaitGroup
	for range callers {
		workers.Go(func() {
			request := returningIncrement("concurrent-quota")
			response, err := server.Write(t.Context(), request)
			if err != nil {
				errors <- err
				return
			}
			results <- response.Results[0]
		})
	}
	workers.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	values := make([]int, 0, callers)
	for result := range results {
		values = append(values, returnedCounter(t, result))
	}
	sort.Ints(values)
	if len(values) != callers {
		t.Fatalf("received %d results", len(values))
	}
	for index, value := range values {
		if value != index+1 {
			t.Fatalf("returned values=%v", values)
		}
	}
}

func TestWriteReturningRejectsAsyncBeforePublishing(t *testing.T) {
	publisher := &recordingPublisher{}
	store := memory.New()
	server := newTestServer(t, store, publisher)
	request := returningIncrement("quota")
	request.CompletionMode = sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	_, err := server.Write(t.Context(), request)
	if status.Code(err) != codes.InvalidArgument || len(publisher.mutations) != 0 {
		t.Fatalf("async request published=%d err=%v", len(publisher.mutations), err)
	}
}

func TestWriteReturningBudgetRejectsBeforeCommit(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: store, Lua: lua, MaxReadBytes: 400}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	first := putWriteOperation("first", strings.Repeat("x", 100))
	first.ReturnDocument = true
	second := putWriteOperation("second", strings.Repeat("x", 100))
	second.ReturnDocument = true
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations: []*sink.WriteOperation{first, second}}
	response, err := server.Write(t.Context(), request)
	if err != nil || response.Results[0].GetDocument() == nil || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatalf("response=%v err=%v", response, err)
	}
	read, err := server.Read(t.Context(), readRequest("second"))
	if err != nil || read.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("over-budget operation committed: %v err=%v", read, err)
	}
}

func TestWriteReturningOnlyAttachesSuccessfulCASResult(t *testing.T) {
	backend := &retryTimeStorage{}
	server := newTestServer(t, backend, nil)
	request := returningIncrement("retry")
	response, err := server.Write(t.Context(), request)
	if err != nil || returnedCounter(t, response.Results[0]) != 1 || len(backend.documents) != 2 {
		t.Fatalf("CAS result=%v attempts=%d err=%v", response, len(backend.documents), err)
	}
	conflicting := conflictStorage{}
	server = newTestServer(t, conflicting, nil)
	response, err = server.Write(t.Context(), request)
	if err != nil || response.Results[0].GetDocument() != nil || response.Results[0].GetStatus() == sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("failed CAS returned a document: %v err=%v", response, err)
	}
}
