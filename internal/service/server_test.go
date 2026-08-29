package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unavailableStorage struct{}

func (unavailableStorage) Ping(context.Context) error {
	return nil
}

func (unavailableStorage) Read(_ context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	response := storage.ReadResponse{Results: make([]storage.ReadResult, len(req.Operations))}
	for index := range response.Results {
		cause := errors.New("storage connection reset")
		response.Results[index] = storage.ReadResult{
			Status: storage.ReadStatusFailed,
			Err:    storage.BackendError(cause),
		}
	}
	return response, nil
}

func (unavailableStorage) Write(_ context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	return response, nil
}

func (unavailableStorage) Delete(_ context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{Results: make([]storage.DeleteResult, len(req.Operations))}
	return response, nil
}

type visibilityStorage struct {
	backend           *memory.Store
	writeWaitVisible  bool
	deleteWaitVisible bool
}

func (s *visibilityStorage) Ping(ctx context.Context) error {
	return s.backend.Ping(ctx)
}

func (s *visibilityStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	return s.backend.Read(ctx, req)
}

func (s *visibilityStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writeWaitVisible = req.WaitUntilVisible
	return s.backend.Write(ctx, req)
}

func (s *visibilityStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	s.deleteWaitVisible = req.WaitUntilVisible
	return s.backend.Delete(ctx, req)
}

const testMergeAttempts = 1000

const incrementLua = `
return function(current, incoming, context)
    current = current or {value = 0}
    current.value = current.value + incoming.value
    return current
end`

type recordingPublisher struct {
	mu        sync.Mutex
	mutations []queue.Mutation
}

func (p *recordingPublisher) Publish(
	_ context.Context,
	req queue.PublishRequest,
) (queue.PublishResponse, error) {
	p.mu.Lock()
	p.mutations = append(p.mutations, req.Mutations...)
	p.mu.Unlock()

	response := queue.PublishResponse{
		Results: make([]queue.PublishResult, len(req.Mutations)),
	}
	for index := range response.Results {
		response.Results[index].Status = queue.PublishStatusAccepted
	}
	return response, nil
}

func (p *recordingPublisher) mutationCount() int {
	p.mu.Lock()
	count := len(p.mutations)
	p.mu.Unlock()
	return count
}

func (p *recordingPublisher) mutation(index int) queue.Mutation {
	p.mu.Lock()
	mutation := p.mutations[index]
	p.mu.Unlock()
	return mutation
}

func TestConcurrentMergeDoesNotLoseSuccessfulUpdates(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	seed := memory.SeedRequest{
		Address:  storageAddress("counter"),
		Document: storageJSONDocument(`{"value":0}`),
	}
	store.Seed(seed)
	server := newTestServer(t, store, nil)

	const writers = 100
	var applied atomic.Int64
	var waitGroup sync.WaitGroup
	waitGroup.Add(writers)
	for range writers {
		go func() {
			defer waitGroup.Done()
			request := mergeWriteRequest("counter", "1")
			response, err := server.Write(ctx, request)
			if err != nil {
				t.Errorf("Write(merge) error = %v", err)
				return
			}
			if response.GetResults()[0].GetStatus() == sink.WriteStatus_WRITE_STATUS_APPLIED {
				applied.Add(1)
				return
			}
			t.Errorf("Write(merge) status = %v, failure = %v", response.GetResults()[0].GetStatus(), response.GetResults()[0].GetFailure())
		}()
	}
	waitGroup.Wait()

	readRequest := readRequest("counter")
	readResponse, err := server.Read(ctx, readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	var final struct {
		Value int `json:"value"`
	}
	err = json.Unmarshal(readResponse.GetResults()[0].GetDocument().GetJson(), &final)
	if err != nil {
		t.Fatalf("decode final counter: %v", err)
	}
	if final.Value != int(applied.Load()) {
		t.Fatalf("final counter = %d, successful writes = %d", final.Value, applied.Load())
	}
	if final.Value != writers {
		t.Fatalf("final counter = %d, want %d", final.Value, writers)
	}
}

func TestWaitUntilVisiblePropagatesThroughRouter(t *testing.T) {
	backend := &visibilityStorage{backend: memory.New()}
	backends := map[string]storage.Storage{"primary": backend}
	router, err := storage.NewRouter(backends)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	server := newTestServer(t, router, nil)

	writeRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
		Operations:     []*sink.WriteOperation{putWriteOperation("visible", "value")},
	}
	written, err := server.Write(t.Context(), writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || !backend.writeWaitVisible {
		t.Fatalf("Write() result = %+v, wait visible = %t", written.GetResults()[0], backend.writeWaitVisible)
	}
	mergeSeed := memory.SeedRequest{
		Address:  storageAddress("merge-visible"),
		Document: storageJSONDocument(`{"value":0}`),
	}
	backend.backend.Seed(mergeSeed)
	backend.writeWaitVisible = false
	mergeRequest := mergeWriteRequest("merge-visible", "1")
	mergeRequest.CompletionMode = sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
	merged, err := server.Write(t.Context(), mergeRequest)
	if err != nil {
		t.Fatalf("Write(merge) error = %v", err)
	}
	if merged.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || !backend.writeWaitVisible {
		t.Fatalf("Write(merge) result = %+v, wait visible = %t", merged.GetResults()[0], backend.writeWaitVisible)
	}

	deleteOperation := &sink.DeleteOperation{Address: protoAddress("visible")}
	deleteRequest := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
		Operations:     []*sink.DeleteOperation{deleteOperation},
	}
	deleted, err := server.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.GetResults()[0].GetStatus() != sink.DeleteStatus_DELETE_STATUS_APPLIED || !backend.deleteWaitVisible {
		t.Fatalf("Delete() result = %+v, wait visible = %t", deleted.GetResults()[0], backend.deleteWaitVisible)
	}
}

func TestReadPreservesRetryableBackendFailureClassification(t *testing.T) {
	server := newTestServer(t, unavailableStorage{}, nil)
	response, err := server.Read(t.Context(), readRequest("record"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	failure := response.GetResults()[0].GetFailure()
	if failure.GetCode() != sink.FailureCode_FAILURE_CODE_UNAVAILABLE || !failure.GetRetryable() {
		t.Fatalf("Read() failure = %+v", failure)
	}
}

func TestWritePreservesSameRecordRequestOrder(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	server := newTestServer(t, store, nil)

	first := putWriteOperation("ordered", "first")
	second := putWriteOperation("ordered", "second")
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{first, second},
	}
	response, err := server.Write(ctx, request)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("Write() result %d status = %v", index, result.GetStatus())
		}
	}

	readResponse, err := server.Read(ctx, readRequest("ordered"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	got := string(readResponse.GetResults()[0].GetDocument().GetJson())
	if got != `{"value":"second"}` {
		t.Fatalf("stored document = %q, want second", got)
	}
}

func TestAsyncWritePublishesOriginalMergeIntent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	publisher := &recordingPublisher{}
	server := newTestServer(t, store, publisher)
	request := mergeWriteRequest("async", "1")
	request.CompletionMode = sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED

	response, err := server.Write(ctx, request)
	if err != nil {
		t.Fatalf("Write(async) error = %v", err)
	}
	if response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
		t.Fatalf("Write(async) status = %v", response.GetResults()[0].GetStatus())
	}
	if publisher.mutationCount() != 1 {
		t.Fatalf("published mutations = %d, want 1", publisher.mutationCount())
	}
	published := publisher.mutation(0)
	publishedProgram := published.Write.GetMerge().GetLuaProgram()
	if string(publishedProgram.GetSource()) != incrementLua || len(publishedProgram.GetSha256()) != sha256.Size {
		t.Fatalf("published Lua program = %+v", publishedProgram)
	}

	readResponse, err := server.Read(ctx, readRequest("async"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if readResponse.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("async write changed storage before worker execution")
	}
}

func TestLuaMergeReturnsSpecificFailureCodes(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		seed       storage.Document
		failure    sink.FailureCode
		seedRecord bool
	}{
		{
			name:    "invalid program",
			source:  "return @",
			failure: sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT,
		},
		{
			name:       "runtime error",
			source:     `return function(current, incoming, context) error("bad rule") end`,
			seed:       storageJSONDocument(`{"value":0}`),
			failure:    sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT,
			seedRecord: true,
		},
		{
			name:       "call depth limit",
			source:     `return function(current, incoming, context) local function recurse() return 1 + recurse() end return recurse() end`,
			seed:       storageJSONDocument(`{"value":0}`),
			failure:    sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED,
			seedRecord: true,
		},
		{
			name:       "invalid stored document",
			source:     `return function(current, incoming, context) return incoming end`,
			seed:       storageDocument("bad"),
			failure:    sink.FailureCode_FAILURE_CODE_INTERNAL,
			seedRecord: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := memory.New()
			if test.seedRecord {
				seed := memory.SeedRequest{Address: storageAddress(test.name), Document: test.seed}
				store.Seed(seed)
			}
			server := newTestServer(t, store, nil)
			request := mergeWriteRequestWithSource(test.name, "1", test.source)
			response, err := server.Write(t.Context(), request)
			if err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			result := response.GetResults()[0]
			if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != test.failure {
				t.Fatalf("Write() result = %+v", result)
			}
		})
	}
}

func TestWriteRejectsInvalidLuaProgramDeclarations(t *testing.T) {
	store := memory.New()
	server := newTestServer(t, store, nil)

	t.Run("undeclared reference", func(t *testing.T) {
		request := mergeWriteRequest("undeclared", "1")
		request.LuaPrograms = nil
		response, err := server.Write(t.Context(), request)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		failure := response.GetResults()[0].GetFailure()
		if failure.GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
			t.Fatalf("Write() failure = %+v", failure)
		}
	})

	t.Run("mismatched declaration digest", func(t *testing.T) {
		request := mergeWriteRequest("mismatch", "1")
		request.LuaPrograms[0].Sha256[0]++
		_, err := server.Write(t.Context(), request)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Write() code = %s, want InvalidArgument", status.Code(err))
		}
	})
}

func TestWriteRejectsInvalidDateTimeMetadata(t *testing.T) {
	store := memory.New()
	server := newTestServer(t, store, nil)
	operation := putWriteOperation("invalid-date-time", "value")
	operation.GetPut().Document = &sink.Document{
		Json:          []byte(`{"created_at":"not-a-date-time"}`),
		DateTimePaths: []string{"/created_at"},
	}
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{operation},
	}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	result := response.GetResults()[0]
	if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_FAILED ||
		result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
		t.Fatalf("Write() result = %+v", result)
	}
}

func TestDeleteIsHardAndMissingDeleteSucceeds(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	server := newTestServer(t, store, nil)

	writeRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{putWriteOperation("delete", "value")},
	}
	_, err := server.Write(ctx, writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	deleteOperation := &sink.DeleteOperation{Address: protoAddress("delete")}
	deleteRequest := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.DeleteOperation{deleteOperation, deleteOperation},
	}
	deleteResponse, err := server.Delete(ctx, deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	for index, result := range deleteResponse.GetResults() {
		if result.GetStatus() != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatalf("Delete() result %d status = %v", index, result.GetStatus())
		}
	}

	readResponse, err := server.Read(ctx, readRequest("delete"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if readResponse.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("deleted record status = %v", readResponse.GetResults()[0].GetStatus())
	}
}

func newTestServer(t *testing.T, store storage.Storage, publisher queue.Publisher) *service.Server {
	t.Helper()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	options := service.Options{
		Storage:          store,
		Lua:              luaEngine,
		Publisher:        publisher,
		MaxMergeAttempts: testMergeAttempts,
	}
	server, err := service.New(options)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	return server
}

func mergeWriteRequest(key string, increment string) *sink.WriteRequest {
	return mergeWriteRequestWithSource(key, increment, incrementLua)
}

func mergeWriteRequestWithSource(key string, increment string, source string) *sink.WriteRequest {
	incoming := &sink.Document{Json: []byte(`{"value":` + increment + `}`)}
	digest := sha256.Sum256([]byte(source))
	programReference := &sink.LuaProgram{Sha256: digest[:]}
	program := &sink.LuaProgram{Source: []byte(source), Sha256: digest[:]}
	mergeOperation := &sink.MergeOperation{
		IncomingDocument:    incoming,
		LuaProgram:          programReference,
		MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
	}
	operation := &sink.WriteOperation{
		Address: protoAddress(key),
		Action:  &sink.WriteOperation_Merge{Merge: mergeOperation},
	}
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{operation},
		LuaPrograms:    []*sink.LuaProgram{program},
	}
	return request
}

func putWriteOperation(key string, value string) *sink.WriteOperation {
	put := &sink.PutOperation{
		Document: protoDocument(value),
		Mode:     sink.WriteMode_WRITE_MODE_UPSERT,
	}
	operation := &sink.WriteOperation{
		Address: protoAddress(key),
		Action:  &sink.WriteOperation_Put{Put: put},
	}
	return operation
}

func readRequest(key string) *sink.ReadRequest {
	operation := &sink.ReadOperation{Address: protoAddress(key)}
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
	return request
}

func protoAddress(key string) *sink.RecordAddress {
	recordKey := &sink.RecordKey{
		Kind: &sink.RecordKey_StringValue{StringValue: key},
	}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   "products",
		Key:       recordKey,
	}
	return address
}

func protoDocument(value string) *sink.Document {
	document := &sink.Document{Json: []byte(`{"value":"` + value + `"}`)}
	return document
}

func storageAddress(key string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   "products",
		Key: storage.Key{
			Type: "string",
			Data: []byte(key),
		},
	}
	return address
}

func storageJSONDocument(value string) storage.Document {
	document := storage.Document{JSON: []byte(value)}
	return document
}

func storageDocument(value string) storage.Document {
	document := storage.Document{JSON: []byte(value)}
	return document
}
