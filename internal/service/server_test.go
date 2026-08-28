package service_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

const testMergeAttempts = 1000

type incrementMerger struct{}

func (incrementMerger) Merge(_ context.Context, req merge.Request) (merge.Result, error) {
	var emptyResult merge.Result
	current := 0
	if req.Current != nil {
		parsed, err := strconv.Atoi(string(req.Current.Data))
		if err != nil {
			return emptyResult, err
		}
		current = parsed
	}
	increment, err := strconv.Atoi(string(req.Incoming.Data))
	if err != nil {
		return emptyResult, err
	}
	document := storage.Document{
		ContentType: "text/plain",
		Data:        []byte(strconv.Itoa(current + increment)),
	}
	result := merge.Result{Document: document}
	return result, nil
}

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

func TestConcurrentMergeDoesNotLoseSuccessfulUpdates(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	seed := memory.SeedRequest{
		Address:  storageAddress("counter"),
		Document: storageDocument("0"),
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
	got, err := strconv.Atoi(string(readResponse.GetResults()[0].GetDocument().GetData()))
	if err != nil {
		t.Fatalf("parse final counter: %v", err)
	}
	if got != int(applied.Load()) {
		t.Fatalf("final counter = %d, successful writes = %d", got, applied.Load())
	}
	if got != writers {
		t.Fatalf("final counter = %d, want %d", got, writers)
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
	got := string(readResponse.GetResults()[0].GetDocument().GetData())
	if got != "second" {
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

	readResponse, err := server.Read(ctx, readRequest("async"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if readResponse.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("async write changed storage before worker execution")
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
	registry := merge.NewRegistry()
	profile := merge.Profile{Name: "increment", Version: 1}
	merger := incrementMerger{}
	if err := registry.Register(profile, merger); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	options := service.Options{
		Storage:          store,
		Merges:           registry,
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
	profile := &sink.MergeProfile{Name: "increment", Version: 1}
	incoming := protoDocument(increment)
	mergeOperation := &sink.MergeOperation{
		IncomingDocument:    incoming,
		Profile:             profile,
		MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
	}
	operation := &sink.WriteOperation{
		Address: protoAddress(key),
		Action:  &sink.WriteOperation_Merge{Merge: mergeOperation},
	}
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{operation},
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
	document := &sink.Document{
		ContentType: "text/plain",
		Data:        []byte(value),
	}
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

func storageDocument(value string) storage.Document {
	document := storage.Document{
		ContentType: "text/plain",
		Data:        []byte(value),
	}
	return document
}
