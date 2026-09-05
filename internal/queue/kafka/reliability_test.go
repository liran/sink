package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
	"github.com/twmb/franz-go/pkg/kfake"
)

func reliabilityProcessor(t *testing.T, backend storage.Storage) *worker.Processor {
	t.Helper()
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := service.Options{Storage: backend, Lua: lua}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func reliabilityAddress() *sink.RecordAddress {
	kind := &sink.RecordKey_StringValue{StringValue: "reliability-record"}
	key := &sink.RecordKey{Kind: kind}
	address := &sink.RecordAddress{Store: "primary", Namespace: "reliability", Dataset: "records", Key: key}
	return address
}

func reliabilityPut(mode sink.WriteMode, payload string) queue.Mutation {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(payload)}
	put := &sink.PutOperation{Mode: mode, Document: document}
	action := &sink.WriteOperation_Put{Put: put}
	operation := &sink.WriteOperation{Address: reliabilityAddress(), Action: action}
	mutation := queue.Mutation{Write: operation}
	return mutation
}

func TestCreateReplayMustNotDiscardFollowingValidMutation(t *testing.T) {
	backend := memory.New()
	processor := reliabilityProcessor(t, backend)
	create := reliabilityPut(sink.WriteMode_WRITE_MODE_CREATE, `{"value":0}`)
	if err := processor.Handle(t.Context(), create); err != nil {
		t.Fatal(err)
	}
	// A crash now leaves CREATE applied, but its Kafka offset uncommitted.
	update := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
	mutations := []queue.Mutation{create, update}
	results := processor.HandleBatch(t.Context(), mutations)
	t.Logf("replayed CREATE error=%v; following valid UPSERT error=%v", results[0], results[1])
	if results[1] != nil {
		t.Fatal("valid following UPSERT is classified as a permanent failure without executing; worker will send both records to DLQ")
	}
}

func TestOversizedKafkaRecordMustBePermanentFailure(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "reliability-mutations"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	publisherOptions := PublisherOptions{Brokers: cluster.ListenAddrs(), Topic: "reliability-mutations"}
	publisher, err := NewPublisher(publisherOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := service.Options{Storage: memory.New(), Lua: lua, Publisher: publisher}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":"`+strings.Repeat("x", 1100000)+`"}`)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sink.WriteOperation{mutation.Write}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := server.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	failure := response.Results[0].Failure
	if failure == nil {
		t.Fatal("oversized Kafka record unexpectedly succeeded")
	}
	t.Logf("encoded request=%d bytes; failure code=%s; retryable=%t; message=%s", request.SizeVT(), failure.Code, failure.Retryable, failure.Message)
	if failure.Retryable {
		t.Fatal("unchanged oversized message cannot succeed on retry but is advertised as retryable unavailable")
	}
}

type cancelDuringStorageWrite struct {
	storage.Storage
	cancel context.CancelFunc
	calls  int
}

func (s *cancelDuringStorageWrite) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.calls++
	s.cancel()
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	for index := range response.Results {
		response.Results[index].Status = storage.WriteStatusFailed
		response.Results[index].Err = storage.BackendError(ctx.Err())
	}
	return response, nil
}

func TestCancellationCannotQuarantineOrOvertakeQueuedWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend := &cancelDuringStorageWrite{Storage: memory.New(), cancel: cancel}
	processor := reliabilityProcessor(t, backend)
	first := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
	second := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":2}`)
	mutations := []queue.Mutation{first, second}
	results := processor.HandleBatch(ctx, mutations)
	for _, result := range results {
		if !retryableProcessingError(result) {
			t.Fatalf("cancellation became terminal: %v", result)
		}
	}
	if backend.calls != 1 {
		t.Fatal("later same-record work overtook a cancelled write")
	}
}
