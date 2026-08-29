package kafka_test

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaPublisherWorkerAppliesAsyncMutations(t *testing.T) {
	const topic = "sink-mutations"
	const deadLetterTopic = "sink-mutations.dlq"
	numBrokers := kfake.NumBrokers(1)
	seedTopics := kfake.SeedTopics(3, topic, deadLetterTopic)
	cluster, err := kfake.NewCluster(numBrokers, seedTopics)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	publisherOptions := kafka.PublisherOptions{
		Brokers: cluster.ListenAddrs(),
		Topic:   topic,
	}
	publisher, err := kafka.NewPublisher(publisherOptions)
	if err != nil {
		t.Fatalf("kafka.NewPublisher() error = %v", err)
	}
	t.Cleanup(publisher.Close)
	pingContext, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := publisher.Ping(pingContext); err != nil {
		t.Fatalf("Publisher.Ping() error = %v", err)
	}

	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	serverOptions := service.Options{
		Storage:   store,
		Lua:       luaEngine,
		Publisher: publisher,
	}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("worker.NewProcessor() error = %v", err)
	}
	workerOptions := kafka.WorkerOptions{
		Brokers:         cluster.ListenAddrs(),
		Topic:           topic,
		GroupID:         "sink-test-worker",
		DeadLetterTopic: deadLetterTopic,
		Handler:         processor,
		RetryBackoff:    time.Millisecond,
		MaxRetryBackoff: 5 * time.Millisecond,
	}
	kafkaWorker, err := kafka.NewWorker(workerOptions)
	if err != nil {
		t.Fatalf("kafka.NewWorker() error = %v", err)
	}
	workerContext, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		workerCancel()
		kafkaWorker.Close()
	})
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- kafkaWorker.Run(workerContext)
	}()

	address := kafkaAddress("record-1")
	document := &sink.Document{Json: []byte(`{"value":1}`)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	writeRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.WriteOperation{operation},
	}
	writeResponse, err := server.Write(context.Background(), writeRequest)
	if err != nil {
		t.Fatalf("Write(async) error = %v", err)
	}
	if writeResponse.Results[0].Status != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
		t.Fatalf("Write(async) status = %v", writeResponse.Results[0].Status)
	}
	waitForReadStatus(t, store, storage.ReadStatusFound)

	mergeSource := []byte(`return function(current, incoming, context) current.value = current.value + incoming.value return current end`)
	digest := sha256.Sum256(mergeSource)
	programReference := &sink.LuaProgram{Sha256: digest[:]}
	program := &sink.LuaProgram{Source: mergeSource, Sha256: digest[:]}
	incoming := &sink.Document{Json: []byte(`{"value":1}`)}
	merge := &sink.MergeOperation{
		IncomingDocument:    incoming,
		MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
		LuaProgram:          programReference,
	}
	mergeOperation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Merge{Merge: merge}}
	mergeRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.WriteOperation{mergeOperation},
		LuaPrograms:    []*sink.LuaProgram{program},
	}
	mergeResponse, err := server.Write(context.Background(), mergeRequest)
	if err != nil {
		t.Fatalf("Write(async merge) error = %v", err)
	}
	if mergeResponse.Results[0].Status != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
		t.Fatalf("Write(async merge) status = %v", mergeResponse.Results[0].Status)
	}
	waitForDocumentData(t, store, `{"value":2}`)

	rawClientOptions := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}
	rawClient, err := kgo.NewClient(rawClientOptions...)
	if err != nil {
		t.Fatalf("kgo.NewClient(raw producer) error = %v", err)
	}
	t.Cleanup(rawClient.Close)
	malformed := &kgo.Record{Topic: topic, Key: []byte("malformed"), Value: []byte("not-a-sink-envelope")}
	if err := rawClient.ProduceSync(t.Context(), malformed).FirstErr(); err != nil {
		t.Fatalf("ProduceSync(malformed) error = %v", err)
	}
	dlqOptions := []kgo.Opt{
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(deadLetterTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	dlqClient, err := kgo.NewClient(dlqOptions...)
	if err != nil {
		t.Fatalf("kgo.NewClient(DLQ) error = %v", err)
	}
	t.Cleanup(dlqClient.Close)
	waitForDeadLetter(t, dlqClient)

	deleteOperation := &sink.DeleteOperation{Address: address}
	deleteRequest := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.DeleteOperation{deleteOperation},
	}
	deleteResponse, err := server.Delete(context.Background(), deleteRequest)
	if err != nil {
		t.Fatalf("Delete(async) error = %v", err)
	}
	if deleteResponse.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_ACCEPTED {
		t.Fatalf("Delete(async) status = %v", deleteResponse.Results[0].Status)
	}
	waitForReadStatus(t, store, storage.ReadStatusNotFound)

	workerCancel()
	select {
	case runErr := <-workerDone:
		if runErr != nil {
			t.Fatalf("Worker.Run() error = %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Worker.Run() did not stop")
	}
}

func waitForDocumentData(t *testing.T, store *memory.Store, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request := storage.ReadRequest{
			Operations: []storage.ReadOperation{{Address: kafkaStorageAddress("record-1")}},
		}
		response, err := store.Read(ctx, request)
		if err != nil {
			t.Fatalf("Store.Read() error = %v", err)
		}
		if response.Results[0].Status == storage.ReadStatusFound && string(response.Results[0].Document.JSON) == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Store.Read() document = %s, want %s", response.Results[0].Document.JSON, want)
		case <-ticker.C:
		}
	}
}

func waitForDeadLetter(t *testing.T, client *kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		fetches := client.PollRecords(ctx, 1)
		if fetches.Err() != nil {
			t.Fatalf("poll dead-letter topic: %v", fetches.Err())
		}
		records := fetches.Records()
		if len(records) > 0 {
			if string(records[0].Value) != "not-a-sink-envelope" {
				t.Fatalf("dead-letter value = %q", records[0].Value)
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatal("dead-letter record was not published")
		}
	}
}

func waitForReadStatus(t *testing.T, store *memory.Store, want storage.ReadStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request := storage.ReadRequest{
			Operations: []storage.ReadOperation{{Address: kafkaStorageAddress("record-1")}},
		}
		response, err := store.Read(ctx, request)
		if err != nil {
			t.Fatalf("Store.Read() error = %v", err)
		}
		if response.Results[0].Status == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Store.Read() status = %v, want %v", response.Results[0].Status, want)
		case <-ticker.C:
		}
	}
}

func kafkaAddress(key string) *sink.RecordAddress {
	recordKey := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: key}}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       recordKey,
	}
	return address
}

func kafkaStorageAddress(key string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       storage.Key{Type: "string", Data: []byte(key)},
	}
	return address
}
