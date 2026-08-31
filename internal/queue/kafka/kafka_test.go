package kafka_test

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type blockingHandler struct {
	started chan struct{}
	once    sync.Once
}

func (h *blockingHandler) HandleBatch(ctx context.Context, mutations []queue.Mutation) []error {
	h.once.Do(func() {
		close(h.started)
	})
	<-ctx.Done()
	results := make([]error, len(mutations))
	for index := range results {
		results[index] = ctx.Err()
	}
	return results
}

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
		Store:           "primary",
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
	document := kafkaJSONDocument(`{"value":1}`)
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

	mergeSource := []byte(`return function(current, incoming) current.value = current.value + incoming.value return current end`)
	digest := sha256.Sum256(mergeSource)
	programReference := &sink.LuaProgram{Sha256: digest[:]}
	program := &sink.LuaProgram{Source: mergeSource, Sha256: digest[:]}
	incoming := kafkaJSONDocument(`{"value":1}`)
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
	waitForDeadLetterValue(t, dlqClient, malformed.Value)

	crossStoreAddress := kafkaAddress("wrong-store")
	crossStoreAddress.Store = "archive"
	crossStoreDocument := kafkaJSONDocument(`{"value":3}`)
	crossStorePut := &sink.PutOperation{Document: crossStoreDocument, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	crossStoreOperation := &sink.WriteOperation{
		Address: crossStoreAddress,
		Action:  &sink.WriteOperation_Put{Put: crossStorePut},
	}
	crossStoreMutation := queue.Mutation{Write: crossStoreOperation}
	crossStoreValue, err := queue.MarshalMutation(crossStoreMutation)
	if err != nil {
		t.Fatalf("queue.MarshalMutation(cross-store) error = %v", err)
	}
	crossStoreRecord := &kgo.Record{Topic: topic, Key: []byte("cross-store"), Value: crossStoreValue}
	if err := rawClient.ProduceSync(t.Context(), crossStoreRecord).FirstErr(); err != nil {
		t.Fatalf("ProduceSync(cross-store) error = %v", err)
	}
	waitForDeadLetterValue(t, dlqClient, crossStoreValue)

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

func TestKafkaWorkerReplaysUncommittedMutationsAfterRestart(t *testing.T) {
	const topic = "sink-restart-mutations"
	const deadLetterTopic = "sink-restart-mutations.dlq"
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

	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("merge.NewLuaEngine() error = %v", err)
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

	address := kafkaAddress("restart-record")
	operations, programs := restartTestOperations(address, 20)
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     operations,
		LuaPrograms:    programs,
	}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatalf("Write(backlog) error = %v", err)
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
			t.Fatalf("Write(backlog) result[%d] = %+v", index, result)
		}
	}

	blocked := &blockingHandler{started: make(chan struct{})}
	firstOptions := kafka.WorkerOptions{
		Brokers:          cluster.ListenAddrs(),
		Store:            "primary",
		Topic:            topic,
		GroupID:          "sink-restart-worker",
		DeadLetterTopic:  deadLetterTopic,
		Handler:          blocked,
		RetryBackoff:     time.Millisecond,
		MaxRetryBackoff:  time.Millisecond,
		MaxRetryAttempts: 2,
	}
	firstWorker, err := kafka.NewWorker(firstOptions)
	if err != nil {
		t.Fatalf("kafka.NewWorker(first) error = %v", err)
	}
	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- firstWorker.Run(firstContext)
	}()
	select {
	case <-blocked.started:
	case <-time.After(10 * time.Second):
		cancelFirst()
		firstWorker.Close()
		t.Fatal("first worker did not fetch the backlog")
	}
	cancelFirst()
	select {
	case runErr := <-firstDone:
		if runErr != nil {
			t.Fatalf("first Worker.Run() error = %v", runErr)
		}
	case <-time.After(10 * time.Second):
		firstWorker.Close()
		t.Fatal("first worker did not stop")
	}
	firstWorker.Close()

	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("worker.NewProcessor() error = %v", err)
	}
	secondOptions := kafka.WorkerOptions{
		Brokers:          cluster.ListenAddrs(),
		Store:            "primary",
		Topic:            topic,
		GroupID:          "sink-restart-worker",
		DeadLetterTopic:  deadLetterTopic,
		Handler:          processor,
		RetryBackoff:     time.Millisecond,
		MaxRetryBackoff:  5 * time.Millisecond,
		MaxRetryAttempts: 3,
	}
	secondWorker, err := kafka.NewWorker(secondOptions)
	if err != nil {
		t.Fatalf("kafka.NewWorker(second) error = %v", err)
	}
	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- secondWorker.Run(secondContext)
	}()
	want := `{"value":20}`
	waitForDocumentAt(t, store, "restart-record", want)
	cancelSecond()
	select {
	case runErr := <-secondDone:
		if runErr != nil {
			t.Fatalf("second Worker.Run() error = %v", runErr)
		}
	case <-time.After(10 * time.Second):
		secondWorker.Close()
		t.Fatal("second worker did not stop")
	}
	secondWorker.Close()
}

func restartTestOperations(address *sink.RecordAddress, increments int) ([]*sink.WriteOperation, []*sink.LuaProgram) {
	document := kafkaJSONDocument(`{"value":0}`)
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	putAction := &sink.WriteOperation_Put{Put: put}
	putOperation := &sink.WriteOperation{Address: address, Action: putAction}
	operations := []*sink.WriteOperation{putOperation}

	mergeSource := []byte(`return function(current, incoming) current.value = current.value + incoming.value return current end`)
	digest := sha256.Sum256(mergeSource)
	program := &sink.LuaProgram{Source: mergeSource, Sha256: digest[:]}
	for range increments {
		incoming := kafkaJSONDocument(`{"value":1}`)
		programReference := &sink.LuaProgram{Sha256: digest[:]}
		mergeOperation := &sink.MergeOperation{
			IncomingDocument:    incoming,
			LuaProgram:          programReference,
			MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
		}
		mergeAction := &sink.WriteOperation_Merge{Merge: mergeOperation}
		operation := &sink.WriteOperation{Address: address, Action: mergeAction}
		operations = append(operations, operation)
	}
	programs := []*sink.LuaProgram{program}
	return operations, programs
}

func waitForDocumentData(t *testing.T, store *memory.Store, want string) {
	t.Helper()
	waitForDocumentAt(t, store, "record-1", want)
}

func waitForDocumentAt(t *testing.T, store *memory.Store, key string, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request := storage.ReadRequest{
			Operations: []storage.ReadOperation{{Address: kafkaStorageAddress(key)}},
		}
		response, err := store.Read(ctx, request)
		if err != nil {
			t.Fatalf("Store.Read() error = %v", err)
		}
		if response.Results[0].Status == storage.ReadStatusFound && string(response.Results[0].Document.Payload) == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Store.Read() document = %s, want %s", response.Results[0].Document.Payload, want)
		case <-ticker.C:
		}
	}
}

func kafkaJSONDocument(value string) *sink.Document {
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(value),
	}
	return document
}

func waitForDeadLetterValue(t *testing.T, client *kgo.Client, want []byte) {
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
			if string(records[0].Value) != string(want) {
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
