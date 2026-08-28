package kafka_test

import (
	"context"
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
)

func TestKafkaPublisherWorkerAppliesAsyncMutations(t *testing.T) {
	const topic = "sink-mutations"
	numBrokers := kfake.NumBrokers(1)
	seedTopics := kfake.SeedTopics(3, topic)
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
	serverOptions := service.Options{
		Storage:   store,
		Merges:    merge.NewRegistry(),
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
	document := &sink.Document{ContentType: "text/plain", Data: []byte("async value")}
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
