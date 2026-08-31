package queue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
)

type routingPublisherStub struct {
	mu        sync.Mutex
	mutations []queue.Mutation
	status    queue.PublishStatus
	err       error
}

type blockingRoutingPublisher struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (p *blockingRoutingPublisher) Publish(_ context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	p.started <- struct{}{}
	<-p.release
	response := queue.PublishResponse{Results: make([]queue.PublishResult, len(req.Mutations))}
	for index := range response.Results {
		response.Results[index].Status = queue.PublishStatusAccepted
	}
	return response, nil
}

func (p *routingPublisherStub) Publish(_ context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	p.mu.Lock()
	p.mutations = append(p.mutations, req.Mutations...)
	p.mu.Unlock()
	if p.err != nil {
		var empty queue.PublishResponse
		return empty, p.err
	}
	response := queue.PublishResponse{Results: make([]queue.PublishResult, len(req.Mutations))}
	for index := range response.Results {
		response.Results[index].Status = p.status
	}
	return response, nil
}

func (p *routingPublisherStub) stores(t *testing.T) []string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	stores := make([]string, len(p.mutations))
	for index, mutation := range p.mutations {
		store, err := queue.MutationStore(mutation)
		if err != nil {
			t.Fatalf("MutationStore() error = %v", err)
		}
		stores[index] = store
	}
	return stores
}

func TestRoutingPublisherRoutesStoresAndPreservesResultOrder(t *testing.T) {
	primary := &routingPublisherStub{status: queue.PublishStatusAccepted}
	archiveErr := errors.New("archive Kafka unavailable")
	archive := &routingPublisherStub{err: archiveErr}
	publishers := map[string]queue.Publisher{
		"primary": primary,
		"archive": archive,
	}
	router, err := queue.NewRoutingPublisher(publishers)
	if err != nil {
		t.Fatalf("NewRoutingPublisher() error = %v", err)
	}
	request := queue.PublishRequest{
		Mutations: []queue.Mutation{
			routingMutation("primary", "first"),
			routingMutation("sync-only", "second"),
			routingMutation("archive", "third"),
			routingMutation("primary", "fourth"),
		},
	}
	response, err := router.Publish(context.Background(), request)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(response.Results) != len(request.Mutations) {
		t.Fatalf("Publish() result count = %d", len(response.Results))
	}
	if response.Results[0].Status != queue.PublishStatusAccepted || response.Results[3].Status != queue.PublishStatusAccepted {
		t.Fatalf("primary results = %#v, %#v", response.Results[0], response.Results[3])
	}
	if response.Results[1].Status != queue.PublishStatusFailed || response.Results[1].Err == nil {
		t.Fatalf("sync-only result = %#v", response.Results[1])
	}
	if response.Results[2].Status != queue.PublishStatusFailed || !errors.Is(response.Results[2].Err, archiveErr) {
		t.Fatalf("archive result = %#v", response.Results[2])
	}
	primaryStores := primary.stores(t)
	if len(primaryStores) != 2 || primaryStores[0] != "primary" || primaryStores[1] != "primary" {
		t.Fatalf("primary publisher stores = %v", primaryStores)
	}
	archiveStores := archive.stores(t)
	if len(archiveStores) != 1 || archiveStores[0] != "archive" {
		t.Fatalf("archive publisher stores = %v", archiveStores)
	}
}

func TestRoutingPublisherPublishesStoresConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePublishers := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releasePublishers()
	primary := &blockingRoutingPublisher{started: started, release: release}
	archive := &blockingRoutingPublisher{started: started, release: release}
	publishers := map[string]queue.Publisher{
		"primary": primary,
		"archive": archive,
	}
	router, err := queue.NewRoutingPublisher(publishers)
	if err != nil {
		t.Fatalf("NewRoutingPublisher() error = %v", err)
	}
	request := queue.PublishRequest{
		Mutations: []queue.Mutation{
			routingMutation("primary", "first"),
			routingMutation("archive", "second"),
		},
	}
	done := make(chan error, 1)
	go func() {
		_, publishErr := router.Publish(context.Background(), request)
		done <- publishErr
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("store publishers did not run concurrently")
		}
	}
	releasePublishers()
	select {
	case publishErr := <-done:
		if publishErr != nil {
			t.Fatalf("Publish() error = %v", publishErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish() did not return")
	}
}

func routingMutation(store string, key string) queue.Mutation {
	recordKey := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: key}}
	address := &sink.RecordAddress{
		Store:     store,
		Namespace: "catalog",
		Dataset:   "products",
		Key:       recordKey,
	}
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(`{"value":1}`),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	mutation := queue.Mutation{Write: operation}
	return mutation
}
