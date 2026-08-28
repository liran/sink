//go:build integration

package mongodb_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongodbTestURI = "SINK_MONGODB_TEST_URI"

type integrationFixture struct {
	client     *mongo.Client
	store      *mongodb.Store
	database   string
	collection *mongo.Collection
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	uri := os.Getenv(mongodbTestURI)
	if uri == "" {
		t.Skipf("%s is not set", mongodbTestURI)
	}
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatalf("mongo.Connect() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("client.Ping() error = %v", err)
	}

	database := "sink_integration_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	dataset := mongodb.Dataset{Namespace: "logical", Dataset: "records"}
	binding := mongodb.Binding{Database: database, Collection: "documents"}
	storeOptions := mongodb.Options{
		Store:    "primary",
		Bindings: map[mongodb.Dataset]mongodb.Binding{dataset: binding},
	}
	store, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatalf("mongodb.New() error = %v", err)
	}
	fixture := &integrationFixture{
		client:     client,
		store:      store,
		database:   database,
		collection: client.Database(database).Collection(binding.Collection),
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = client.Database(database).Drop(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})
	return fixture
}

func TestMongoDBStorageLifecycleAndLegacyUpgrade(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := context.Background()
	first := integrationAddress("first")
	second := integrationAddress("second")
	firstValue := bson.D{{Key: "value", Value: "one"}}
	firstDocument := bsonStorageDocument(t, firstValue)
	secondValue := bson.D{{Key: "value", Value: "two"}}
	secondDocument := bsonStorageDocument(t, secondValue)
	createOperations := []storage.WriteOperation{
		{
			Address:  first,
			Document: firstDocument,
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordNotExists,
			},
		},
		{
			Address:  second,
			Document: secondDocument,
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordNotExists,
			},
		},
	}
	createRequest := storage.WriteRequest{Operations: createOperations}
	created, err := fixture.store.Write(ctx, createRequest)
	if err != nil {
		t.Fatalf("Write(create batch) error = %v", err)
	}
	for index, result := range created.Results {
		if result.Status != storage.WriteStatusApplied || len(result.Revision.Data) == 0 {
			t.Fatalf("Write(create batch) result[%d] = %#v", index, result)
		}
	}

	readOperations := []storage.ReadOperation{
		{Address: second},
		{Address: integrationAddress("missing")},
		{Address: first},
	}
	readRequest := storage.ReadRequest{Operations: readOperations}
	read, err := fixture.store.Read(ctx, readRequest)
	if err != nil {
		t.Fatalf("Read(batch) error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusFound ||
		read.Results[1].Status != storage.ReadStatusNotFound ||
		read.Results[2].Status != storage.ReadStatusFound {
		t.Fatalf("Read(batch) statuses = %v, %v, %v", read.Results[0].Status, read.Results[1].Status, read.Results[2].Status)
	}

	replacementValue := bson.D{{Key: "value", Value: "updated"}}
	replacementDocument := bsonStorageDocument(t, replacementValue)
	replaceOperation := storage.WriteOperation{
		Address:  first,
		Document: replacementDocument,
		Precondition: storage.Precondition{
			Kind:     storage.PreconditionRevisionMatches,
			Revision: created.Results[0].Revision,
		},
	}
	replaceRequest := storage.WriteRequest{Operations: []storage.WriteOperation{replaceOperation}}
	replaced, err := fixture.store.Write(ctx, replaceRequest)
	if err != nil {
		t.Fatalf("Write(revision match) error = %v", err)
	}
	if replaced.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(revision match) result = %#v", replaced.Results[0])
	}
	stale, err := fixture.store.Write(ctx, replaceRequest)
	if err != nil {
		t.Fatalf("Write(stale revision) error = %v", err)
	}
	if stale.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write(stale revision) status = %v", stale.Results[0].Status)
	}

	legacyDocument := bson.D{
		{Key: "_id", Value: "legacy"},
		{Key: "value", Value: "old"},
	}
	_, err = fixture.collection.InsertOne(ctx, legacyDocument)
	if err != nil {
		t.Fatalf("InsertOne(legacy) error = %v", err)
	}
	legacyAddress := integrationAddress("legacy")
	legacyReadRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: legacyAddress}},
	}
	legacyRead, err := fixture.store.Read(ctx, legacyReadRequest)
	if err != nil {
		t.Fatalf("Read(legacy) error = %v", err)
	}
	if legacyRead.Results[0].Status != storage.ReadStatusFound || len(legacyRead.Results[0].Revision.Data) != 0 {
		t.Fatalf("Read(legacy) result = %#v", legacyRead.Results[0])
	}
	legacyUpdate := storage.WriteOperation{
		Address: legacyAddress,
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRevisionAbsent,
		},
	}
	legacyUpdateValue := bson.D{{Key: "value", Value: "new"}}
	legacyUpdate.Document = bsonStorageDocument(t, legacyUpdateValue)
	legacyUpdateRequest := storage.WriteRequest{Operations: []storage.WriteOperation{legacyUpdate}}
	legacyUpdated, err := fixture.store.Write(ctx, legacyUpdateRequest)
	if err != nil {
		t.Fatalf("Write(legacy upgrade) error = %v", err)
	}
	if legacyUpdated.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(legacy upgrade) result = %#v", legacyUpdated.Results[0])
	}
	legacyStale, err := fixture.store.Write(ctx, legacyUpdateRequest)
	if err != nil {
		t.Fatalf("Write(legacy stale) error = %v", err)
	}
	if legacyStale.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write(legacy stale) status = %v", legacyStale.Results[0].Status)
	}

	var raw bson.Raw
	legacyFilter := bson.D{{Key: "_id", Value: "legacy"}}
	if err := fixture.collection.FindOne(ctx, legacyFilter).Decode(&raw); err != nil {
		t.Fatalf("FindOne(upgraded legacy) error = %v", err)
	}
	if raw.Lookup("value").StringValue() != "new" {
		t.Fatalf("upgraded legacy value = %s", raw.Lookup("value").StringValue())
	}
	if _, ok := raw.Lookup("__sink").DocumentOK(); !ok {
		t.Fatal("upgraded legacy document has no Sink metadata")
	}

	deleteOperations := []storage.DeleteOperation{
		{Address: first},
		{Address: integrationAddress("missing")},
	}
	deleteRequest := storage.DeleteRequest{Operations: deleteOperations}
	deleted, err := fixture.store.Delete(ctx, deleteRequest)
	if err != nil {
		t.Fatalf("Delete(batch) error = %v", err)
	}
	for index, result := range deleted.Results {
		if result.Status != storage.DeleteStatusApplied {
			t.Fatalf("Delete(batch) result[%d] = %#v", index, result)
		}
	}
}

func TestMongoDBConcurrentUpsertsDoNotSurfaceDuplicateKey(t *testing.T) {
	fixture := newIntegrationFixture(t)
	const writers = 50
	documents := make([]storage.Document, writers)
	for index := range writers {
		value := bson.D{{Key: "writer", Value: index}}
		documents[index] = bsonStorageDocument(t, value)
	}
	statuses := make(chan storage.WriteStatus, writers)
	errorsChannel := make(chan error, writers)
	var workers sync.WaitGroup
	workers.Add(writers)
	for index := range writers {
		go func() {
			defer workers.Done()
			operation := storage.WriteOperation{
				Address:  integrationAddress("concurrent-upsert"),
				Document: documents[index],
				Precondition: storage.Precondition{
					Kind: storage.PreconditionNone,
				},
			}
			request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			response, err := fixture.store.Write(context.Background(), request)
			if err != nil {
				errorsChannel <- err
				return
			}
			if response.Results[0].Err != nil {
				errorsChannel <- response.Results[0].Err
				return
			}
			statuses <- response.Results[0].Status
		}()
	}
	workers.Wait()
	close(statuses)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("Write(concurrent upsert) error = %v", err)
	}
	for status := range statuses {
		if status != storage.WriteStatusApplied {
			t.Errorf("Write(concurrent upsert) status = %v", status)
		}
	}
}

type counterMerger struct{}

func (counterMerger) Merge(_ context.Context, req merge.Request) (merge.Result, error) {
	current := int64(0)
	if req.Current != nil {
		value := bson.Raw(req.Current.Data).Lookup("counter")
		parsed, ok := value.AsInt64OK()
		if !ok {
			var empty merge.Result
			return empty, fmt.Errorf("current counter is not an integer")
		}
		current = parsed
	}
	deltaValue := bson.Raw(req.Incoming.Data).Lookup("delta")
	delta, ok := deltaValue.AsInt64OK()
	if !ok {
		var empty merge.Result
		return empty, fmt.Errorf("delta is not an integer")
	}
	value := bson.D{{Key: "counter", Value: current + delta}}
	document, err := bson.Marshal(value)
	if err != nil {
		var empty merge.Result
		return empty, err
	}
	merged := storage.Document{ContentType: mongodb.ContentTypeBSON, Data: document}
	result := merge.Result{Document: merged}
	return result, nil
}

func TestMongoDBServiceConcurrentMergeIsAtomic(t *testing.T) {
	fixture := newIntegrationFixture(t)
	registry := merge.NewRegistry()
	profile := merge.Profile{Name: "counter", Version: 1}
	merger := counterMerger{}
	if err := registry.Register(profile, merger); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	serverOptions := service.Options{
		Storage:          fixture.store,
		Merges:           registry,
		MaxMergeAttempts: 200,
	}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	address := sinkAddress("counter")
	initialValue := bson.D{{Key: "counter", Value: int64(0)}}
	initial := sinkDocument(t, initialValue)
	put := &sink.PutOperation{Document: initial, Mode: sink.WriteMode_WRITE_MODE_CREATE}
	putOperation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	putRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{putOperation},
	}
	putResponse, err := server.Write(context.Background(), putRequest)
	if err != nil {
		t.Fatalf("Write(initial counter) error = %v", err)
	}
	if putResponse.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("Write(initial counter) result = %#v", putResponse.Results[0])
	}

	const mutations = 50
	incomingValue := bson.D{{Key: "delta", Value: int64(1)}}
	incoming := sinkDocument(t, incomingValue)
	statuses := make(chan sink.WriteStatus, mutations)
	errorsChannel := make(chan error, mutations)
	var workers sync.WaitGroup
	workers.Add(mutations)
	for range mutations {
		go func() {
			defer workers.Done()
			mergeProfile := &sink.MergeProfile{Name: "counter", Version: 1}
			mergeOperation := &sink.MergeOperation{
				IncomingDocument:    incoming,
				Profile:             mergeProfile,
				MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
			}
			operation := &sink.WriteOperation{
				Address: address,
				Action:  &sink.WriteOperation_Merge{Merge: mergeOperation},
			}
			request := &sink.WriteRequest{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations:     []*sink.WriteOperation{operation},
			}
			response, writeErr := server.Write(context.Background(), request)
			if writeErr != nil {
				errorsChannel <- writeErr
				return
			}
			statuses <- response.Results[0].Status
		}()
	}
	workers.Wait()
	close(statuses)
	close(errorsChannel)
	for writeErr := range errorsChannel {
		t.Errorf("Write(concurrent merge) error = %v", writeErr)
	}
	for status := range statuses {
		if status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Errorf("Write(concurrent merge) status = %v", status)
		}
	}
	if t.Failed() {
		return
	}

	readRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: integrationAddress("counter")}},
	}
	read, err := fixture.store.Read(context.Background(), readRequest)
	if err != nil {
		t.Fatalf("Read(final counter) error = %v", err)
	}
	counterValue := bson.Raw(read.Results[0].Document.Data).Lookup("counter")
	counter, ok := counterValue.AsInt64OK()
	if !ok || counter != mutations {
		t.Fatalf("final counter = %d, want %d", counter, mutations)
	}
}

func integrationAddress(key string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       storage.Key{Type: "string", Data: []byte(key)},
	}
	return address
}

func sinkAddress(key string) *sink.RecordAddress {
	recordKey := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: key}}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       recordKey,
	}
	return address
}

func bsonStorageDocument(t *testing.T, value bson.D) storage.Document {
	t.Helper()
	encoded, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("bson.Marshal() error = %v", err)
	}
	document := storage.Document{ContentType: mongodb.ContentTypeBSON, Data: encoded}
	return document
}

func sinkDocument(t *testing.T, value bson.D) *sink.Document {
	t.Helper()
	encoded, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("bson.Marshal() error = %v", err)
	}
	document := &sink.Document{ContentType: mongodb.ContentTypeBSON, Data: encoded}
	return document
}
