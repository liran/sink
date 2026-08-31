//go:build integration

package mongodb_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

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

func TestMongoDBDateTimeRoundTrip(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	address := fixture.address("date-time")
	createdAt := time.Date(2026, time.August, 29, 4, 34, 56, 789000000, time.UTC)
	value := bson.D{
		{Key: "created_at", Value: createdAt},
		{Key: "literal", Value: "2026-08-29T04:34:56.789Z"},
	}
	document := bsonStorageDocument(t, value)
	operation := storage.WriteOperation{Address: address, Document: document}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	written, err := fixture.store.Write(ctx, request)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write() result = %#v", written.Results[0])
	}

	var raw bson.Raw
	filter := bson.D{{Key: "_id", Value: "date-time"}}
	if err := fixture.collection.FindOne(ctx, filter).Decode(&raw); err != nil {
		t.Fatalf("FindOne() error = %v", err)
	}
	if raw.Lookup("created_at").Type != bson.TypeDateTime {
		t.Fatalf("created_at BSON type = %s", raw.Lookup("created_at").Type)
	}
	if raw.Lookup("literal").Type != bson.TypeString {
		t.Fatalf("literal BSON type = %s", raw.Lookup("literal").Type)
	}

	readOperation := storage.ReadOperation{Address: address}
	readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{readOperation}}
	read, err := fixture.store.Read(ctx, readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	result := read.Results[0]
	if result.Status != storage.ReadStatusFound || result.Document.Encoding != storage.DocumentEncodingBSON {
		t.Fatalf("Read() result = %#v", result)
	}
	if bson.Raw(result.Document.Payload).Lookup("created_at").Type != bson.TypeDateTime {
		t.Fatalf("read created_at BSON type = %s", bson.Raw(result.Document.Payload).Lookup("created_at").Type)
	}
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
	storeOptions := mongodb.Options{Store: "primary"}
	store, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatalf("mongodb.New() error = %v", err)
	}
	fixture := &integrationFixture{
		client:     client,
		store:      store,
		database:   database,
		collection: client.Database(database).Collection("documents"),
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
	first := fixture.address("first")
	second := fixture.address("second")
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
		{Address: fixture.address("missing")},
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
	legacyAddress := fixture.address("legacy")
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
		{Address: fixture.address("missing")},
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
				Address:  fixture.address("concurrent-upsert"),
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

func (f *integrationFixture) address(key string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: f.database,
		Dataset:   "documents",
		Key:       storage.Key{Type: "string", Data: []byte(key)},
	}
	return address
}

func bsonStorageDocument(t *testing.T, value bson.D) storage.Document {
	t.Helper()
	encoded, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("bson.Marshal() error = %v", err)
	}
	document := storage.Document{Encoding: storage.DocumentEncodingBSON, Payload: encoded}
	return document
}
