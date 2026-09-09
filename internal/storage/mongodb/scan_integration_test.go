//go:build integration

package mongodb_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoScanCrossClientMixedBSONKeys(t *testing.T) {
	fixture := newIntegrationFixture(t)
	binary := bson.Binary{Subtype: 0, Data: []byte{1, 2}}
	ids := []any{nil, int64(1), int64(1<<53 + 1), "a", "z", binary, bson.NewObjectID(), false, true, bson.DateTime(1234)}
	documents := make([]any, 0, len(ids))
	for _, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "value", Value: "test"}}
		documents = append(documents, document)
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI))
	otherClient, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = otherClient.Disconnect(context.Background()) }()
	storeOptions := mongodb.Options{Store: "primary"}
	other, err := mongodb.New(otherClient, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	for _, direction := range []int32{1, -1} {
		sorting := bson.D{{Key: "_id", Value: direction}}
		command := bson.D{{Key: "find", Value: "documents"}, {Key: "sort", Value: sorting}}
		request := storage.ScanRequest{Request: mongoNativeRequest(t, fixture.database, command), BatchSize: 2}
		query := storage.QueryRequest{Request: request.Request, PageSize: 100}
		baseline, err := fixture.store.Query(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		var found []storage.Document
		for calls := 0; calls < 20; calls++ {
			selected := other
			if calls == 0 {
				selected = fixture.store
			}
			page, err := selected.Scan(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			found = append(found, page.Documents...)
			if len(page.NextCursor) == 0 {
				break
			}
			request.Cursor = page.NextCursor
		}
		if len(found) != len(ids) {
			t.Fatalf("direction=%d count=%d", direction, len(found))
		}
		for index, document := range found {
			got := bson.Raw(document.Payload).Lookup("_id")
			want := bson.Raw(baseline.Documents[index].Payload).Lookup("_id")
			if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("direction=%d row=%d got=%v want=%v", direction, index, got, want)
			}
		}
	}
}

func TestMongoScanObservesChangesWithoutRetainingCursor(t *testing.T) {
	fixture := newIntegrationFixture(t)
	for _, id := range []int{1, 2, 3, 5} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "value", Value: "old"}}
		if _, err := fixture.collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	projection := bson.D{{Key: "_id", Value: 0}, {Key: "value", Value: 1}}
	command := bson.D{{Key: "find", Value: "documents"}, {Key: "projection", Value: projection}}
	request := storage.ScanRequest{Request: mongoNativeRequest(t, fixture.database, command), BatchSize: 2}
	page, err := fixture.store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 2 || len(page.NextCursor) == 0 {
		t.Fatalf("first=%+v err=%v", page, err)
	}
	checkpoint := bytes.Clone(page.NextCursor)
	filter := bson.D{{Key: "_id", Value: 3}}
	if _, err := fixture.collection.DeleteOne(t.Context(), filter); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{0, 4} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "value", Value: fmt.Sprint(id)}}
		if _, err := fixture.collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	request.Cursor = checkpoint
	page, err = fixture.store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 2 || len(page.NextCursor) != 0 {
		t.Fatalf("resumed=%+v err=%v", page, err)
	}
	for index, want := range []string{"4", "old"} {
		raw := bson.Raw(page.Documents[index].Payload)
		if raw.Lookup("_id").Type != 0 || raw.Lookup("value").StringValue() != want {
			t.Fatalf("projection or live seek lost: %s", raw)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fixture.store.Scan(canceled, request); err == nil {
		t.Fatal("canceled page succeeded")
	}
	retry, err := fixture.store.Scan(t.Context(), request)
	if err != nil || len(retry.Documents) != 2 || !bytes.Equal(request.Cursor, checkpoint) {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
}

func TestMongoScanSeekUsesIndexAndClosesByteLimitedPage(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := make([]any, 1100)
	for index := range documents {
		document := bson.D{{Key: "_id", Value: index}, {Key: "value", Value: strings.Repeat("x", 100)}}
		documents[index] = document
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	var observed bson.Raw
	var closed int
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		if event.CommandName == "find" {
			observed = bytes.Clone(event.Command)
		}
		if event.CommandName == "killCursors" {
			closed++
		}
	}}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	storeOptions := mongodb.Options{Store: "primary"}
	backend, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	command := bson.D{{Key: "find", Value: "documents"}}
	request := storage.ScanRequest{Request: mongoNativeRequest(t, fixture.database, command), BatchSize: 1000}
	first, err := backend.Scan(t.Context(), request)
	if err != nil || len(first.Documents) != 1000 {
		t.Fatalf("first count=%d err=%v", len(first.Documents), err)
	}
	request.Cursor = first.NextCursor
	request.BatchSize = 10
	page, err := backend.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 10 {
		t.Fatalf("next count=%d err=%v", len(page.Documents), err)
	}
	var executed bson.D
	if err := bson.Unmarshal(observed, &executed); err != nil {
		t.Fatal(err)
	}
	query := bson.D{}
	for _, field := range executed {
		switch field.Key {
		case "$db", "lsid", "$clusterTime", "maxTimeMS":
			continue
		}
		query = append(query, field)
	}
	explain := bson.D{{Key: "explain", Value: query}, {Key: "verbosity", Value: "executionStats"}}
	raw, err := fixture.client.Database(fixture.database).RunCommand(t.Context(), explain).Raw()
	if err != nil {
		t.Fatal(err)
	}
	stats := raw.Lookup("executionStats").Document()
	if !strings.Contains(raw.String(), `"IXSCAN"`) || stats.Lookup("totalKeysExamined").AsInt64() > 12 {
		t.Fatalf("seek did not bound indexed work: %s", raw)
	}
	// Make the initial wire batch hit MongoDB's byte cap before the query
	// limit, so returning a short page must explicitly close a live cursor.
	all := bson.D{}
	values := bson.D{{Key: "value", Value: strings.Repeat("x", 32768)}}
	update := bson.D{{Key: "$set", Value: values}}
	if _, err := fixture.collection.UpdateMany(t.Context(), all, update); err != nil {
		t.Fatal(err)
	}
	request.Cursor = nil
	request.Request.MaxBytes = 40000
	request.BatchSize = 1000
	page, err = backend.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) == 0 {
		t.Fatalf("byte-limited page=%+v err=%v", page, err)
	}
	// A limit of 1001 rows can leave a cursor open after an early byte-budget
	// return. The request must close it before returning to the client.
	if closed == 0 {
		t.Fatal("byte-limited scan retained its database cursor")
	}
	request.Cursor = page.NextCursor
	page, err = backend.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || bson.Raw(page.Documents[0].Payload).Lookup("_id").Int32() != 1 {
		t.Fatalf("byte-limited resume=%+v err=%v", page, err)
	}
}
