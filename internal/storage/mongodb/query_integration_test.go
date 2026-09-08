//go:build integration

package mongodb_test

import (
	"context"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"os"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeMongoPagesSortProjectionAndCount(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := bson.A{}
	for number := range 7 {
		document := bson.D{{Key: "number", Value: number}, {Key: "name", Value: "item"}}
		documents = append(documents, document)
	}
	insert := bson.D{{Key: "insert", Value: "documents"}, {Key: "documents", Value: documents}}
	seed := mongoNativeRequest(t, fixture.database, insert)
	if result, err := fixture.store.Execute(t.Context(), seed); err != nil || !result.Success {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	filter := bson.D{{Key: "number", Value: bson.D{{Key: "$gte", Value: 2}}}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}, {Key: "skip", Value: 999},
		{Key: "limit", Value: 1}, {Key: "sort", Value: bson.D{{Key: "number", Value: 1}}}, {Key: "projection", Value: bson.D{{Key: "name", Value: 1}}}}
	command := mongoNativeRequest(t, fixture.database, find)
	projection := &storage.Projection{Fields: []string{"number"}}
	query := storage.QueryRequest{Request: command, Offset: 2, PageSize: 2, Sort: []storage.SortField{{Field: "number", Descending: true}}, Projection: projection}
	for _, offset := range []int64{2, 4, 6} {
		query.Offset = offset
		page, err := fixture.store.Query(t.Context(), query)
		expected := max(0, min(2, 5-int(offset)))
		if err != nil || len(page.Documents) != expected || page.HasMore != (offset == 2) {
			t.Fatalf("offset=%d page=%+v err=%v", offset, page, err)
		}
		for index, document := range page.Documents {
			raw := bson.Raw(document.Payload)
			if raw.Lookup("number").AsInt64() != 6-offset-int64(index) || raw.Lookup("name").Type != 0 {
				t.Fatalf("sort or projection lost: %s", raw)
			}
		}
	}
	countRequest := storage.CountRequest{Request: command}
	if count, err := fixture.store.Count(t.Context(), countRequest); err != nil || count.Count != 5 || count.Estimated {
		t.Fatalf("find count=%+v err=%v", count, err)
	}
	match := bson.D{{Key: "$match", Value: filter}}
	limit := bson.D{{Key: "$limit", Value: 4}}
	aggregate := bson.D{{Key: "aggregate", Value: "documents"}, {Key: "pipeline", Value: bson.A{match, limit}}}
	command = mongoNativeRequest(t, fixture.database, aggregate)
	query.Request, query.Offset = command, 0
	page, err := fixture.store.Query(t.Context(), query)
	if err != nil || len(page.Documents) != 2 || !page.HasMore || bson.Raw(page.Documents[0].Payload).Lookup("number").AsInt64() != 5 {
		t.Fatalf("aggregate page=%+v err=%v", page, err)
	}
	countRequest = storage.CountRequest{Request: command}
	if count, err := fixture.store.Count(t.Context(), countRequest); err != nil || count.Count != 4 || count.Estimated {
		t.Fatalf("pipeline count=%+v err=%v", count, err)
	}
	missing := bson.D{{Key: "find", Value: "absent"}}
	command = mongoNativeRequest(t, fixture.database, missing)
	countRequest = storage.CountRequest{Request: command}
	if count, err := fixture.store.Count(t.Context(), countRequest); err != nil || count.Count != 0 || count.Estimated {
		t.Fatalf("empty count=%+v err=%v", count, err)
	}
}

func TestMongoEmptyCountUsesMetadataUnlessExactSemanticsAreRequired(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := []any{bson.D{{Key: "number", Value: 1}}, bson.D{{Key: "number", Value: 2}}}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	commands := make(chan *event.CommandStartedEvent, 16)
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		if event.CommandName == "count" || event.CommandName == "aggregate" {
			copyEvent := *event
			copyEvent.Command = append(bson.Raw(nil), event.Command...)
			commands <- &copyEvent
		}
	}}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	storeOptions := mongodb.Options{Store: "primary"}
	store, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		fields    bson.D
		estimate  bool
		estimated bool
		count     uint64
	}{
		{name: "absent filter", estimate: true, estimated: true, count: 2},
		{name: "empty filter", fields: bson.D{{Key: "filter", Value: bson.D{}}}, estimate: true, estimated: true, count: 2},
		{name: "presentation ignored", fields: bson.D{{Key: "filter", Value: bson.D{}}, {Key: "skip", Value: 100}, {Key: "limit", Value: 1}}, estimate: true, estimated: true, count: 2},
		{name: "default exact", count: 2},
		{name: "filtered", estimate: true, fields: bson.D{{Key: "filter", Value: bson.D{{Key: "number", Value: 1}}}}, count: 1},
		{name: "hint retained", estimate: true, fields: bson.D{{Key: "hint", Value: "_id_"}}, count: 2},
		{name: "read concern retained", estimate: true, fields: bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}}}, count: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			command := bson.D{{Key: "find", Value: "documents"}, {Key: "comment", Value: test.name}}
			command = append(command, test.fields...)
			request := storage.CountRequest{Request: mongoNativeRequest(t, fixture.database, command), Estimate: test.estimate}
			result, err := store.Count(t.Context(), request)
			if err != nil || result.Count != test.count || result.Estimated != test.estimated {
				t.Fatalf("count=%+v err=%v", result, err)
			}
			select {
			case executed := <-commands:
				want := "aggregate"
				if test.estimated {
					want = "count"
				}
				if executed.CommandName != want || executed.Command.Lookup("comment").StringValue() != test.name {
					t.Fatalf("wrong count strategy or lost comment: %s", executed.Command)
				}
				for _, field := range test.fields {
					if (field.Key == "hint" || field.Key == "readConcern") && executed.Command.Lookup(field.Key).Type == 0 {
						t.Fatalf("lost option %q: %s", field.Key, executed.Command)
					}
				}
			default:
				t.Fatal("count did not execute a database command")
			}
		})
	}
	invalid := []bson.D{
		{{Key: "find", Value: "documents"}, {Key: "unknownOption", Value: true}},
		{{Key: "find", Value: "documents"}, {Key: "filter", Value: bson.A{}}},
		{{Key: "find", Value: "documents"}, {Key: "filter", Value: nil}},
	}
	for _, command := range invalid {
		request := storage.CountRequest{Request: mongoNativeRequest(t, fixture.database, command), Estimate: true}
		if result, err := store.Count(t.Context(), request); err == nil || result.Count != 0 || result.Estimated {
			t.Fatalf("invalid count accepted: %s result=%+v err=%v", command, result, err)
		}
	}
	command := bson.D{{Key: "find", Value: "documents"}}
	request := storage.CountRequest{Request: mongoNativeRequest(t, fixture.database, command), Estimate: true}
	request.Request.Store = "other"
	if _, err := store.Count(t.Context(), request); err == nil {
		t.Fatal("estimate bypassed store validation")
	}
}
