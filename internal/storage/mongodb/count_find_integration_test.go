//go:build integration

package mongodb_test

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestNativeMongoCountPreservesFindOnlyPredicates(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := []any{}
	for index, longitude := range []float64{0, 0.005, 1} {
		point := bson.D{{Key: "type", Value: "Point"}, {Key: "coordinates", Value: bson.A{longitude, 0.0}}}
		document := bson.D{{Key: "number", Value: index}, {Key: "location", Value: point}, {Key: "large", Value: strings.Repeat("x", 4096)}}
		documents = append(documents, document)
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	keys := bson.D{{Key: "location", Value: "2dsphere"}}
	index := mongo.IndexModel{Keys: keys}
	if _, err := fixture.collection.Indexes().CreateOne(t.Context(), index); err != nil {
		t.Fatal(err)
	}
	filters := []string{
		`{"$where":"this.number < 2"}`,
		`{"$and":[{"$where":"this.number < 2"},{"$expr":{"$lt":["$number","$$cap"]}}]}`,
		`{"location":{"$near":{"$geometry":{"type":"Point","coordinates":[0,0]},"$maxDistance":1000}}}`,
		`{"location":{"$nearSphere":{"$geometry":{"type":"Point","coordinates":[0,0]},"$maxDistance":1000}}}`,
	}
	for _, input := range filters {
		t.Run(input, func(t *testing.T) {
			var filter bson.D
			if err := bson.UnmarshalExtJSON([]byte(input), false, &filter); err != nil {
				t.Fatal(err)
			}
			variables := bson.D{{Key: "cap", Value: 2}}
			projection := bson.D{{Key: "large", Value: 1}}
			find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter},
				{Key: "let", Value: variables}, {Key: "projection", Value: projection}, {Key: "skip", Value: 100}, {Key: "limit", Value: 1}}
			native := mongoNativeRequest(t, fixture.database, find)
			query := storage.QueryRequest{Request: native, PageSize: 10}
			page, err := fixture.store.Query(t.Context(), query)
			if err != nil || len(page.Documents) != 2 {
				t.Fatalf("native Query=%+v error=%v", page, err)
			}
			// Full source documents cannot fit, but counting must not retain them.
			native.MaxBytes = 256
			req := storage.CountRequest{Request: native}
			result, err := fixture.store.Count(t.Context(), req)
			if err != nil || result.Estimated || result.Count != 2 {
				t.Fatalf("same find returned Count=%+v error=%v", result, err)
			}
		})
	}
}

func TestNativeMongoCountFindBoundsPagesAndDiscardsPartialTotals(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := make([]any, 1005)
	for index := range documents {
		document := bson.D{{Key: "number", Value: index}, {Key: "large", Value: strings.Repeat("x", 2048)}}
		documents[index] = document
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	for _, cancelOnMore := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var findCalls, moreCalls, closedCalls atomic.Int32
		monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
			switch command.CommandName {
			case "aggregate":
				t.Error("find-only predicate was translated into aggregation")
			case "find":
				findCalls.Add(1)
				projection := command.Command.Lookup("projection").Document()
				if projection.Lookup("_id").AsInt64() != 0 || projection.Lookup("count", "$literal").AsInt64() != 1 {
					t.Errorf("count did not use a constant projection: %s", projection)
				}
			case "getMore":
				moreCalls.Add(1)
				if cancelOnMore {
					cancel()
				}
			case "killCursors":
				closedCalls.Add(1)
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
		filter := bson.D{{Key: "$where", Value: "this.number >= 0"}}
		find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}}
		native := mongoNativeRequest(t, fixture.database, find)
		native.MaxBytes = 256
		req := storage.CountRequest{Request: native}
		result, err := store.Count(ctx, req)
		if findCalls.Load() != 1 || moreCalls.Load() != 1 {
			t.Fatalf("count did not traverse its bounded cursor exactly once: find=%d more=%d", findCalls.Load(), moreCalls.Load())
		}
		if cancelOnMore {
			if err == nil || result.Count != 0 || result.Estimated || closedCalls.Load() != 1 {
				t.Fatalf("canceled count exposed a partial total or leaked its cursor: %+v close=%d error=%v", result, closedCalls.Load(), err)
			}
		} else if err != nil || result.Count != 1005 || result.Estimated {
			t.Fatalf("multi-page count=%+v error=%v", result, err)
		}
	}
}
