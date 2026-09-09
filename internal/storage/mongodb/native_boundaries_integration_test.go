//go:build integration

package mongodb_test

import (
	"context"
	"errors"
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

func TestMongoQueryChargesOnlyReturnedDocuments(t *testing.T) {
	fixture := newIntegrationFixture(t)
	first := bson.D{{Key: "_id", Value: 1}, {Key: "value", Value: "small"}}
	second := bson.D{{Key: "_id", Value: 2}, {Key: "value", Value: strings.Repeat("x", 4096)}}
	documents := []any{first, second}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"find", "aggregate"} {
		t.Run(name, func(t *testing.T) {
			sort := bson.D{{Key: "_id", Value: 1}}
			command := bson.D{{Key: name, Value: "documents"}}
			if name == "find" {
				field := bson.E{Key: "sort", Value: sort}
				command = append(command, field)
			} else {
				stage := bson.D{{Key: "$sort", Value: sort}}
				field := bson.E{Key: "pipeline", Value: bson.A{stage}}
				command = append(command, field)
			}
			request := storage.QueryRequest{Request: mongoNativeRequest(t, fixture.database, command), PageSize: 1}
			request.Request.MaxBytes = 512
			page, err := fixture.store.Query(t.Context(), request)
			if err != nil || len(page.Documents) != 1 || !page.HasMore {
				t.Fatalf("lookahead consumed the page budget: documents=%d more=%t err=%v", len(page.Documents), page.HasMore, err)
			}
			if bson.Raw(page.Documents[0].Payload).Lookup("_id").Int32() != 1 {
				t.Fatal("wrong page returned")
			}
			request.Offset = 1
			page, err = fixture.store.Query(t.Context(), request)
			code, _ := storage.ErrorDetails(err)
			if code != storage.ErrorCodeResourceExhausted || len(page.Documents) != 0 || page.HasMore {
				t.Fatalf("oversized returned document did not fail atomically: %+v err=%v", page, err)
			}
		})
	}
}

func TestMongoQueryClosesCursorAfterCancellationOrBudgetFailure(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		name := "budget"
		if cancelRequest {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			var documents []any
			for i := range 4 {
				document := bson.D{{Key: "_id", Value: i}, {Key: "value", Value: strings.Repeat("x", 1024)}}
				documents = append(documents, document)
			}
			if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var closed atomic.Int32
			monitor := &event.CommandMonitor{
				Started: func(_ context.Context, command *event.CommandStartedEvent) {
					if command.CommandName == "killCursors" {
						closed.Add(1)
					}
				},
				Succeeded: func(_ context.Context, command *event.CommandSucceededEvent) {
					if cancelRequest && command.CommandName == "find" {
						cancel()
					}
				},
			}
			clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
			client, err := mongo.Connect(clientOptions)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Disconnect(context.Background()) }()
			storeOptions := mongodb.Options{Store: "primary"}
			store, err := mongodb.New(client, storeOptions)
			if err != nil {
				t.Fatal(err)
			}
			command := bson.D{{Key: "find", Value: "documents"}}
			request := storage.QueryRequest{Request: mongoNativeRequest(t, fixture.database, command), PageSize: 2}
			if !cancelRequest {
				request.Request.MaxBytes = 512
			}
			page, err := store.Query(ctx, request)
			code, _ := storage.ErrorDetails(err)
			if err == nil || len(page.Documents) != 0 || page.HasMore || closed.Load() != 1 {
				t.Fatalf("failed Query retained output or its backend cursor: documents=%d more=%t closed=%d err=%v", len(page.Documents), page.HasMore, closed.Load(), err)
			}
			if cancelRequest && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if !cancelRequest && code != storage.ErrorCodeResourceExhausted {
				t.Fatalf("lost budget failure: %v", err)
			}
		})
	}
}

func TestMongoExecuteReportsPartialWritesAndUncertainAcknowledgements(t *testing.T) {
	for _, ordered := range []bool{true, false} {
		name := "unordered"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			first := bson.D{{Key: "_id", Value: "duplicate"}}
			last := bson.D{{Key: "_id", Value: "following"}}
			command := bson.D{{Key: "insert", Value: "documents"}, {Key: "documents", Value: bson.A{first, first, last}}, {Key: "ordered", Value: ordered}}
			request := mongoNativeRequest(t, fixture.database, command)
			response, err := fixture.store.Execute(t.Context(), request)
			if err != nil || response.Success || bson.Raw(response.Payload).Lookup("writeErrors").Type != bson.TypeArray {
				t.Fatalf("partial write reported success: %s err=%v success=%t", bson.Raw(response.Payload), err, response.Success)
			}
			filter := bson.D{}
			count, err := fixture.collection.CountDocuments(t.Context(), filter)
			want := int64(2)
			if ordered {
				want = 1
			}
			if err != nil || count != want {
				t.Fatalf("native ordered semantics changed: count=%d want=%d err=%v", count, want, err)
			}
		})
	}
	t.Run("write concern timeout", func(t *testing.T) {
		fixture := newIntegrationFixture(t)
		document := bson.D{{Key: "_id", Value: "committed"}}
		// The disposable single-member replica set cannot acknowledge two copies.
		concern := bson.D{{Key: "w", Value: 2}, {Key: "wtimeout", Value: 20}}
		command := bson.D{{Key: "insert", Value: "documents"}, {Key: "documents", Value: bson.A{document}}, {Key: "writeConcern", Value: concern}}
		request := mongoNativeRequest(t, fixture.database, command)
		response, err := fixture.store.Execute(t.Context(), request)
		if err != nil || response.Success || bson.Raw(response.Payload).Lookup("writeConcernError").Type != bson.TypeEmbeddedDocument {
			t.Fatalf("uncertain acknowledgement reported success: %s err=%v success=%t", bson.Raw(response.Payload), err, response.Success)
		}
		if err := fixture.collection.FindOne(t.Context(), document).Err(); err != nil {
			t.Fatalf("write concern failure lost the already committed document: %v", err)
		}
	})
}
