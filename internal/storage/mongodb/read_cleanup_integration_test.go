//go:build integration

package mongodb_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoReadKillsCursorAfterRequestCancellation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := make([]any, 200)
	operations := make([]storage.ReadOperation, len(documents))
	for i := range documents {
		document := bson.D{{Key: "_id", Value: int64(i)}}
		documents[i] = document
		// Use string keys to match the public adapter without encoding details.
		key := string(rune(0x1000 + i))
		document[0].Value = key
		operations[i].Address = fixture.address(key)
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var opened, killed atomic.Int32
	monitor := &event.CommandMonitor{Succeeded: func(ctx context.Context, command *event.CommandSucceededEvent) {
		if command.CommandName == "find" {
			if command.Reply.Lookup("cursor", "id").Int64() != 0 {
				opened.Add(1)
				cancel()
			}
		}
		if command.CommandName == "killCursors" && ctx.Err() == nil {
			killed.Add(1)
		}
	}}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	storeOptions := mongodb.Options{Store: "primary"}
	backend, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.ReadRequest{Operations: operations}
	_, _ = backend.Read(ctx, request)
	if opened.Load() != 1 || killed.Load() != 1 {
		t.Fatalf("opened=%d successfully killed=%d", opened.Load(), killed.Load())
	}
}
