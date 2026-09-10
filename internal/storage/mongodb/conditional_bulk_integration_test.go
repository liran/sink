//go:build integration

package mongodb_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestMongoDBConditionalBulkKeepsPerRecordOutcomes(t *testing.T) {
	fixture := newIntegrationFixture(t)
	initialValue := bson.D{{Key: "value", Value: 0}}
	initial := bsonStorageDocument(t, initialValue)
	seed := storage.WriteRequest{}
	for index := range 6 {
		address := fixture.address(fmt.Sprintf("bulk-%d", index))
		if index == 5 {
			address.Dataset = "alternate"
		}
		operation := storage.WriteOperation{Address: address, Document: initial}
		seed.Operations = append(seed.Operations, operation)
	}
	created, err := fixture.store.Write(t.Context(), seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range created.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatalf("seed: %+v", result)
		}
	}
	legacy := bson.D{{Key: "_id", Value: "legacy"}, {Key: "value", Value: 0}}
	if _, err := fixture.collection.InsertOne(t.Context(), legacy); err != nil {
		t.Fatal(err)
	}
	validator := bson.D{{Key: "value", Value: bson.D{{Key: "$lte", Value: 10}}}}
	command := bson.D{{Key: "collMod", Value: "documents"}, {Key: "validator", Value: validator}}
	if err := fixture.client.Database(fixture.database).RunCommand(t.Context(), command).Err(); err != nil {
		t.Fatal(err)
	}
	changedValue := bson.D{{Key: "value", Value: 1}}
	changed := bsonStorageDocument(t, changedValue)
	request := storage.WriteRequest{}
	for index, seeded := range seed.Operations {
		precondition := storage.Precondition{Kind: storage.PreconditionRevisionMatches, Revision: created.Results[index].Revision}
		switch index {
		case 1:
			precondition.Revision = created.Results[0].Revision
		case 2:
			precondition.Kind = storage.PreconditionRecordExists
		case 3:
			precondition.Kind = storage.PreconditionRevisionAbsent
		}
		operation := storage.WriteOperation{Address: seeded.Address, Document: changed, Precondition: precondition}
		if index == 4 {
			invalid := bson.D{{Key: "value", Value: 99}}
			operation.Document = bsonStorageDocument(t, invalid)
		}
		request.Operations = append(request.Operations, operation)
	}
	absent := storage.Precondition{Kind: storage.PreconditionRevisionAbsent}
	legacyWrite := storage.WriteOperation{Address: fixture.address("legacy"), Document: changed, Precondition: absent}
	request.Operations = append(request.Operations, legacyWrite)
	exists := storage.Precondition{Kind: storage.PreconditionRecordExists}
	missingWrite := storage.WriteOperation{Address: fixture.address("missing"), Document: changed, Precondition: exists}
	request.Operations = append(request.Operations, missingWrite)
	written, err := fixture.store.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	expected := []storage.WriteStatus{storage.WriteStatusApplied, storage.WriteStatusPreconditionFailed, storage.WriteStatusApplied,
		storage.WriteStatusPreconditionFailed, storage.WriteStatusFailed, storage.WriteStatusApplied, storage.WriteStatusApplied, storage.WriteStatusPreconditionFailed}
	for index, result := range written.Results {
		if result.Status != expected[index] {
			t.Fatalf("operation %d: %+v, want %v", index, result, expected[index])
		}
	}
	kind, retryable := storage.ErrorDetails(written.Results[4].Err)
	if kind != storage.ErrorCodeInvalidArgument || retryable {
		t.Fatalf("validation rejection: %+v", written.Results[4])
	}
	read := storage.ReadRequest{}
	for _, operation := range request.Operations {
		readOperation := storage.ReadOperation{Address: operation.Address}
		read.Operations = append(read.Operations, readOperation)
	}
	persisted, err := fixture.store.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range persisted.Results {
		if index == 7 {
			if result.Status != storage.ReadStatusNotFound {
				t.Fatal("conditional bulk created a missing record")
			}
			continue
		}
		var value struct {
			Value int `bson:"value"`
		}
		if err := bson.Unmarshal(result.Document.Payload, &value); err != nil {
			t.Fatal(err)
		}
		want := 0
		if expected[index] == storage.WriteStatusApplied {
			want = 1
		}
		if value.Value != want {
			t.Fatalf("record %d: value=%d want=%d", index, value.Value, want)
		}
	}
}

func TestMongoDBConditionalWritesSelectCapabilityAndPreserveDurabilityErrors(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		name := "acknowledged"
		if uncertain {
			name = "unsatisfiable write concern"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			command := bson.D{{Key: "hello", Value: 1}}
			var hello struct {
				MaxWireVersion int      `bson:"maxWireVersion"`
				Hosts          []string `bson:"hosts"`
			}
			if err := fixture.client.Database("admin").RunCommand(t.Context(), command).Decode(&hello); err != nil {
				t.Fatal(err)
			}
			if len(hello.Hosts) == 0 {
				t.Fatal("this durability test requires a disposable replica set")
			}
			var bulks, updates atomic.Int64
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				switch e.CommandName {
				case "bulkWrite":
					bulks.Add(1)
				case "update":
					updates.Add(1)
				}
			}}
			clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
			if uncertain {
				concern := &writeconcern.WriteConcern{W: len(hello.Hosts) + 1}
				clientOptions.SetWriteConcern(concern)
			}
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
			seed := storage.WriteRequest{}
			initial := bson.D{{Key: "value", Value: 0}}
			for index := range 2 {
				operation := storage.WriteOperation{Address: fixture.address(fmt.Sprintf("concern-%d", index)), Document: bsonStorageDocument(t, initial)}
				seed.Operations = append(seed.Operations, operation)
			}
			created, err := fixture.store.Write(t.Context(), seed)
			if err != nil {
				t.Fatal(err)
			}
			request := storage.WriteRequest{}
			changed := bson.D{{Key: "value", Value: 1}}
			for index, operation := range seed.Operations {
				if created.Results[index].Status != storage.WriteStatusApplied {
					t.Fatalf("seed: %+v", created.Results[index])
				}
				operation.Document = bsonStorageDocument(t, changed)
				operation.Precondition = storage.Precondition{Kind: storage.PreconditionRevisionMatches, Revision: created.Results[index].Revision}
				request.Operations = append(request.Operations, operation)
			}
			written, err := store.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			for index, result := range written.Results {
				if uncertain {
					_, retryable := storage.ErrorDetails(result.Err)
					if result.Status != storage.WriteStatusFailed || !retryable {
						t.Fatalf("durability uncertainty acknowledged: %+v", result)
					}
				} else if result.Status != storage.WriteStatusApplied {
					t.Fatalf("write not applied: %+v", result)
				}
				filter := bson.D{{Key: "_id", Value: fmt.Sprintf("concern-%d", index)}}
				var persisted struct{ Value int }
				if err := fixture.collection.FindOne(t.Context(), filter).Decode(&persisted); err != nil || persisted.Value != 1 {
					t.Fatalf("committed state lost: value=%d err=%v", persisted.Value, err)
				}
			}
			if hello.MaxWireVersion >= 25 {
				if bulks.Load() != 1 || updates.Load() != 0 {
					t.Fatalf("expected one client bulk: bulk=%d update=%d", bulks.Load(), updates.Load())
				}
			} else if bulks.Load() != 0 || updates.Load() != 2 {
				t.Fatalf("legacy fallback: bulk=%d update=%d", bulks.Load(), updates.Load())
			}
		})
	}
}
