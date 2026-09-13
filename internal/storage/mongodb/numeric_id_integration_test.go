//go:build integration

package mongodb_test

import (
	"encoding/binary"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoDBNumericIDRoundTrip(t *testing.T) {
	decimal, err := bson.ParseDecimal128("42.00")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		id    any
		valid bool
	}{
		{name: "int32", id: int32(42), valid: true},
		{name: "double", id: float64(42), valid: true},
		{name: "decimal", id: decimal, valid: true},
		{name: "fraction", id: 42.5},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			data := make([]byte, 8)
			binary.BigEndian.PutUint64(data, 42)
			key := storage.Key{Type: "int64", Data: data}
			address := storage.Address{Store: "primary", Namespace: fixture.database, Dataset: "documents", Key: key}
			value := bson.D{{Key: "_id", Value: test.id}, {Key: "value", Value: "created"}}
			document := bsonStorageDocument(t, value)
			condition := storage.Precondition{Kind: storage.PreconditionRecordNotExists}
			operation := storage.WriteOperation{Address: address, Document: document, Precondition: condition}
			request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			created, err := fixture.store.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if !test.valid {
				code, _ := storage.ErrorDetails(created.Results[0].Err)
				if created.Results[0].Status != storage.WriteStatusFailed || code != storage.ErrorCodeInvalidArgument {
					t.Fatalf("mismatched ID was accepted: %+v", created.Results[0])
				}
				filter := bson.D{}
				count, err := fixture.collection.CountDocuments(t.Context(), filter)
				if err != nil || count != 0 {
					t.Fatalf("invalid write reached MongoDB: %d, %v", count, err)
				}
				return
			}
			if created.Results[0].Status != storage.WriteStatusApplied {
				t.Fatal(created.Results[0])
			}
			readOperation := storage.ReadOperation{Address: address}
			readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{readOperation}}
			read, err := fixture.store.Read(t.Context(), readRequest)
			if err != nil || read.Results[0].Status != storage.ReadStatusFound {
				t.Fatalf("read: %+v, %v", read, err)
			}
			if bson.Raw(read.Results[0].Document.Payload).Lookup("value").StringValue() != "created" {
				t.Fatal(read)
			}

			value[1].Value = "replaced"
			request.Operations[0].Document = bsonStorageDocument(t, value)
			request.Operations[0].Precondition = storage.Precondition{Kind: storage.PreconditionRevisionMatches, Revision: read.Results[0].Revision}
			replaced, err := fixture.store.Write(t.Context(), request)
			if err != nil || replaced.Results[0].Status != storage.WriteStatusApplied {
				t.Fatalf("replace: %+v, %v", replaced, err)
			}
			read, err = fixture.store.Read(t.Context(), readRequest)
			if err != nil || read.Results[0].Status != storage.ReadStatusFound || bson.Raw(read.Results[0].Document.Payload).Lookup("value").StringValue() != "replaced" {
				t.Fatalf("read replacement: %+v, %v", read, err)
			}
			deleteOperation := storage.DeleteOperation{Address: address}
			deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{deleteOperation}}
			deleted, err := fixture.store.Delete(t.Context(), deleteRequest)
			if err != nil || deleted.Results[0].Status != storage.DeleteStatusApplied {
				t.Fatalf("delete: %+v, %v", deleted, err)
			}
			read, err = fixture.store.Read(t.Context(), readRequest)
			if err != nil || read.Results[0].Status != storage.ReadStatusNotFound {
				t.Fatalf("read deleted: %+v, %v", read, err)
			}
		})
	}
}
