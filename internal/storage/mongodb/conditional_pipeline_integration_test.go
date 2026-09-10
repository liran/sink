//go:build integration

package mongodb_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoDBLargeConditionalReplacementPreservesLiteralBSONAndUsesDeltaOplog(t *testing.T) {
	fixture := newIntegrationFixture(t)
	command := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		MaxWireVersion int `bson:"maxWireVersion"`
	}
	if err := fixture.client.Database("admin").RunCommand(t.Context(), command).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if hello.MaxWireVersion < 25 {
		t.Skip("MongoDB 8 client bulk is required for this optimization")
	}
	decimal, err := bson.ParseDecimal128("123.456")
	if err != nil {
		t.Fatal(err)
	}
	identifier := bson.NewObjectID()
	expected := bson.D{
		{Key: "padding", Value: strings.Repeat("x", 64<<10)},
		{Key: "value", Value: int64(1)},
		{Key: "literal", Value: "$value"},
		{Key: "expression", Value: bson.D{{Key: "$add", Value: bson.A{1, 2}}}},
		{Key: "date", Value: bson.DateTime(123456)},
		{Key: "decimal", Value: decimal},
		{Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}},
		{Key: "identifier", Value: identifier},
		{Key: "regex", Value: bson.Regex{Pattern: "literal", Options: "im"}},
		{Key: "timestamp", Value: bson.Timestamp{T: 123, I: 4}},
		{Key: "bounds", Value: bson.A{bson.MinKey{}, bson.MaxKey{}, nil}},
		{Key: "javascript", Value: bson.JavaScript("return '$value';")},
		{Key: "scope", Value: bson.CodeWithScope{Code: "return x;", Scope: bson.D{{Key: "x", Value: int32(7)}}}},
		{Key: "undefined", Value: bson.Undefined{}},
		{Key: "pointer", Value: bson.DBPointer{DB: "literal", Pointer: identifier}},
		{Key: "symbol", Value: bson.Symbol("literal")},
	}
	initial := slices.Clone(expected)
	initial[1].Value = int64(0)
	removed := bson.E{Key: "removed", Value: "must disappear"}
	initial = append(initial, removed)
	seed := storage.WriteRequest{}
	for index := range 2 {
		operation := storage.WriteOperation{Address: fixture.address(fmt.Sprintf("literal-%d", index)), Document: bsonStorageDocument(t, initial)}
		seed.Operations = append(seed.Operations, operation)
	}
	created, err := fixture.store.Write(t.Context(), seed)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.WriteRequest{}
	document := bsonStorageDocument(t, expected)
	for index, operation := range seed.Operations {
		if created.Results[index].Status != storage.WriteStatusApplied {
			t.Fatalf("seed: %+v", created.Results[index])
		}
		operation.Document = document
		operation.Precondition = storage.Precondition{Kind: storage.PreconditionRevisionMatches, Revision: created.Results[index].Revision}
		request.Operations = append(request.Operations, operation)
	}
	response, err := fixture.store.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	// A streamed working-set tail can contain only one record. It must retain
	// the delta oplog path while preserving the same literal replacement.
	singleOperation := request.Operations[0]
	singleOperation.Precondition.Revision = response.Results[0].Revision
	singleRequest := storage.WriteRequest{Operations: []storage.WriteOperation{singleOperation}}
	singleResponse, err := fixture.store.Write(t.Context(), singleRequest)
	if err != nil || len(singleResponse.Results) != 1 {
		t.Fatalf("singleton replacement: %+v, %v", singleResponse, err)
	}
	response.Results[0] = singleResponse.Results[0]
	wantElements, err := bson.Raw(document.Payload).Elements()
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.Status != storage.WriteStatusApplied || slices.Equal(result.Revision.Data, created.Results[index].Revision.Data) {
			t.Fatalf("conditional replacement: %+v", result)
		}
		key := fmt.Sprintf("literal-%d", index)
		filter := bson.D{{Key: "_id", Value: key}}
		var stored bson.Raw
		if err := fixture.collection.FindOne(t.Context(), filter).Decode(&stored); err != nil {
			t.Fatal(err)
		}
		for _, element := range wantElements {
			if !stored.Lookup(element.Key()).Equal(element.Value()) {
				t.Fatalf("replacement changed literal BSON field %q", element.Key())
			}
		}
		if stored.Lookup("removed").Type != 0 {
			t.Fatal("replacement retained a deleted field")
		}
		oplogFilter := bson.D{{Key: "ns", Value: fixture.database + ".documents"}, {Key: "op", Value: "u"}, {Key: "o2._id", Value: key}}
		sort := bson.D{{Key: "$natural", Value: -1}}
		findOptions := options.FindOne().SetSort(sort)
		var entry struct{ O bson.Raw }
		err := fixture.client.Database("local").Collection("oplog.rs").FindOne(t.Context(), oplogFilter, findOptions).Decode(&entry)
		if err != nil {
			t.Fatal(err)
		}
		if entry.O.Lookup("diff").Type != bson.TypeEmbeddedDocument || len(entry.O) > 4096 {
			t.Fatalf("large unchanged padding was copied into the oplog: %d bytes", len(entry.O))
		}
	}
}
