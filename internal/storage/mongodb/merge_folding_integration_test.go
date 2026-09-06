//go:build integration

package mongodb_test

import (
	"bytes"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoDBFoldedMergesPreserveBSONAndFinalRevision(t *testing.T) {
	fixture := newIntegrationFixture(t)
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{Storage: fixture.store, Lua: engine}
	server, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	base := fixture.address("folded")
	keyValue := &sink.RecordKey_StringValue{StringValue: "folded"}
	key := &sink.RecordKey{Kind: keyValue}
	address := &sink.RecordAddress{Store: base.Store, Namespace: base.Namespace, Dataset: base.Dataset, Key: key}
	created := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	value := bson.M{"_id": "folded", "counter": 1, "created_at": created}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	const source = `return function(current, incoming)
        if current == nil then return incoming end
        current.counter = current.counter + incoming.counter
        return current
    end`
	const count = 16
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	for range count {
		document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: payload}
		program := &sink.LuaProgram{Source: []byte(source)}
		mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE}
		action := &sink.WriteOperation_Merge{Merge: mutation}
		operation := &sink.WriteOperation{Address: address, Action: action}
		request.Operations = append(request.Operations, operation)
	}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || result.Failure != nil {
			t.Fatal(result)
		}
		if len(result.GetRevision().GetData()) == 0 || !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatal("folded BSON merges did not share one revision")
		}
	}
	filter := bson.M{"_id": "folded"}
	var stored struct {
		Counter int       `bson:"counter"`
		Created time.Time `bson:"created_at"`
	}
	if err := fixture.collection.FindOne(t.Context(), filter).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Counter != count || !stored.Created.Equal(created) {
		t.Fatalf("folded BSON document = %+v", stored)
	}
}
