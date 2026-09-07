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

func TestMongoDBFoldedPutAndMergePreserveConditionsAndBSON(t *testing.T) {
	fixture := newIntegrationFixture(t)
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: fixture.store, Lua: lua}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	base := fixture.address("folded-put")
	kind := &sink.RecordKey_StringValue{StringValue: "folded-put"}
	key := &sink.RecordKey{Kind: kind}
	address := &sink.RecordAddress{Store: base.Store, Namespace: base.Namespace, Dataset: base.Dataset, Key: key}
	created := time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	modes := []sink.WriteMode{sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_REPLACE, sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_UPSERT}
	for index, mode := range modes {
		value := bson.M{"_id": "folded-put", "counter": index + 1, "created_at": created}
		payload, err := bson.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: payload}
		put := &sink.PutOperation{Mode: mode, Document: document}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Address: address, Action: action}
		request.Operations = append(request.Operations, operation)
	}
	incoming := bson.M{"delta": 1}
	payload, err := bson.Marshal(incoming)
	if err != nil {
		t.Fatal(err)
	}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: payload}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) current.counter=current.counter+incoming.delta return current end`)}
	mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL}
	action := &sink.WriteOperation_Merge{Merge: mutation}
	operation := &sink.WriteOperation{Address: address, Action: action}
	request.Operations = append(request.Operations, operation)
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if index == 2 {
			if result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED {
				t.Fatalf("duplicate create: %v", result)
			}
			continue
		}
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || len(result.GetRevision().GetData()) == 0 || !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatal(result)
		}
	}
	filter := bson.M{"_id": "folded-put"}
	var stored struct {
		Counter int       `bson:"counter"`
		Created time.Time `bson:"created_at"`
	}
	if err := fixture.collection.FindOne(t.Context(), filter).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Counter != 5 || !stored.Created.Equal(created) {
		t.Fatalf("folded BSON state: %+v", stored)
	}
}
