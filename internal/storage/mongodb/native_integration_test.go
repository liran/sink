//go:build integration

package mongodb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func mongoNativeRequest(t *testing.T, database string, command bson.D) storage.NativeRequest {
	t.Helper()
	payload, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.NativeRequest{Store: "primary", Namespace: database, ContentType: "application/bson", Payload: payload, MaxBytes: 1 << 20}
	return request
}

func TestNativeMongoIndexesRawErrorsAndCursorCleanup(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	index := bson.D{{Key: "key", Value: bson.D{{Key: "signature", Value: 1}}}, {Key: "name", Value: "signature"}, {Key: "unique", Value: true}}
	command := bson.D{{Key: "createIndexes", Value: "documents"}, {Key: "indexes", Value: bson.A{index}}}
	req := mongoNativeRequest(t, fixture.database, command)
	for range 2 {
		response, err := fixture.store.Execute(ctx, req)
		if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("ok").Double() != 1 {
			t.Fatalf("create indexes response=%+v err=%v", response, err)
		}
	}
	for i := range 7 {
		document := bson.D{{Key: "_id", Value: fmt.Sprint(i)}, {Key: "signature", Value: fmt.Sprint(i)}, {Key: "number", Value: i}, {Key: "date", Value: time.Now()}}
		if _, err := fixture.collection.InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
	}
	duplicate := bson.D{{Key: "_id", Value: "duplicate"}, {Key: "signature", Value: "0"}}
	if _, err := fixture.collection.InsertOne(ctx, duplicate); !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("unique index did not protect signature: %v", err)
	}
	invalid := bson.D{{Key: "count", Value: "documents"}, {Key: "unknownOption", Value: true}}
	req = mongoNativeRequest(t, fixture.database, invalid)
	response, err := fixture.store.Execute(ctx, req)
	if err != nil || response.Success || len(response.Payload) == 0 || bson.Raw(response.Payload).Lookup("errmsg").Type != bson.TypeString {
		t.Fatalf("database error response lost: %+v err=%v", response, err)
	}
	var getMore atomic.Int32
	var closed atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
		if command.CommandName == "getMore" {
			getMore.Add(1)
		}
		if command.CommandName == "killCursors" {
			closed.Add(1)
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
	filter := bson.D{{Key: "number", Value: bson.D{{Key: "$gte", Value: 1}}}}
	sorting := bson.D{{Key: "number", Value: 1}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}, {Key: "sort", Value: sorting}}
	native := mongoNativeRequest(t, fixture.database, find)
	scan := storage.ScanRequest{Request: native, BatchSize: 2}
	seen := 0
	visit := func(documents []storage.Document) error {
		for _, document := range documents {
			seen++
			raw := bson.Raw(document.Payload)
			if raw.Lookup("number").Int32() != int32(seen) || raw.Lookup("date").Type != bson.TypeDateTime {
				return fmt.Errorf("scan did not preserve sort or BSON types: %s", raw)
			}
		}
		return nil
	}
	if err := store.Scan(ctx, scan, visit); err != nil || seen != 6 || getMore.Load() == 0 {
		t.Fatalf("seen=%d getMore=%d err=%v", seen, getMore.Load(), err)
	}
	stop := errors.New("callback stopped")
	visit = func(_ []storage.Document) error { return stop }
	if err := store.Scan(ctx, scan, visit); !errors.Is(err, stop) || closed.Load() == 0 {
		t.Fatalf("cursor cleanup=%d err=%v", closed.Load(), err)
	}
	match := bson.D{{Key: "$match", Value: filter}}
	group := bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$number"}}}}}}
	aggregate := bson.D{{Key: "aggregate", Value: "documents"}, {Key: "pipeline", Value: bson.A{match, group}}}
	scan.Request = mongoNativeRequest(t, fixture.database, aggregate)
	total := int64(0)
	visit = func(documents []storage.Document) error {
		for _, document := range documents {
			total = bson.Raw(document.Payload).Lookup("total").AsInt64()
		}
		return nil
	}
	if err := store.Scan(ctx, scan, visit); err != nil || total != 21 {
		t.Fatalf("aggregation total=%d err=%v", total, err)
	}
	list := bson.D{{Key: "listIndexes", Value: "documents"}}
	scan.Request = mongoNativeRequest(t, fixture.database, list)
	indexes := 0
	visit = func(documents []storage.Document) error { indexes += len(documents); return nil }
	if err := store.Scan(ctx, scan, visit); err != nil || indexes != 2 {
		t.Fatalf("indexes=%d err=%v", indexes, err)
	}
}

func TestMongoReturningMergeCommitsIndependentCounterValues(t *testing.T) {
	fixture := newIntegrationFixture(t)
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := service.Options{Storage: fixture.store, Lua: lua, MaxReadBytes: 1 << 20}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	incoming := bson.D{{Key: "count", Value: int32(1)}}
	encoded, err := bson.Marshal(incoming)
	if err != nil {
		t.Fatal(err)
	}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: encoded}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) current = current or {count = 0}; current.count = current.count + incoming.count; return current end`)}
	action := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE}
	wrapper := &sink.WriteOperation_Merge{Merge: action}
	keyKind := &sink.RecordKey_StringValue{StringValue: "quota"}
	key := &sink.RecordKey{Kind: keyKind}
	address := &sink.RecordAddress{Store: "primary", Namespace: fixture.database, Dataset: "documents", Key: key}
	first := &sink.WriteOperation{Address: address, Action: wrapper, ReturnDocument: true}
	second := &sink.WriteOperation{Address: address, Action: wrapper, ReturnDocument: true}
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{first, second}}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("write failed: %v", result)
		}
		count, ok := bson.Raw(result.GetDocument().GetPayload()).Lookup("count").AsInt64OK()
		if !ok || count != int64(index+1) {
			t.Fatalf("counter=%d ok=%v", count, ok)
		}
	}
	if bytes.Equal(response.Results[0].Revision.Data, response.Results[1].Revision.Data) {
		t.Fatal("writes shared a revision")
	}
}

func TestNativeMongoWritesPreserveResultsAndWriteErrors(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	document := bson.D{{Key: "_id", Value: "native"}, {Key: "count", Value: int64(1)}}
	insert := bson.D{{Key: "insert", Value: "documents"}, {Key: "documents", Value: bson.A{document}}}
	req := mongoNativeRequest(t, fixture.database, insert)
	response, err := fixture.store.Execute(ctx, req)
	if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("n").AsInt64() != 1 {
		t.Fatalf("native insert response=%+v err=%v", response, err)
	}
	response, err = fixture.store.Execute(ctx, req)
	if err != nil || response.Success {
		t.Fatalf("duplicate insert did not return native failure: %+v err=%v", response, err)
	}
	raw := bson.Raw(response.Payload)
	writeErrors, arrayErr := raw.Lookup("writeErrors").Array().Values()
	if arrayErr != nil || len(writeErrors) != 1 || writeErrors[0].Document().Lookup("code").AsInt64() != 11000 || raw.Lookup("ok").Double() != 1 {
		t.Fatalf("write error envelope lost: %s err=%v", raw, arrayErr)
	}
	modify := bson.D{{Key: "findAndModify", Value: "documents"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "native"}}},
		{Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int64(1)}}}}}, {Key: "new", Value: true}}
	req = mongoNativeRequest(t, fixture.database, modify)
	response, err = fixture.store.Execute(ctx, req)
	if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("value", "count").Int64() != 2 {
		t.Fatalf("findAndModify response=%+v err=%v", response, err)
	}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "batchSize", Value: int32(1)}}
	req = mongoNativeRequest(t, fixture.database, find)
	_, err = fixture.store.Execute(ctx, req)
	code, _ := storage.ErrorDetails(err)
	if code != storage.ErrorCodeInvalidArgument {
		t.Fatalf("cursor command must be rejected: %v", err)
	}
	unknown := bson.D{{Key: "sinkUnknownNativeCommand", Value: 1}}
	req = mongoNativeRequest(t, fixture.database, unknown)
	_, err = fixture.store.Execute(ctx, req)
	code, _ = storage.ErrorDetails(err)
	if code != storage.ErrorCodeInvalidArgument {
		t.Fatalf("unknown command escaped revision protection: %v", err)
	}
	drop := bson.D{{Key: "drop", Value: "documents"}}
	req = mongoNativeRequest(t, fixture.database, drop)
	_, err = fixture.store.Execute(ctx, req)
	code, _ = storage.ErrorDetails(err)
	if code != storage.ErrorCodeInvalidArgument {
		t.Fatalf("native drop escaped revision protection: %v", err)
	}
}
