//go:build integration

package mongodb_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type heldNativeSnapshot struct {
	*mongodb.Store
	entered chan struct{}
	release chan struct{}
	held    atomic.Bool
	reads   atomic.Int32
}

func (s *heldNativeSnapshot) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	response, err := s.Store.Read(ctx, req)
	s.reads.Add(1)
	if err == nil && s.held.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return response, ctx.Err()
		}
	}
	return response, err
}

func nativeRevisionService(t *testing.T, backend storage.Storage) *service.Server {
	t.Helper()
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: backend, Lua: lua, MaxReadBytes: 1 << 20}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func nativeRevisionCommand(t *testing.T, input string) bson.D {
	t.Helper()
	var command bson.D
	if err := bson.UnmarshalExtJSON([]byte(input), false, &command); err != nil {
		t.Fatal(err)
	}
	return command
}

func nativeRevisionRead(t *testing.T, fixture *integrationFixture, key string) storage.ReadResult {
	t.Helper()
	operation := storage.ReadOperation{Address: fixture.address(key)}
	req := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
	response, err := fixture.store.Read(t.Context(), req)
	if err != nil || len(response.Results) != 1 || response.Results[0].Status != storage.ReadStatusFound {
		t.Fatalf("read failed: %+v %v", response, err)
	}
	result := response.Results[0]
	if len(result.Revision.Data) != 16 {
		t.Fatalf("missing revision: %+v", result)
	}
	return result
}

func nativeRevisionSeed(t *testing.T, fixture *integrationFixture, key string) {
	t.Helper()
	value := bson.D{{Key: "count", Value: int64(0)}}
	operation := storage.WriteOperation{Address: fixture.address(key), Document: bsonStorageDocument(t, value)}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	response, err := fixture.store.Write(t.Context(), request)
	if err != nil || response.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("seed failed: %+v %v", response, err)
	}
}

func TestNativeMongoWriteInvalidatesConcurrentMergeSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected int64
	}{
		{"operator", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":{"$inc":{"count":1}}}]}`, 2},
		{"replacement", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":{"count":10}}]}`, 11},
		{"pipeline", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":[{"$set":{"count":{"$add":["$count",1]}}}]}]}`, 2},
		{"add and unset pipeline", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":[{"$addFields":{"count":{"$add":["$count",1]},"temporary":1}},{"$unset":["temporary"]}]}]}`, 2},
		{"project pipeline", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":[{"$project":{"count":{"$add":["$count",1]}}}]}]}`, 2},
		{"replace root", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":[{"$replaceRoot":{"newRoot":{"count":10}}}]}]}`, 11},
		{"replace with", `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":[{"$replaceWith":{"count":10,"__sink":{"revision":"spoofed"}}}]}]}`, 11},
		{"findAndModify", `{"findAndModify":"documents","query":{"_id":"quota"},"update":{"$inc":{"count":1}},"new":true}`, 2},
		{"modify replacement", `{"findAndModify":"documents","query":{"_id":"quota"},"update":{"count":10},"new":true}`, 11},
		{"modify pipeline", `{"findAndModify":"documents","query":{"_id":"quota"},"update":[{"$set":{"count":{"$add":["$count",1]}}}],"new":true}`, 2},
	}
	for _, metadataField := range []string{"__sink", "__revision"} {
		for _, test := range tests {
			t.Run(metadataField+"/"+test.name, func(t *testing.T) {
				fixture := newIntegrationFixture(t)
				opts := mongodb.Options{Store: "primary", MetadataField: metadataField}
				firstStore, err := mongodb.New(fixture.client, opts)
				if err != nil {
					t.Fatal(err)
				}
				fixture.store = firstStore
				secondStore, err := mongodb.New(fixture.client, opts)
				if err != nil {
					t.Fatal(err)
				}
				nativeRevisionSeed(t, fixture, "quota")
				initial := nativeRevisionRead(t, fixture, "quota")
				backend := &heldNativeSnapshot{Store: firstStore, entered: make(chan struct{}), release: make(chan struct{})}
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(backend.release) }) }
				t.Cleanup(unblock)
				first := nativeRevisionService(t, backend)
				second := nativeRevisionService(t, secondStore)
				value := bson.D{{Key: "count", Value: int64(1)}}
				encoded := bsonStorageDocument(t, value)
				document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: encoded.Payload}
				program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) current.count = current.count + incoming.count; return current end`)}
				mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL}
				action := &sink.WriteOperation_Merge{Merge: mutation}
				kind := &sink.RecordKey_StringValue{StringValue: "quota"}
				key := &sink.RecordKey{Kind: kind}
				address := &sink.RecordAddress{Store: "primary", Namespace: fixture.database, Dataset: "documents", Key: key}
				operation := &sink.WriteOperation{Address: address, Action: action, ReturnDocument: true}
				request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
				type outcome struct {
					response *sink.WriteResponse
					err      error
				}
				done := make(chan outcome, 1)
				go func() {
					response, callErr := first.Write(t.Context(), request)
					result := outcome{response: response, err: callErr}
					done <- result
				}()
				select {
				case <-backend.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("Merge did not read its snapshot")
				}
				command := nativeRevisionCommand(t, test.command)
				native := mongoNativeRequest(t, fixture.database, command)
				wire := &sink.Command{Store: native.Store, Namespace: native.Namespace, ContentType: native.ContentType, Payload: native.Payload}
				execute := &sink.ExecuteRequest{Command: wire}
				response, err := second.Execute(t.Context(), execute)
				if err != nil || !response.GetSuccess() {
					t.Fatalf("native write failed: %+v %v", response, err)
				}
				afterNative := nativeRevisionRead(t, fixture, "quota")
				if bytes.Equal(initial.Revision.Data, afterNative.Revision.Data) {
					t.Error("native write retained the stale revision")
				}
				unblock()
				select {
				case result := <-done:
					if result.err != nil || len(result.response.GetResults()) != 1 || result.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
						t.Fatalf("Merge failed: %+v %v", result.response, result.err)
					}
					count := bson.Raw(result.response.Results[0].Document.Payload).Lookup("count").AsInt64()
					if count != test.expected || backend.reads.Load() < 2 {
						t.Fatalf("stale snapshot was committed: count=%d expected=%d reads=%d", count, test.expected, backend.reads.Load())
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Merge did not finish after native write")
				}
				final := nativeRevisionRead(t, fixture, "quota")
				if bson.Raw(final.Document.Payload).Lookup("count").AsInt64() != test.expected || bytes.Equal(final.Revision.Data, afterNative.Revision.Data) {
					t.Fatalf("incorrect persisted result: %+v", final)
				}
				if bson.Raw(final.Document.Payload).Lookup(metadataField).Type != 0 {
					t.Fatal("internal revision leaked through the record API")
				}
			})
		}
	}
}

func TestNativeMongoMultiUpdateUpsertAndRecreationRevisions(t *testing.T) {
	fixture := newIntegrationFixture(t)
	for _, key := range []string{"first", "second"} {
		nativeRevisionSeed(t, fixture, key)
	}
	first := nativeRevisionRead(t, fixture, "first")
	second := nativeRevisionRead(t, fixture, "second")
	command := nativeRevisionCommand(t, `{"update":"documents","updates":[{"q":{},"u":{"$inc":{"count":1}},"multi":true}]}`)
	req := mongoNativeRequest(t, fixture.database, command)
	response, err := fixture.store.Execute(t.Context(), req)
	if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("n").AsInt64() != 2 {
		t.Fatalf("multi update failed: %+v %v", response, err)
	}
	for i, key := range []string{"first", "second"} {
		updated := nativeRevisionRead(t, fixture, key)
		before := []storage.ReadResult{first, second}[i]
		if bytes.Equal(updated.Revision.Data, before.Revision.Data) || bson.Raw(updated.Document.Payload).Lookup("count").AsInt64() != 1 {
			t.Fatalf("multi update failed to version %s", key)
		}
	}
	for _, input := range []string{
		`{"update":"documents","updates":[{"q":{"_id":"upsert"},"u":{"$inc":{"count":1}},"upsert":true}]}`,
		`{"findAndModify":"documents","query":{"_id":"replace-upsert"},"update":{"count":1},"upsert":true,"new":true}`,
		`{"update":"documents","updates":[{"q":{"_id":"pipeline-upsert"},"u":[{"$set":{"count":1}}],"upsert":true}]}`,
	} {
		command = nativeRevisionCommand(t, input)
		req = mongoNativeRequest(t, fixture.database, command)
		response, err = fixture.store.Execute(t.Context(), req)
		if err != nil || !response.Success {
			t.Fatalf("upsert failed: %+v %v", response, err)
		}
	}
	for _, key := range []string{"upsert", "replace-upsert", "pipeline-upsert"} {
		nativeRevisionRead(t, fixture, key)
	}
	for _, deletion := range []string{
		`{"delete":"documents","deletes":[{"q":{"_id":"first"},"limit":1}]}`,
		`{"findAndModify":"documents","query":{"_id":"first"},"remove":true}`,
	} {
		before := nativeRevisionRead(t, fixture, "first")
		command = nativeRevisionCommand(t, deletion)
		req = mongoNativeRequest(t, fixture.database, command)
		response, err = fixture.store.Execute(t.Context(), req)
		if err != nil || !response.Success {
			t.Fatalf("delete failed: %+v %v", response, err)
		}
		command = nativeRevisionCommand(t, `{"insert":"documents","documents":[{"_id":"first","count":7}]}`)
		req = mongoNativeRequest(t, fixture.database, command)
		response, err = fixture.store.Execute(t.Context(), req)
		if err != nil || !response.Success {
			t.Fatalf("reinsert failed: %+v %v", response, err)
		}
		recreated := nativeRevisionRead(t, fixture, "first")
		if bytes.Equal(before.Revision.Data, recreated.Revision.Data) {
			t.Fatal("recreation reused revision")
		}
		precondition := storage.Precondition{Kind: storage.PreconditionRevisionMatches, Revision: before.Revision}
		operation := storage.WriteOperation{Address: fixture.address("first"), Document: before.Document, Precondition: precondition}
		write := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
		written, err := fixture.store.Write(t.Context(), write)
		if err != nil || written.Results[0].Status != storage.WriteStatusPreconditionFailed {
			t.Fatalf("stale CAS overwrote recreated record: %+v %v", written, err)
		}
	}
}

func TestNativeMongoUnsafeBatchHasNoPartialEffects(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "first")
	before := nativeRevisionRead(t, fixture, "first")
	for _, input := range []string{
		`{"update":"documents","updates":[{"q":{"_id":"first"},"u":{"$inc":{"count":1}}},{"q":{},"u":{"$unset":{"__sink":1}}}],"ordered":false}`,
		`{"insert":"documents","documents":[{"_id":"new"},{"_id":"bad","__sink":{}}],"ordered":true}`,
		`{"update":"documents","updates":[{"q":{},"u":{"$rename":{"count":"__sink.revision"}}}]}`,
		`{"drop":"documents"}`,
	} {
		command := nativeRevisionCommand(t, input)
		req := mongoNativeRequest(t, fixture.database, command)
		_, err := fixture.store.Execute(t.Context(), req)
		code, _ := storage.ErrorDetails(err)
		if code != storage.ErrorCodeInvalidArgument {
			t.Fatalf("unsafe command accepted: %s %v", input, err)
		}
		after := nativeRevisionRead(t, fixture, "first")
		if !bytes.Equal(before.Revision.Data, after.Revision.Data) || !bytes.Equal(before.Document.Payload, after.Document.Payload) {
			t.Fatal("rejected command had a partial effect")
		}
		filter := bson.D{}
		count, err := fixture.collection.CountDocuments(t.Context(), filter)
		if err != nil || count != 1 {
			t.Fatalf("rejected insert had a partial effect: count=%d error=%v", count, err)
		}
	}
}

func TestNativeMongoFailedWriteDoesNotAdvanceRevision(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "first")
	before := nativeRevisionRead(t, fixture, "first")
	for _, input := range []string{
		`{"update":"documents","updates":[{"q":{"_id":"first"},"u":{"$inc":{"count":"invalid"}}}]}`,
		`{"findAndModify":"documents","query":{"_id":"first"},"update":{"$inc":{"count":"invalid"}},"new":true}`,
		`{"update":"documents","updates":[{"q":{"_id":"first"},"u":{"_id":"changed","count":2}}]}`,
		`{"update":"documents","updates":[{"q":{"_id":"first"},"u":[{"$set":{"count":{"$divide":[1,0]}}}]}]}`,
	} {
		command := nativeRevisionCommand(t, input)
		req := mongoNativeRequest(t, fixture.database, command)
		response, err := fixture.store.Execute(t.Context(), req)
		if err != nil || response.Success || len(response.Payload) == 0 {
			t.Fatalf("native database failure was lost: %+v %v", response, err)
		}
		after := nativeRevisionRead(t, fixture, "first")
		if !bytes.Equal(before.Revision.Data, after.Revision.Data) || !bytes.Equal(before.Document.Payload, after.Document.Payload) {
			t.Fatal("failed business mutation still changed the revision or document")
		}
	}
}

func TestNativeMongoWriteVersionsLegacyDocuments(t *testing.T) {
	fixture := newIntegrationFixture(t)
	legacy := bson.D{{Key: "_id", Value: "legacy"}, {Key: "count", Value: int64(0)}}
	if _, err := fixture.collection.InsertOne(t.Context(), legacy); err != nil {
		t.Fatal(err)
	}
	command := nativeRevisionCommand(t, `{"update":"documents","updates":[{"q":{"_id":"legacy"},"u":{"$inc":{"count":1}}}]}`)
	req := mongoNativeRequest(t, fixture.database, command)
	response, err := fixture.store.Execute(t.Context(), req)
	if err != nil || !response.Success {
		t.Fatalf("legacy native update failed: %+v %v", response, err)
	}
	updated := nativeRevisionRead(t, fixture, "legacy")
	if bson.Raw(updated.Document.Payload).Lookup("count").AsInt64() != 1 {
		t.Fatal("legacy update was lost")
	}
	precondition := storage.Precondition{Kind: storage.PreconditionRevisionAbsent}
	operation := storage.WriteOperation{Address: fixture.address("legacy"), Document: bsonStorageDocument(t, legacy), Precondition: precondition}
	write := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	written, err := fixture.store.Write(t.Context(), write)
	if err != nil || written.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("pre-migration CAS overwrote native update: %+v %v", written, err)
	}
}
