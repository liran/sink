//go:build integration

package mongodb_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"github.com/liran/sink/internal/worker"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"
)

func testOperationID(key string, created time.Time) string {
	digest := sha256.Sum256([]byte(key))
	return fmt.Sprintf("v1:%d:%s", created.UnixMilli(), base64.RawURLEncoding.EncodeToString(digest[:]))
}

func idempotentMergeRequest(t *testing.T, fixture *integrationFixture, id string) *sink.WriteRequest {
	t.Helper()
	value := bson.D{{Key: "count", Value: int64(1)}}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: payload}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) current.count = current.count + incoming.count; return current end`)}
	mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL}
	action := &sink.WriteOperation_Merge{Merge: mutation}
	kind := &sink.RecordKey_StringValue{StringValue: "quota"}
	key := &sink.RecordKey{Kind: kind}
	address := &sink.RecordAddress{Store: "primary", Namespace: fixture.database, Dataset: "documents", Key: key}
	operation := &sink.WriteOperation{Address: address, Action: action, ReturnDocument: true, OperationId: id}
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
	return request
}

type lostReceiptStore struct {
	*mongodb.Store
	lost atomic.Bool
}

func (s *lostReceiptStore) ExecuteOnce(ctx context.Context, req storage.IdempotentRequest) ([]byte, error) {
	result, err := s.Store.ExecuteOnce(ctx, req)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return nil, storage.BackendError(errors.New("response lost after transaction commit"))
	}
	return result, err
}

func TestIdempotentMongoLostAckAndConcurrentReplay(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	backend := &lostReceiptStore{Store: fixture.store}
	first := nativeRevisionService(t, backend)
	request := idempotentMergeRequest(t, fixture, testOperationID("increment", time.Now()))
	response, err := first.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_FAILED {
		t.Fatalf("lost ack not surfaced: %v %v", response, err)
	}
	options := mongodb.Options{Store: "primary"}
	otherStore, err := mongodb.New(fixture.client, options)
	if err != nil {
		t.Fatal(err)
	}
	second := nativeRevisionService(t, otherStore)
	var wg sync.WaitGroup
	results := make(chan *sink.WriteResult, 3)
	for range 3 {
		wg.Go(func() {
			response, err := second.WriteIdempotent(t.Context(), request)
			if err != nil || len(response.GetResults()) != 1 {
				t.Errorf("replay: %v %v", response, err)
				return
			}
			results <- response.Results[0]
		})
	}
	wg.Wait()
	close(results)
	var revision []byte
	for result := range results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || bson.Raw(result.GetDocument().GetPayload()).Lookup("count").AsInt64() != 1 {
			t.Fatalf("bad receipt: %v", result)
		}
		if revision != nil && !bytes.Equal(revision, result.Revision.Data) {
			t.Fatal("replay returned another revision")
		}
		revision = result.Revision.Data
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 1 || !bytes.Equal(stored.Revision.Data, revision) {
		t.Fatal("lost ack caused duplicate effect")
	}
	changed := proto.Clone(request).(*sink.WriteRequest)
	changed.Operations[0].ReturnDocument = false
	response, err = second.WriteIdempotent(t.Context(), changed)
	if err != nil || response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_CONFLICT || response.Results[0].GetFailure().GetRetryable() {
		t.Fatalf("ID reuse not rejected: %v %v", response, err)
	}
	command := nativeRevisionCommand(t, `{"update":"documents","updates":[{"q":{"_id":"quota"},"u":{"count":100}}]}`)
	native := mongoNativeRequest(t, fixture.database, command)
	if _, err := fixture.store.Execute(t.Context(), native); err != nil {
		t.Fatal(err)
	}
	response, err = second.WriteIdempotent(t.Context(), request)
	if err != nil || !bytes.Equal(response.Results[0].Revision.Data, revision) {
		t.Fatalf("native replacement erased receipt: %v %v", response, err)
	}
	stored = nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 100 {
		t.Fatal("replay overwrote later native update")
	}
	deletion := storage.DeleteOperation{Address: fixture.address("quota")}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{deletion}}
	if _, err := fixture.store.Delete(t.Context(), deleteRequest); err != nil {
		t.Fatal(err)
	}
	response, err = second.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("delete erased receipt: %v %v", response, err)
	}
	filter := bson.D{{Key: "_id", Value: "quota"}}
	count, err := fixture.collection.CountDocuments(t.Context(), filter)
	if err != nil || count != 0 {
		t.Fatalf("replay resurrected deleted record: %d %v", count, err)
	}
}

func TestIdempotentMongoAtomicRollbackAndExpiry(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	server := nativeRevisionService(t, fixture.store)
	id := testOperationID("rollback", time.Now())
	request := idempotentMergeRequest(t, fixture, id)
	// Force receipt failure AFTER the document write. The transaction must
	// roll both back, and a later retry with the same ID must still apply once.
	fingerprint := sha256.Sum256([]byte("rollback"))
	apply := func(ctx context.Context) ([]byte, error) {
		value := bson.D{{Key: "count", Value: int64(99)}}
		operation := storage.WriteOperation{Address: fixture.address("quota"), Document: bsonStorageDocument(t, value)}
		write := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
		response, err := fixture.store.Write(ctx, write)
		if err != nil || response.Results[0].Status != storage.WriteStatusApplied {
			return nil, fmt.Errorf("write failed: %v %v", response, err)
		}
		return []byte("too large"), nil
	}
	once := storage.IdempotentRequest{Address: fixture.address("quota"), OperationID: id, Fingerprint: fingerprint[:], MaxReceiptBytes: 1, Apply: apply}
	if _, err := fixture.store.ExecuteOnce(t.Context(), once); err == nil {
		t.Fatal("oversize receipt committed")
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 0 {
		t.Fatal("receipt failure leaked business write")
	}
	response, err := server.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("rollback retry: %v %v", response, err)
	}
	expired := proto.Clone(request).(*sink.WriteRequest)
	expired.Operations[0].OperationId = testOperationID("rollback", time.Now().Add(-32*24*time.Hour))
	if _, err := server.WriteIdempotent(t.Context(), expired); err == nil {
		t.Fatal("expired operation was accepted")
	}
}

func TestIdempotentMongoWorkerDuplicateDelivery(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	server := nativeRevisionService(t, fixture.store)
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	request := idempotentMergeRequest(t, fixture, testOperationID("queued", time.Now()))
	request.Operations[0].ReturnDocument = false
	mutation := queue.Mutation{Write: request.Operations[0]}
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	if payload[4] != 2 {
		t.Fatal("old workers could ignore idempotency")
	}
	for range 3 {
		replayed, err := queue.UnmarshalMutation(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.Handle(t.Context(), replayed); err != nil {
			t.Fatal(err)
		}
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 1 {
		t.Fatal("worker redelivery repeated increment")
	}
}

func TestIdempotentMongoConcurrentFirstCommit(t *testing.T) {
	for _, sameID := range []bool{true, false} {
		fixture := newIntegrationFixture(t)
		nativeRevisionSeed(t, fixture, "quota")
		server := nativeRevisionService(t, fixture.store)
		var wg sync.WaitGroup
		created := time.Now()
		for index := range 3 {
			wg.Go(func() {
				key := fmt.Sprintf("operation-%d", index)
				if sameID {
					key = "shared"
				}
				request := idempotentMergeRequest(t, fixture, testOperationID(key, created))
				response, err := server.WriteIdempotent(t.Context(), request)
				if err != nil || len(response.GetResults()) != 1 || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Errorf("concurrent first write: %v %v", response, err)
				}
			})
		}
		wg.Wait()
		want := int64(3)
		if sameID {
			want = 1
		}
		stored := nativeRevisionRead(t, fixture, "quota")
		if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != want {
			t.Fatalf("sameID=%v expected count=%d: %s", sameID, want, stored.Document.Payload)
		}
	}
}

type heldReceiptStore struct {
	*mongodb.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *heldReceiptStore) ExecuteOnce(ctx context.Context, req storage.IdempotentRequest) ([]byte, error) {
	apply := req.Apply
	req.Apply = func(ctx context.Context) ([]byte, error) {
		result, err := apply(ctx)
		if err != nil {
			return nil, err
		}
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
			return result, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.ExecuteOnce(ctx, req)
}

func TestIdempotentMongoBatchWaitsForReceiptCommit(t *testing.T) {
	for _, cancelBeforeCommit := range []bool{false, true} {
		fixture := newIntegrationFixture(t)
		nativeRevisionSeed(t, fixture, "quota")
		backend := &heldReceiptStore{Store: fixture.store, entered: make(chan struct{}), release: make(chan struct{})}
		core := nativeRevisionService(t, backend)
		opts := service.BatchingOptions{StoreNames: []string{"primary"}}
		server, err := service.NewBatchingServer(core, opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(server.Close)
		var release sync.Once
		unblock := func() { release.Do(func() { close(backend.release) }) }
		t.Cleanup(unblock)
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		request := idempotentMergeRequest(t, fixture, testOperationID("held-commit", time.Now()))
		done := make(chan error, 1)
		go func() { _, err := server.WriteIdempotent(ctx, request); done <- err }()
		select {
		case <-backend.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("transaction did not reach receipt boundary")
		}
		select {
		case err := <-done:
			t.Fatalf("acknowledged before receipt commit: %v", err)
		default:
		}
		stored := nativeRevisionRead(t, fixture, "quota")
		if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 0 {
			t.Fatal("uncommitted transaction became visible")
		}
		if cancelBeforeCommit {
			cancel()
		} else {
			unblock()
		}
		select {
		case err := <-done:
			if (err != nil) != cancelBeforeCommit {
				t.Fatalf("completion: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("transaction did not settle")
		}
		unblock()
		// The batcher can return cancellation before its executor has aborted;
		// Close drains that executor before inspecting the persisted state.
		server.Close()
		stored = nativeRevisionRead(t, fixture, "quota")
		want := int64(1)
		if cancelBeforeCommit {
			want = 0
		}
		if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != want {
			t.Fatalf("commit/cancel boundary expected count=%d", want)
		}
	}
}

func TestIdempotentMongoSurvivesProcessKillBeforeDeliverySettlement(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	id := testOperationID("process-crash", time.Now())
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestIdempotentMongoCrashHelper$")
	command.Env = append(os.Environ(), "SINK_IDEMPOTENCY_CRASH_DATABASE="+fixture.database, "SINK_IDEMPOTENCY_CRASH_ID="+id)
	output, err := command.CombinedOutput()
	var killed *exec.ExitError
	if !errors.As(err, &killed) || killed.ProcessState.ExitCode() != -1 || !bytes.Contains(output, []byte("transaction committed before kill")) {
		t.Fatalf("did not kill committed worker: %v %s", err, output)
	}
	server := nativeRevisionService(t, fixture.store)
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	request := idempotentMergeRequest(t, fixture, id)
	request.Operations[0].ReturnDocument = false
	mutation := queue.Mutation{Write: request.Operations[0]}
	encoded, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := queue.UnmarshalMutation(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Handle(t.Context(), replayed); err != nil {
		t.Fatal(err)
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 1 {
		t.Fatal("restart redelivery repeated committed effect")
	}
}

func TestIdempotentMongoCrashHelper(t *testing.T) {
	database := os.Getenv("SINK_IDEMPOTENCY_CRASH_DATABASE")
	if database == "" {
		return
	}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI))
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	storeOptions := mongodb.Options{Store: "primary"}
	backend, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &integrationFixture{client: client, store: backend, database: database, collection: client.Database(database).Collection("documents")}
	server := nativeRevisionService(t, backend)
	request := idempotentMergeRequest(t, fixture, os.Getenv("SINK_IDEMPOTENCY_CRASH_ID"))
	request.Operations[0].ReturnDocument = false
	response, err := server.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("child commit: %v %v", response, err)
	}
	fmt.Println("transaction committed before kill")
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotentMongoDoesNotAdoptExistingBusinessCollection(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	collection := fixture.client.Database(fixture.database).Collection("__sink_receipts_v1")
	document := bson.D{{Key: "_id", Value: "business-data"}, {Key: "expires_at", Value: time.Now().Add(-time.Hour)}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	server := nativeRevisionService(t, fixture.store)
	request := idempotentMergeRequest(t, fixture, testOperationID("collision", time.Now()))
	response, err := server.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
		t.Fatalf("adopted unowned collection: %v %v", response, err)
	}
	indexes, err := collection.Indexes().ListSpecifications(t.Context())
	if err != nil || len(indexes) != 1 || indexes[0].Name != "_id_" {
		t.Fatalf("installed TTL on business data: %v %v", indexes, err)
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 0 {
		t.Fatal("ownership failure mutated business record")
	}
}

type failedFirstReceiptStore struct {
	*mongodb.Store
	failed atomic.Bool
}

func (s *failedFirstReceiptStore) ExecuteOnce(ctx context.Context, req storage.IdempotentRequest) ([]byte, error) {
	if s.failed.CompareAndSwap(false, true) {
		return nil, storage.BackendError(errors.New("connection failed before transaction"))
	}
	return s.Store.ExecuteOnce(ctx, req)
}

func TestIdempotentMongoRetryPreservesSameRecordOrder(t *testing.T) {
	fixture := newIntegrationFixture(t)
	nativeRevisionSeed(t, fixture, "quota")
	backend := &failedFirstReceiptStore{Store: fixture.store}
	server := nativeRevisionService(t, backend)
	request := idempotentMergeRequest(t, fixture, testOperationID("first", time.Now()))
	second := proto.Clone(request.Operations[0]).(*sink.WriteOperation)
	second.OperationId = testOperationID("second", time.Now())
	value := bson.D{{Key: "count", Value: int64(100)}}
	encoded := bsonStorageDocument(t, value)
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: encoded.Payload}
	put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: document}
	action := &sink.WriteOperation_Put{Put: put}
	second.Action = action
	request.Operations = append(request.Operations, second)
	response, err := server.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_FAILED || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_FAILED {
		t.Fatalf("later operation passed unresolved predecessor: %v %v", response, err)
	}
	stored := nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 0 {
		t.Fatal("failed predecessor did not block later write")
	}
	response, err = server.WriteIdempotent(t.Context(), request)
	if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("retry failed: %v %v", response, err)
	}
	stored = nativeRevisionRead(t, fixture, "quota")
	if bson.Raw(stored.Document.Payload).Lookup("count").AsInt64() != 100 {
		t.Fatal("retry reordered logical operations")
	}
}
