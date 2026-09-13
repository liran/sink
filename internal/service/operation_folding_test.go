package service_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func foldingPut(key string, mode sink.WriteMode, value int) *sink.WriteOperation {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: fmt.Appendf(nil, `{"value":%d}`, value)}
	put := &sink.PutOperation{Mode: mode, Document: document}
	action := &sink.WriteOperation_Put{Put: put}
	operation := &sink.WriteOperation{Address: protoAddress(key), Action: action}
	return operation
}

func TestPutFoldingMatchesSequentialPreconditions(t *testing.T) {
	// Exhaust every five-operation sequence of Create/Replace/Upsert, with
	// and without an existing record, against individually committed puts.
	modes := []sink.WriteMode{sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_REPLACE, sink.WriteMode_WRITE_MODE_UPSERT}
	for _, exists := range []bool{false, true} {
		for sequence := range 243 {
			backend := memory.New()
			control := memory.New()
			if exists {
				seed := memory.SeedRequest{Address: storageAddress("key"), Document: storageJSONDocument(`{"value":0}`)}
				backend.Seed(seed)
				control.Seed(seed)
			}
			observed := &countingStorage{backend: backend}
			folded := newTestServer(t, observed, nil)
			sequential := newTestServer(t, control, nil)
			var operations []*sink.WriteOperation
			var expected []*sink.WriteResult
			remaining := sequence
			for index := range 5 {
				operation := foldingPut("key", modes[remaining%3], index+1)
				remaining /= 3
				operations = append(operations, operation)
				request := foldingRequest(operation)
				response, err := sequential.Write(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				expected = append(expected, response.Results[0])
			}
			request := foldingRequest(operations...)
			response, err := folded.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			var revision []byte
			for index, result := range response.Results {
				want := expected[index]
				if result.OperationIndex != uint32(index) || result.Status != want.Status || result.GetFailure().GetCode() != want.GetFailure().GetCode() {
					t.Fatalf("exists=%t sequence=%d operation=%d: got %v want %v", exists, sequence, index, result, want)
				}
				if result.Status == sink.WriteStatus_WRITE_STATUS_APPLIED {
					if revision == nil {
						revision = result.GetRevision().GetData()
					}
					if len(revision) == 0 || !bytes.Equal(revision, result.GetRevision().GetData()) {
						t.Fatal("puts did not share a committed revision")
					}
				}
			}
			read := storage.ReadOperation{Address: storageAddress("key")}
			readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
			got, _ := backend.Read(t.Context(), readRequest)
			want, _ := control.Read(t.Context(), readRequest)
			if got.Results[0].Status != want.Results[0].Status || !bytes.Equal(got.Results[0].Document.Payload, want.Results[0].Document.Payload) {
				t.Fatalf("exists=%t sequence=%d: final state differs", exists, sequence)
			}
			if observed.writeCalls.Load() > 1 || observed.readCalls.Load() > 1 || observed.maxWriteOperations.Load() > 1 {
				t.Fatalf("sequence %d was not folded: %+v", sequence, observed)
			}
		}
	}
}

func TestUpsertFoldingAvoidsReadsAndSharesRevision(t *testing.T) {
	observed := &countingStorage{backend: memory.New()}
	server := newTestServer(t, observed, nil)
	var operations []*sink.WriteOperation
	for index := range 64 {
		operations = append(operations, foldingPut("hot", sink.WriteMode_WRITE_MODE_UPSERT, index))
	}
	request := foldingRequest(operations...)
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatal(result)
		}
	}
	if observed.readCalls.Load() != 0 || observed.writeCalls.Load() != 1 || observed.maxWriteOperations.Load() != 1 || foldingValue(t, observed.backend, "hot") != 63 {
		t.Fatal("upserts did not reduce to one direct backend write")
	}
}

func TestMixedWriteFoldingReevaluatesPutFailuresAfterConflict(t *testing.T) {
	backend := memory.New()
	observed := &foldingFaultStorage{Storage: backend, backend: backend, fault: "conflict"}
	server := newTestServer(t, observed, nil)
	create := foldingPut("counter", sink.WriteMode_WRITE_MODE_CREATE, 1)
	add := foldingMerge("counter", incrementLua, `{"value":2}`)
	request := foldingRequest(create, add)
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("stale speculative create result: %v", response)
	}
	if observed.writes != 2 || observed.reads != 2 || foldingValue(t, backend, "counter") != 12 {
		t.Fatal("conflict did not rebase the entire Put/Merge chain")
	}
}

func TestMixedWriteFoldingRejectsWholeUncommittedChain(t *testing.T) {
	for _, fault := range []string{"reject", "lost acknowledgement"} {
		t.Run(fault, func(t *testing.T) {
			backend := memory.New()
			observed := &foldingFaultStorage{Storage: backend, backend: backend, fault: fault}
			server := newTestServer(t, observed, nil)
			first := foldingPut("counter", sink.WriteMode_WRITE_MODE_CREATE, 1)
			duplicate := foldingPut("counter", sink.WriteMode_WRITE_MODE_CREATE, 2)
			add := foldingMerge("counter", incrementLua, `{"value":3}`)
			request := foldingRequest(first, duplicate, add)
			response, err := server.Write(t.Context(), request)
			if err == nil {
				for _, result := range response.Results {
					if result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_UNAVAILABLE {
						t.Fatalf("uncommitted result acknowledged or speculative failure finalized: %v", result)
					}
				}
			}
			if observed.writes != 1 {
				t.Fatalf("ambiguous commit replayed %d times", observed.writes)
			}
		})
	}
}

func TestRepeatedReadAndDeleteUseOneBackendOperation(t *testing.T) {
	backend := memory.New()
	seed := memory.SeedRequest{Address: storageAddress("hot"), Document: storageJSONDocument(`{"value":7}`)}
	backend.Seed(seed)
	observed := &countingStorage{backend: backend}
	server := newTestServer(t, observed, nil)
	read := &sink.ReadRequest{}
	remove := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	for range 64 {
		readOperation := &sink.ReadOperation{Address: protoAddress("hot")}
		deleteOperation := &sink.DeleteOperation{Address: protoAddress("hot")}
		read.Operations = append(read.Operations, readOperation)
		remove.Operations = append(remove.Operations, deleteOperation)
	}
	response, err := server.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.OperationIndex != uint32(index) || result.Status != sink.ReadStatus_READ_STATUS_FOUND || !bytes.Equal(result.GetDocument().GetPayload(), seed.Document.Payload) {
			t.Fatal(result)
		}
	}
	response.Results[0].Document.Payload[0] = 'x'
	if response.Results[1].Document.Payload[0] == 'x' {
		t.Fatal("read result payloads alias across operations")
	}
	deleted, err := server.Delete(t.Context(), remove)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range deleted.Results {
		if result.OperationIndex != uint32(index) || result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatal(result)
		}
	}
	if observed.readCalls.Load() != 1 || observed.maxReadOperations.Load() != 1 || observed.deleteCalls.Load() != 1 || observed.maxDeleteOperations.Load() != 1 || !observed.deleteWaitVisible.Load() {
		t.Fatal("repeated reads/deletes were not deduplicated")
	}
}

func TestReadFoldingRetainsResponseBudgetAndOriginalOrder(t *testing.T) {
	backend := memory.New()
	for _, key := range []string{"a", "b"} {
		seed := memory.SeedRequest{Address: storageAddress(key), Document: storageJSONDocument(`{"value":7}`)}
		backend.Seed(seed)
	}
	observed := &countingStorage{backend: backend}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: observed, Lua: lua, MaxReadBytes: 280}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := &sink.ReadRequest{}
	for _, key := range []string{"a", "b", "a"} {
		operation := &sink.ReadOperation{Address: protoAddress(key)}
		request.Operations = append(request.Operations, operation)
	}
	response, err := server.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[1].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[2].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || !response.Results[2].GetFailure().GetRetryable() {
		t.Fatalf("duplicates bypassed budget or reordered results: %v", response)
	}
	if observed.maxReadOperations.Load() != 2 {
		t.Fatal("duplicate was read from backend again")
	}
}

func TestMicrobatchFoldsPutsReadsAndDeletesAcrossRPCs(t *testing.T) {
	for _, method := range []string{"Write", "Read", "Delete"} {
		t.Run(method, func(t *testing.T) {
			observed := &countingStorage{backend: memory.New()}
			core := newTestServer(t, observed, nil)
			opts := service.BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 16, MaxWait: time.Second}
			server, err := service.NewBatchingServer(core, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			errors := make(chan error, 16)
			for index := range 16 {
				go func() {
					var callErr error
					switch method {
					case "Write":
						operation := foldingPut("hot", sink.WriteMode_WRITE_MODE_UPSERT, index)
						request := foldingRequest(operation)
						response, err := server.Write(ctx, request)
						callErr = err
						if err == nil && (len(response.Results) != 1 || response.Results[0].OperationIndex != 0 || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED) {
							callErr = fmt.Errorf("write response: %v", response)
						}
					case "Read":
						operation := &sink.ReadOperation{Address: protoAddress("hot")}
						request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
						response, err := server.Read(ctx, request)
						callErr = err
						if err == nil && (len(response.Results) != 1 || response.Results[0].OperationIndex != 0 || response.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND) {
							callErr = fmt.Errorf("read response: %v", response)
						}
					case "Delete":
						operation := &sink.DeleteOperation{Address: protoAddress("hot")}
						request := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{operation}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
						response, err := server.Delete(ctx, request)
						callErr = err
						if err == nil && (len(response.Results) != 1 || response.Results[0].OperationIndex != 0 || response.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED) {
							callErr = fmt.Errorf("delete response: %v", response)
						}
					}
					errors <- callErr
				}()
			}
			for range 16 {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			if observed.readCalls.Load()+observed.writeCalls.Load()+observed.deleteCalls.Load() != 1 || observed.maxReadOperations.Load()+observed.maxWriteOperations.Load()+observed.maxDeleteOperations.Load() != 1 {
				t.Fatalf("%s RPCs were not folded", method)
			}
		})
	}
}

func TestConditionalPutFoldingBoundsOutputAndConflicts(t *testing.T) {
	for _, scenario := range []string{"output", "conflict"} {
		t.Run(scenario, func(t *testing.T) {
			var backend storage.Storage = memory.New()
			if scenario == "conflict" {
				backend = conflictStorage{}
			}
			observed := &countingStorage{backend: backend}
			luaOptions := merge.LuaOptions{}
			lua, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				t.Fatal(err)
			}
			opts := service.Options{Storage: observed, Lua: lua, MaxMergeAttempts: 2, MaxReadBytes: 256}
			if scenario == "output" {
				opts.MaxReadBytes = 130
			}
			server, err := service.New(opts)
			if err != nil {
				t.Fatal(err)
			}
			first := foldingPut("hot", sink.WriteMode_WRITE_MODE_UPSERT, 1)
			second := foldingPut("hot", sink.WriteMode_WRITE_MODE_REPLACE, 2)
			request := foldingRequest(first, second)
			response, err := server.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			want := sink.FailureCode_FAILURE_CODE_CONFLICT
			writes := int64(2)
			if scenario == "output" {
				want = sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
				writes = 0
			}
			for _, result := range response.Results {
				if result.GetFailure().GetCode() != want || result.Status == sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatalf("unbounded conditional put: %v", result)
				}
			}
			if observed.writeCalls.Load() != writes {
				t.Fatalf("writes=%d want=%d", observed.writeCalls.Load(), writes)
			}
		})
	}
}

func TestReadAndDeleteFoldingKeepFullAddressesSeparate(t *testing.T) {
	for _, difference := range []string{"store", "namespace", "dataset", "key_type"} {
		t.Run(difference, func(t *testing.T) {
			observed := &countingStorage{backend: memory.New()}
			server := newTestServer(t, observed, nil)
			first := protoAddress("same")
			second := protoAddress("same")
			switch difference {
			case "store":
				second.Store = "secondary"
			case "namespace":
				second.Namespace = "another"
			case "dataset":
				second.Dataset = "another"
			case "key_type":
				kind := &sink.RecordKey_BytesValue{BytesValue: []byte("same")}
				second.Key.Kind = kind
			}
			read := &sink.ReadRequest{}
			remove := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
			for _, address := range []*sink.RecordAddress{first, second, first, second} {
				readOperation := &sink.ReadOperation{Address: address}
				deleteOperation := &sink.DeleteOperation{Address: address}
				read.Operations = append(read.Operations, readOperation)
				remove.Operations = append(remove.Operations, deleteOperation)
			}
			if _, err := server.Read(t.Context(), read); err != nil {
				t.Fatal(err)
			}
			if _, err := server.Delete(t.Context(), remove); err != nil {
				t.Fatal(err)
			}
			if observed.maxReadOperations.Load() != 2 || observed.maxDeleteOperations.Load() != 2 {
				t.Fatal("folding crossed a complete record address boundary")
			}
		})
	}
}

func TestAsyncPutAndDeletePreserveEveryOriginalOperation(t *testing.T) {
	publisher := &recordingPublisher{}
	observed := &countingStorage{backend: memory.New()}
	server := newTestServer(t, observed, publisher)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED}
	remove := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED}
	for index := range 4 {
		write.Operations = append(write.Operations, foldingPut("same", sink.WriteMode_WRITE_MODE_UPSERT, index))
		operation := &sink.DeleteOperation{Address: protoAddress("same")}
		remove.Operations = append(remove.Operations, operation)
	}
	if _, err := server.Write(t.Context(), write); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Delete(t.Context(), remove); err != nil {
		t.Fatal(err)
	}
	if publisher.mutationCount() != 8 || observed.writeCalls.Load() != 0 || observed.deleteCalls.Load() != 0 {
		t.Fatal("folding discarded asynchronously accepted intents")
	}
	for index := range 4 {
		mutation := publisher.mutation(index)
		if !bytes.Equal(mutation.Write.GetPut().GetDocument().GetPayload(), write.Operations[index].GetPut().GetDocument().GetPayload()) {
			t.Fatal("asynchronous put payload or order changed")
		}
	}
}
