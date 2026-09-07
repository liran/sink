package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestRegressionAppliedRequestBehindRunningVisibleBatch(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10), blocked: "product", release: make(chan struct{})}
	core := completionServer(t, backend).server
	opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 1, MaxWait: time.Millisecond}
	server, err := NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer close(backend.release)
	visibleCall := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionPut("product", 1))
	visibleDone := make(chan error, 1)
	go func() {
		_, writeErr := server.Write(t.Context(), visibleCall.request)
		visibleDone <- writeErr
	}()
	awaitCompletion(t, backend.events)
	// The core has capacity and the archive backend has no refresh wait.
	control := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("control", 1))
	controlResponse, err := core.Write(t.Context(), control.request)
	if err != nil || controlResponse.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("unbatched control failed: %v, %v", controlResponse, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	applied := completionWriteCall(ctx, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("archive", 1))
	response, err := server.Write(ctx, applied.request)
	if err != nil {
		t.Fatalf("independent applied RPC timed out behind running visible batch despite free capacity: %v", err)
	}
	if response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatal(response)
	}
}

func TestRegressionMicrobatchMergeBudgetIsolation(t *testing.T) {
	backend := memory.New()
	server := completionServer(t, backend)
	server.server.maxReadBytes = 256
	makeCall := func(key string) *batchCall[*sink.WriteRequest, *sink.WriteResponse] {
		op := completionMerge(key, 1)
		op.GetMerge().LuaProgram.Source = []byte(`return function(current, incoming) return incoming end`)
		op.GetMerge().IncomingDocument.Payload = []byte(fmt.Sprintf(`{"value":1,"padding":"%s"}`, strings.Repeat("x", 80)))
		return completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, op)
	}
	for _, key := range []string{"control-a", "control-b"} {
		call := makeCall(key)
		response, err := server.server.Write(t.Context(), call.request)
		if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("single-RPC control: %v, %v", response, err)
		}
	}
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{makeCall("a"), makeCall("b")}
	server.executeWrites(t.Context(), calls)
	for index, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Errorf("RPC %d: %v", index, result.err)
			continue
		}
		if result.response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Errorf("individually valid merge RPC %d rejected after coalescing: %v", index, result.response)
		}
	}
}

func TestRegressionMicrobatchReadBudgetIsolation(t *testing.T) {
	backend := memory.New()
	server := completionServer(t, backend)
	server.server.maxReadBytes = 256
	var calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse]
	for _, key := range []string{"a", "b"} {
		address, err := convertAddress(completionAddress(key))
		if err != nil {
			t.Fatal(err)
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(fmt.Sprintf(`{"value":"%s"}`, strings.Repeat("x", 80)))}
		seed := memory.SeedRequest{Address: address, Document: document}
		backend.Seed(seed)
		op := &sink.ReadOperation{Address: completionAddress(key)}
		req := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
		response, readErr := server.server.Read(t.Context(), req)
		if readErr != nil || response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
			t.Fatalf("single-RPC control: %v, %v", response, readErr)
		}
		call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: req, operationCount: 1, result: make(chan batchResult[*sink.ReadResponse], 1)}
		calls = append(calls, call)
	}
	server.executeReads(t.Context(), calls)
	for index, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Errorf("RPC %d: %v", index, result.err)
			continue
		}
		if result.response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
			t.Errorf("individually valid read RPC %d rejected after coalescing: %v", index, result.response)
		}
	}
}

func TestMicrobatchReadKeepsOversizedCallerIsolatedForSharedKey(t *testing.T) {
	backend := memory.New()
	server := completionServer(t, backend)
	server.server.maxReadBytes = 256
	var operations []*sink.ReadOperation
	for _, key := range []string{"a", "b"} {
		address, err := convertAddress(completionAddress(key))
		if err != nil {
			t.Fatal(err)
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(fmt.Sprintf(`{"value":"%s"}`, strings.Repeat("x", 80)))}
		seed := memory.SeedRequest{Address: address, Document: document}
		backend.Seed(seed)
		operation := &sink.ReadOperation{Address: completionAddress(key)}
		operations = append(operations, operation)
	}
	firstRequest := &sink.ReadRequest{Operations: operations}
	secondRequest := &sink.ReadRequest{Operations: operations[1:]}
	first := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: firstRequest, operationCount: 2, result: make(chan batchResult[*sink.ReadResponse], 1)}
	second := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: secondRequest, operationCount: 1, result: make(chan batchResult[*sink.ReadResponse], 1)}
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{first, second}
	server.executeReads(t.Context(), calls)
	oversized := awaitCompletion(t, first.result)
	healthy := awaitCompletion(t, second.result)
	if oversized.err != nil || healthy.err != nil {
		t.Fatalf("read errors: %v, %v", oversized.err, healthy.err)
	}
	if oversized.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND || oversized.response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatal(oversized.response)
	}
	if healthy.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatal(healthy.response)
	}
}

func TestMicrobatchMergeBudgetFailureDoesNotLeakIntoNextCaller(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			backend := memory.New()
			server := completionServer(t, backend)
			server.server.maxReadBytes = 150
			if present {
				for _, key := range []string{"a", "b"} {
					address, err := convertAddress(completionAddress(key))
					if err != nil {
						t.Fatal(err)
					}
					document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"value":0}`)}
					seed := memory.SeedRequest{Address: address, Document: document}
					backend.Seed(seed)
				}
			}
			first := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge("a", 10), completionMerge("b", 100))
			second := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge("b", 1))
			calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second}
			server.executeWrites(t.Context(), calls)
			oversized := awaitCompletion(t, first.result)
			healthy := awaitCompletion(t, second.result)
			if oversized.err != nil || healthy.err != nil {
				t.Fatalf("write errors: %v, %v", oversized.err, healthy.err)
			}
			if oversized.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || oversized.response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
				t.Fatal(oversized.response)
			}
			if healthy.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatal(healthy.response)
			}
			address, err := convertAddress(completionAddress("b"))
			if err != nil {
				t.Fatal(err)
			}
			operation := storage.ReadOperation{Address: address}
			read := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
			stored, err := backend.Read(t.Context(), read)
			if err != nil {
				t.Fatal(err)
			}
			if string(stored.Results[0].Document.Payload) != `{"value":1}` {
				t.Fatalf("failed caller changed the folded state: %s", stored.Results[0].Document.Payload)
			}
		})
	}
}

func TestMicrobatchBudgetsSplitWithinExecutionMemoryLimit(t *testing.T) {
	for _, method := range []string{"Read", "Write"} {
		t.Run(method, func(t *testing.T) {
			backend := memory.New()
			server := completionServer(t, backend)
			server.server.maxReadBytes = 256
			server.server.maxInFlightBytes = 1000
			switch method {
			case "Write":
				var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
				for _, key := range []string{"a", "b", "c"} {
					calls = append(calls, completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge(key, 1)))
				}
				server.executeWrites(t.Context(), calls)
				assertCompletionWrites(t, calls)
			case "Read":
				var calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse]
				for _, key := range []string{"a", "b", "c"} {
					operation := &sink.ReadOperation{Address: completionAddress(key)}
					request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
					call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: request, operationCount: 1, result: make(chan batchResult[*sink.ReadResponse], 1)}
					calls = append(calls, call)
				}
				server.executeReads(t.Context(), calls)
				for _, call := range calls {
					result := awaitCompletion(t, call.result)
					if result.err != nil || result.response.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
						t.Fatalf("split read: %+v", result)
					}
				}
			}
			if server.server.inFlightBytes != 0 || server.server.inFlightRequests != 0 {
				t.Fatal("execution reservation leaked")
			}
		})
	}
}

func TestMutationDispatcherPreservesDependenciesAcrossBatches(t *testing.T) {
	cases := []struct {
		method string
		cancel bool
	}{{"Write", false}, {"Write", true}, {"Delete", false}, {"Delete", true}}
	for _, test := range cases {
		t.Run(fmt.Sprintf("%s/cancel=%v", test.method, test.cancel), func(t *testing.T) {
			method := test.method
			backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 16), blocked: "product", release: make(chan struct{})}
			core := completionServer(t, backend).server
			core.maxStoreRequests = 2
			core.maxInFlightRequests = 2
			opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 1, MaxWait: time.Millisecond}
			server, err := NewBatchingServer(core, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			defer func() {
				select {
				case <-backend.release:
				default:
					close(backend.release)
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			submit := func(mode sink.CompletionMode, keys ...string) <-chan error {
				done := make(chan error, 1)
				go func() {
					if method == "Write" {
						request := &sink.WriteRequest{CompletionMode: mode}
						for _, key := range keys {
							request.Operations = append(request.Operations, completionPut(key, 1))
						}
						_, callErr := server.Write(ctx, request)
						done <- callErr
					} else {
						request := &sink.DeleteRequest{CompletionMode: mode}
						for _, key := range keys {
							operation := &sink.DeleteOperation{Address: completionAddress(key)}
							request.Operations = append(request.Operations, operation)
						}
						_, callErr := server.Delete(ctx, request)
						done <- callErr
					}
				}()
				return done
			}
			visible := submit(sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, "product")
			awaitCompletion(t, backend.events)
			bridge := submit(sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "product", "dependent")
			if method == "Write" {
				waitForQueuedCalls(t, server.writes["primary"], 1)
			} else {
				waitForQueuedCalls(t, server.deletes["primary"], 1)
			}
			dependent := submit(sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "dependent")
			if method == "Write" {
				waitForQueuedCalls(t, server.writes["primary"], 2)
			} else {
				waitForQueuedCalls(t, server.deletes["primary"], 2)
			}
			independent := submit(sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "archive")
			event := awaitCompletion(t, backend.events)
			if len(event.keys) != 1 || event.keys[0] != "archive" {
				t.Fatalf("dependency was bypassed: %+v", event)
			}
			if err := awaitCompletion(t, independent); err != nil {
				t.Fatal(err)
			}
			select {
			case event := <-backend.events:
				t.Fatalf("blocked dependent reached storage: %+v", event)
			case <-time.After(20 * time.Millisecond):
			}
			if !test.cancel {
				close(backend.release)
				for _, done := range []<-chan error{visible, bridge, dependent} {
					if err := awaitCompletion(t, done); err != nil {
						t.Fatal(err)
					}
				}
				first := awaitCompletion(t, backend.events)
				second := awaitCompletion(t, backend.events)
				if len(first.keys) != 2 || first.keys[0] != "product" || first.keys[1] != "dependent" || len(second.keys) != 1 || second.keys[0] != "dependent" {
					t.Fatalf("dependency order: %+v, %+v", first, second)
				}
			}
			// Cancellation and Close must drain both blocked dependency chains and the
			// active refresh wait without retaining queue/admission reservations.
			cancel()
			server.Close()
			if core.inFlightBytes != 0 || core.inFlightRequests != 0 {
				t.Fatal("shutdown leaked execution capacity")
			}
		})
	}
}

func TestMicrobatchConditionalWritesKeepEachCallersInputAndOutputBudget(t *testing.T) {
	for _, kind := range []string{"Merge", "Replace"} {
		t.Run(kind, func(t *testing.T) {
			backend := memory.New()
			server := completionServer(t, backend)
			server.server.maxReadBytes = 256
			var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
			for _, key := range []string{"a", "b"} {
				address, err := convertAddress(completionAddress(key))
				if err != nil {
					t.Fatal(err)
				}
				payload := []byte(fmt.Sprintf(`{"value":0,"padding":"%s"}`, strings.Repeat("x", 80)))
				document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: payload}
				seed := memory.SeedRequest{Address: address, Document: document}
				backend.Seed(seed)
				var operations []*sink.WriteOperation
				if kind == "Merge" {
					operation := completionMerge(key, 1)
					operation.GetMerge().LuaProgram.Source = []byte(`return function(current, incoming) current.value=current.value+incoming.value return current end`)
					operations = append(operations, operation)
				} else {
					for range 2 {
						operation := completionPut(key, 1)
						operation.GetPut().Mode = sink.WriteMode_WRITE_MODE_REPLACE
						operation.GetPut().Document.Payload = payload
						operations = append(operations, operation)
					}
				}
				call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operations...)
				calls = append(calls, call)
			}
			server.executeWrites(t.Context(), calls)
			assertCompletionWrites(t, calls)
		})
	}
}

func TestCancelledRPCIsOmittedFromLaterMemoryLimitedSegment(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 8), blocked: "a", release: make(chan struct{})}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 256
	server.server.maxInFlightBytes = 1000
	cancelled, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer func() {
		select {
		case <-backend.release:
		default:
			close(backend.release)
		}
	}()
	first := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionMerge("a", 1))
	second := completionWriteCall(cancelled, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionMerge("b", 1))
	third := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionMerge("c", 1))
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second, third}
	done := make(chan struct{})
	go func() { server.executeWrites(t.Context(), calls); close(done) }()
	event := awaitCompletion(t, backend.events)
	if len(event.keys) != 1 || event.keys[0] != "a" {
		t.Fatal(event)
	}
	cancel()
	close(backend.release)
	awaitCompletion(t, done)
	for _, call := range []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, third} {
		result := awaitCompletion(t, call.result)
		if result.err != nil || result.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("live segment: %+v", result)
		}
	}
	result := awaitCompletion(t, second.result)
	if result.err == nil {
		t.Fatal("cancelled segment was executed")
	}
	event = awaitCompletion(t, backend.events)
	if len(event.keys) != 1 || event.keys[0] != "c" {
		t.Fatalf("cancelled write reached backend: %+v", event)
	}
	select {
	case extra := <-backend.events:
		t.Fatalf("unexpected write: %+v", extra)
	default:
	}
}

func TestHotRecordSharesPhysicalReservationAcrossRPCBudgets(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 8)}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 256
	server.server.maxInFlightBytes = 2048
	var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
	for range 4 {
		calls = append(calls, completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge("hot", 1)))
	}
	server.executeWrites(t.Context(), calls)
	assertCompletionWrites(t, calls)
	event := awaitCompletion(t, backend.events)
	if len(event.keys) != 1 || event.keys[0] != "hot" {
		t.Fatal(event)
	}
	select {
	case extra := <-backend.events:
		t.Fatalf("shared physical state was reserved once per caller and split: %+v", extra)
	default:
	}
}
