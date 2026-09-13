package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readCapacityStorage struct {
	storage.Storage
	maximum int
	reads   atomic.Int64
	active  atomic.Int64
	peak    atomic.Int64
	started chan struct{}
	gate    chan struct{}
	failAt  int64
}

func (s *readCapacityStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	var empty storage.ReadResponse
	call := s.reads.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); active > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, active) {
			break
		}
	}
	if s.started != nil {
		s.started <- struct{}{}
	}
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return empty, ctx.Err()
		}
	}
	if call == s.failAt {
		return empty, errors.New("injected backend failure")
	}
	response, err := s.Storage.Read(ctx, req)
	bytes := 0
	for _, result := range response.Results {
		if result.Status == storage.ReadStatusFound {
			bytes += len(result.Document.Payload) + 128
		}
	}
	if s.maximum > 0 && bytes > s.maximum {
		return empty, fmt.Errorf("retained snapshots exceed %d bytes: %d", s.maximum, bytes)
	}
	return response, err
}

func readCapacityCall(ctx context.Context, keys ...string) *batchCall[*sink.ReadRequest, *sink.ReadResponse] {
	request := &sink.ReadRequest{}
	for _, key := range keys {
		operation := &sink.ReadOperation{Address: completionAddress(key)}
		request.Operations = append(request.Operations, operation)
	}
	call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: ctx, request: request,
		operationCount: len(keys), result: make(chan batchResult[*sink.ReadResponse], 1)}
	return call
}

func seedReadCapacity(t testing.TB, backend *memory.Store, key string, size int) {
	t.Helper()
	address, err := convertAddress(completionAddress(key))
	if err != nil {
		t.Fatal(err)
	}
	document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(fmt.Sprintf(`{"value":"%s"}`, strings.Repeat("x", size)))}
	seed := memory.SeedRequest{Address: address, Document: document}
	backend.Seed(seed)
}

func TestReadMicrobatchSharesBoundedWorkingSet(t *testing.T) {
	memoryStore := memory.New()
	backend := &readCapacityStorage{Storage: memoryStore, maximum: storage.DefaultMaxReadBytes}
	server := completionServer(t, backend)
	calls := make([]*batchCall[*sink.ReadRequest, *sink.ReadResponse], 128)
	for index := range calls {
		key := fmt.Sprintf("record-%d", index)
		seedReadCapacity(t, memoryStore, key, 1024)
		calls[index] = readCapacityCall(t.Context(), key)
	}
	server.executeReads(t.Context(), calls)
	for _, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil || result.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
			t.Fatalf("read: %v, %v", result.response, result.err)
		}
	}
	if backend.reads.Load() != 1 {
		t.Fatalf("128 small RPCs used %d backend reads, want 1", backend.reads.Load())
	}
	if server.server.inFlightBytes != 0 || server.server.inFlightRequests != 0 {
		t.Fatal("read admission leaked")
	}
}

func TestReadMicrobatchBoundsLargeSnapshotsAndRepeatedOutputs(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			memoryStore := memory.New()
			backend := &readCapacityStorage{Storage: memoryStore, maximum: 512}
			server := completionServer(t, backend)
			server.server.maxReadBytes = 512
			calls := make([]*batchCall[*sink.ReadRequest, *sink.ReadResponse], 12)
			for index := range calls {
				key := fmt.Sprintf("record-%d", index)
				if shared {
					key = "shared"
				}
				seedReadCapacity(t, memoryStore, key, 300)
				calls[index] = readCapacityCall(t.Context(), key)
			}
			server.executeReads(t.Context(), calls)
			responses := make([]*sink.ReadResponse, 0, len(calls))
			for _, call := range calls {
				result := awaitCompletion(t, call.result)
				if result.err != nil || result.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
					t.Fatalf("read: %v, %v", result.response, result.err)
				}
				responses = append(responses, result.response)
			}
			if backend.reads.Load() != int64(len(calls)) {
				t.Fatalf("unexpected reads for large records: %d", backend.reads.Load())
			}
			responses[0].Results[0].Document.Payload[0] = '!'
			if responses[1].Results[0].Document.Payload[0] != '{' {
				t.Fatal("RPC responses share mutable payloads")
			}
		})
	}
}

func TestReadMicrobatchRetainsCallerLimitsAndOrderAcrossShrinking(t *testing.T) {
	memoryStore := memory.New()
	for _, key := range []string{"a", "b", "c"} {
		seedReadCapacity(t, memoryStore, key, 100)
	}
	seedReadCapacity(t, memoryStore, "oversized", 700)
	backend := &readCapacityStorage{Storage: memoryStore, maximum: 512}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 512
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{
		readCapacityCall(t.Context(), "a", "b", "a"),
		readCapacityCall(t.Context(), "c", "missing", "c"),
		readCapacityCall(t.Context(), "oversized"),
		readCapacityCall(t.Context(), "b"),
	}
	server.executeReads(t.Context(), calls)
	for caller, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Fatal(result.err)
		}
		for index, operation := range result.response.Results {
			if operation.OperationIndex != uint32(index) {
				t.Fatalf("wrong result order: %v", result.response)
			}
			if (caller == 0 && index == 2) || caller == 2 {
				if operation.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
					t.Fatal(operation)
				}
			} else if caller == 1 && index == 1 {
				if operation.Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
					t.Fatal(operation)
				}
			} else if operation.Status != sink.ReadStatus_READ_STATUS_FOUND {
				t.Fatal(operation)
			}
		}
	}
}

func TestReadMicrobatchKeepsCompletedCallWhenLaterReadFails(t *testing.T) {
	memoryStore := memory.New()
	for _, key := range []string{"a", "b"} {
		seedReadCapacity(t, memoryStore, key, 300)
	}
	backend := &readCapacityStorage{Storage: memoryStore, maximum: 512, failAt: 2}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 512
	first := readCapacityCall(t.Context(), "a")
	second := readCapacityCall(t.Context(), "b")
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{first, second}
	server.executeReads(t.Context(), calls)
	completed := awaitCompletion(t, first.result)
	failed := awaitCompletion(t, second.result)
	if completed.err != nil || completed.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatal(completed)
	}
	if status.Code(failed.err) != codes.Unavailable {
		t.Fatal(failed.err)
	}
}

func TestReadBatchConcurrencyUsesStoreLimit(t *testing.T) {
	backend := &readCapacityStorage{Storage: memory.New(), started: make(chan struct{}, 3), gate: make(chan struct{})}
	core := completionServer(t, backend).server
	core.maxStoreRequests = 2
	opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 1, MaxWait: time.Millisecond}
	server, err := NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer close(backend.gate)
	results := make(chan error, 3)
	for index := range 3 {
		call := readCapacityCall(t.Context(), fmt.Sprint(index))
		go func() { _, err := server.Read(t.Context(), call.request); results <- err }()
		if index < 2 {
			awaitCompletion(t, backend.started)
		}
	}
	waitForQueuedCalls(t, server.reads["primary"], 1)
	if backend.peak.Load() != 2 {
		t.Fatalf("read concurrency = %d", backend.peak.Load())
	}
	select {
	case <-backend.started:
		t.Fatal("third read exceeded the store limit")
	default:
	}
	// Canceling all RPCs is separately covered; here release the I/O gate and
	// verify queued requests can use the newly available capacity.
	backend.gate <- struct{}{}
	if err := awaitCompletion(t, results); err != nil {
		t.Fatal(err)
	}
	awaitCompletion(t, backend.started)
	backend.gate <- struct{}{}
	backend.gate <- struct{}{}
	for range 2 {
		if err := awaitCompletion(t, results); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadBatchCancellationKeepsOtherCallerAlive(t *testing.T) {
	backend := &readCapacityStorage{Storage: memory.New(), started: make(chan struct{}, 1), gate: make(chan struct{})}
	core := completionServer(t, backend).server
	opts := BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 2, MaxWait: time.Second}
	server, err := NewBatchingServer(core, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer close(backend.gate)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := readCapacityCall(ctx, "first")
	second := readCapacityCall(t.Context(), "second")
	canceled, healthy := make(chan error, 1), make(chan error, 1)
	go func() { _, err := server.Read(ctx, first.request); canceled <- err }()
	waitForQueuedCalls(t, server.reads["primary"], 1)
	go func() { _, err := server.Read(t.Context(), second.request); healthy <- err }()
	awaitCompletion(t, backend.started)
	cancel()
	if err := awaitCompletion(t, canceled); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	backend.gate <- struct{}{}
	if err := awaitCompletion(t, healthy); err != nil {
		t.Fatalf("healthy caller canceled: %v", err)
	}
}
