package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type countingStorage struct {
	backend             storage.Storage
	readCalls           atomic.Int64
	writeCalls          atomic.Int64
	deleteCalls         atomic.Int64
	maxReadOperations   atomic.Int64
	maxWriteOperations  atomic.Int64
	maxDeleteOperations atomic.Int64
	writeWaitVisible    atomic.Bool
	deleteWaitVisible   atomic.Bool
}

type readBatchResult struct {
	request  int
	response *sink.ReadResponse
	err      error
}

type writeBatchResult struct {
	request  int
	response *sink.WriteResponse
	err      error
}

type deleteBatchResult struct {
	request  int
	response *sink.DeleteResponse
	err      error
}

type latencyStorage struct {
	backend     storage.Storage
	delay       time.Duration
	concurrency chan struct{}
}

func (s *latencyStorage) Ping(ctx context.Context) error {
	return s.backend.Ping(ctx)
}

func (s *latencyStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	select {
	case s.concurrency <- struct{}{}:
		defer func() { <-s.concurrency }()
	case <-ctx.Done():
		var response storage.ReadResponse
		return response, ctx.Err()
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return s.backend.Read(ctx, req)
	case <-ctx.Done():
		var response storage.ReadResponse
		return response, ctx.Err()
	}
}

func (s *latencyStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	return s.backend.Write(ctx, req)
}

func (s *latencyStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	return s.backend.Delete(ctx, req)
}

func (s *countingStorage) Ping(ctx context.Context) error {
	return s.backend.Ping(ctx)
}

func (s *countingStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.readCalls.Add(1)
	updateMaximum(&s.maxReadOperations, len(req.Operations))
	return s.backend.Read(ctx, req)
}

func (s *countingStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writeCalls.Add(1)
	updateMaximum(&s.maxWriteOperations, len(req.Operations))
	s.writeWaitVisible.Store(req.WaitUntilVisible)
	return s.backend.Write(ctx, req)
}

func (s *countingStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	s.deleteCalls.Add(1)
	updateMaximum(&s.maxDeleteOperations, len(req.Operations))
	s.deleteWaitVisible.Store(req.WaitUntilVisible)
	return s.backend.Delete(ctx, req)
}

func updateMaximum(maximum *atomic.Int64, value int) {
	wanted := int64(value)
	for {
		current := maximum.Load()
		if current >= wanted || maximum.CompareAndSwap(current, wanted) {
			return
		}
	}
}

func TestNewBatchingServerValidatesLimits(t *testing.T) {
	core := newTestServer(t, memory.New(), nil)
	var defaultOptions service.BatchingOptions
	valid, err := service.NewBatchingServer(core, defaultOptions)
	if err != nil {
		t.Fatalf("NewBatchingServer() error = %v", err)
	}
	valid.Close()

	tests := []struct {
		name    string
		server  *service.Server
		options service.BatchingOptions
	}{
		{
			name:   "server required",
			server: nil,
		},
		{
			name:   "batch operation limit exceeds service",
			server: core,
			options: service.BatchingOptions{
				MaxOperations: 1001,
			},
		},
		{
			name:   "queue cannot hold one service request",
			server: core,
			options: service.BatchingOptions{
				MaxQueuedOperations: 999,
			},
		},
		{
			name:   "byte queue is smaller than a batch",
			server: core,
			options: service.BatchingOptions{
				MaxBytes:       1024,
				MaxQueuedBytes: 512,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.NewBatchingServer(test.server, test.options)
			if err == nil {
				t.Fatal("NewBatchingServer() error = nil")
			}
		})
	}
}

func TestBatchingServerValidatesBeforeQueueing(t *testing.T) {
	server := newBatchingTestServer(t, memory.New(), nil, 1000)
	defer server.Close()

	_, err := server.Read(t.Context(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Read(nil) error = %v", err)
	}
	overLimitRead := &sink.ReadRequest{Operations: make([]*sink.ReadOperation, 1001)}
	_, err = server.Read(t.Context(), overLimitRead)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Read(over limit) error = %v", err)
	}

	invalidWrite := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_UNSPECIFIED,
		Operations:     []*sink.WriteOperation{putWriteOperation("write", "value")},
	}
	_, err = server.Write(t.Context(), invalidWrite)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write(invalid mode) error = %v", err)
	}
	invalidProgram := &sink.LuaProgram{Source: []byte("source"), Sha256: []byte("wrong")}
	invalidLuaWrite := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{putWriteOperation("write", "value")},
		LuaPrograms:    []*sink.LuaProgram{invalidProgram},
	}
	_, err = server.Write(t.Context(), invalidLuaWrite)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write(invalid Lua) error = %v", err)
	}

	invalidDelete := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_UNSPECIFIED,
		Operations: []*sink.DeleteOperation{
			{Address: protoAddress("delete")},
		},
	}
	_, err = server.Delete(t.Context(), invalidDelete)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Delete(invalid mode) error = %v", err)
	}
}

func TestBatchingServerAggregatesSingleOperationRPCs(t *testing.T) {
	const requestCount = 32

	t.Run("read", func(t *testing.T) {
		backend := memory.New()
		for index := range requestCount {
			seed := memory.SeedRequest{
				Address:  storageAddress(batchTestKey(index)),
				Document: storageJSONDocument(`{"value":"read"}`),
			}
			backend.Seed(seed)
		}
		observed := &countingStorage{backend: backend}
		server := newBatchingTestServer(t, observed, nil, requestCount)
		defer server.Close()

		var waitGroup sync.WaitGroup
		errors := make(chan error, requestCount)
		start := make(chan struct{})
		waitGroup.Add(requestCount)
		for index := range requestCount {
			go func() {
				defer waitGroup.Done()
				<-start
				response, err := server.Read(t.Context(), readRequest(batchTestKey(index)))
				if err == nil && response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
					err = errUnexpectedStatus(response.GetResults()[0].GetStatus())
				}
				errors <- err
			}()
		}
		close(start)
		waitGroup.Wait()
		close(errors)
		assertNoBatchErrors(t, errors)
		if observed.readCalls.Load() != 1 || observed.maxReadOperations.Load() != requestCount {
			t.Fatalf("storage reads = %d, max operations = %d", observed.readCalls.Load(), observed.maxReadOperations.Load())
		}
	})

	t.Run("write", func(t *testing.T) {
		observed := &countingStorage{backend: memory.New()}
		server := newBatchingTestServer(t, observed, nil, requestCount)
		defer server.Close()

		var waitGroup sync.WaitGroup
		errors := make(chan error, requestCount)
		start := make(chan struct{})
		waitGroup.Add(requestCount)
		for index := range requestCount {
			go func() {
				defer waitGroup.Done()
				<-start
				request := &sink.WriteRequest{
					CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
					Operations:     []*sink.WriteOperation{putWriteOperation(batchTestKey(index), "write")},
				}
				response, err := server.Write(t.Context(), request)
				if err == nil && response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
					err = errUnexpectedStatus(response.GetResults()[0].GetStatus())
				}
				errors <- err
			}()
		}
		close(start)
		waitGroup.Wait()
		close(errors)
		assertNoBatchErrors(t, errors)
		if observed.writeCalls.Load() != 1 || observed.maxWriteOperations.Load() != requestCount {
			t.Fatalf("storage writes = %d, max operations = %d", observed.writeCalls.Load(), observed.maxWriteOperations.Load())
		}
	})

	t.Run("delete", func(t *testing.T) {
		backend := memory.New()
		for index := range requestCount {
			seed := memory.SeedRequest{
				Address:  storageAddress(batchTestKey(index)),
				Document: storageJSONDocument(`{"value":"delete"}`),
			}
			backend.Seed(seed)
		}
		observed := &countingStorage{backend: backend}
		server := newBatchingTestServer(t, observed, nil, requestCount)
		defer server.Close()

		var waitGroup sync.WaitGroup
		errors := make(chan error, requestCount)
		start := make(chan struct{})
		waitGroup.Add(requestCount)
		for index := range requestCount {
			go func() {
				defer waitGroup.Done()
				<-start
				operation := &sink.DeleteOperation{Address: protoAddress(batchTestKey(index))}
				request := &sink.DeleteRequest{
					CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
					Operations:     []*sink.DeleteOperation{operation},
				}
				response, err := server.Delete(t.Context(), request)
				if err == nil && response.GetResults()[0].GetStatus() != sink.DeleteStatus_DELETE_STATUS_APPLIED {
					err = errUnexpectedStatus(response.GetResults()[0].GetStatus())
				}
				errors <- err
			}()
		}
		close(start)
		waitGroup.Wait()
		close(errors)
		assertNoBatchErrors(t, errors)
		if observed.deleteCalls.Load() != 1 || observed.maxDeleteOperations.Load() != requestCount {
			t.Fatalf("storage deletes = %d, max operations = %d", observed.deleteCalls.Load(), observed.maxDeleteOperations.Load())
		}
	})
}

func TestBatchingServerAggregatesAcrossGRPCRPCs(t *testing.T) {
	const requestCount = 32
	backend := memory.New()
	for index := range requestCount {
		seed := memory.SeedRequest{
			Address:  storageAddress(batchTestKey(index)),
			Document: storageJSONDocument(`{"value":"grpc"}`),
		}
		backend.Seed(seed)
	}
	observed := &countingStorage{backend: backend}
	server := newBatchingTestServer(t, observed, nil, requestCount)
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	sink.RegisterSinkServer(grpcServer, server)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	dialOption := grpc.WithContextDialer(dialer)
	credentialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	connection, err := grpc.NewClient("passthrough:///sink", dialOption, credentialOption)
	if err != nil {
		grpcServer.Stop()
		server.Close()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		server.Close()
		_ = listener.Close()
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	client := sink.NewSinkClient(connection)

	var waitGroup sync.WaitGroup
	errors := make(chan error, requestCount)
	start := make(chan struct{})
	waitGroup.Add(requestCount)
	for index := range requestCount {
		go func() {
			defer waitGroup.Done()
			<-start
			response, readErr := client.Read(t.Context(), readRequest(batchTestKey(index)))
			if readErr == nil && response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
				readErr = errUnexpectedStatus(response.GetResults()[0].GetStatus())
			}
			errors <- readErr
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errors)
	assertNoBatchErrors(t, errors)
	if observed.readCalls.Load() != 1 || observed.maxReadOperations.Load() != requestCount {
		t.Fatalf("storage reads = %d, max operations = %d", observed.readCalls.Load(), observed.maxReadOperations.Load())
	}
}

func TestBatchingServerPreservesMultiOperationResponseBoundaries(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		backend := memory.New()
		for index, value := range []string{"first", "second", "third"} {
			seed := memory.SeedRequest{
				Address:  storageAddress(batchTestKey(index)),
				Document: storageJSONDocument(`{"value":"` + value + `"}`),
			}
			backend.Seed(seed)
		}
		observed := &countingStorage{backend: backend}
		server := newBatchingTestServer(t, observed, nil, 3)
		defer server.Close()

		requests := []*sink.ReadRequest{
			{
				Operations: []*sink.ReadOperation{
					{Address: protoAddress(batchTestKey(0))},
					{Address: protoAddress(batchTestKey(1))},
				},
			},
			{
				Operations: []*sink.ReadOperation{
					{Address: protoAddress(batchTestKey(2))},
				},
			},
		}
		results := make(chan readBatchResult, len(requests))
		start := make(chan struct{})
		for requestIndex, request := range requests {
			go func() {
				<-start
				response, err := server.Read(t.Context(), request)
				result := readBatchResult{request: requestIndex, response: response, err: err}
				results <- result
			}()
		}
		close(start)

		seen := make(map[int]*sink.ReadResponse, len(requests))
		for range requests {
			result := <-results
			if result.err != nil {
				t.Fatalf("Read() error = %v", result.err)
			}
			seen[result.request] = result.response
		}
		if len(seen[0].GetResults()) != 2 || len(seen[1].GetResults()) != 1 {
			t.Fatalf("Read() response lengths = %d, %d", len(seen[0].GetResults()), len(seen[1].GetResults()))
		}
		for _, response := range seen {
			for index, result := range response.GetResults() {
				if result.GetOperationIndex() != uint32(index) {
					t.Fatalf("Read() operation index = %d, want %d", result.GetOperationIndex(), index)
				}
			}
		}
		if string(seen[0].GetResults()[0].GetDocument().GetJson()) != `{"value":"first"}` ||
			string(seen[0].GetResults()[1].GetDocument().GetJson()) != `{"value":"second"}` ||
			string(seen[1].GetResults()[0].GetDocument().GetJson()) != `{"value":"third"}` {
			t.Fatalf("Read() response mapping = %+v", seen)
		}
		if observed.readCalls.Load() != 1 || observed.maxReadOperations.Load() != 3 {
			t.Fatalf("storage reads = %d, max operations = %d", observed.readCalls.Load(), observed.maxReadOperations.Load())
		}
	})

	t.Run("write", func(t *testing.T) {
		observed := &countingStorage{backend: memory.New()}
		server := newBatchingTestServer(t, observed, nil, 3)
		defer server.Close()
		requests := []*sink.WriteRequest{
			{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations: []*sink.WriteOperation{
					putWriteOperation(batchTestKey(0), "first"),
					putWriteOperation(batchTestKey(1), "second"),
				},
			},
			{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations:     []*sink.WriteOperation{putWriteOperation(batchTestKey(2), "third")},
			},
		}
		results := make(chan writeBatchResult, len(requests))
		start := make(chan struct{})
		for requestIndex, request := range requests {
			go func() {
				<-start
				response, err := server.Write(t.Context(), request)
				result := writeBatchResult{request: requestIndex, response: response, err: err}
				results <- result
			}()
		}
		close(start)
		for range requests {
			result := <-results
			if result.err != nil {
				t.Fatalf("Write() error = %v", result.err)
			}
			if len(result.response.GetResults()) != len(requests[result.request].GetOperations()) {
				t.Fatalf("Write() result count = %d", len(result.response.GetResults()))
			}
			for index, operationResult := range result.response.GetResults() {
				if operationResult.GetOperationIndex() != uint32(index) {
					t.Fatalf("Write() operation index = %d, want %d", operationResult.GetOperationIndex(), index)
				}
			}
		}
		if observed.writeCalls.Load() != 1 || observed.maxWriteOperations.Load() != 3 {
			t.Fatalf("storage writes = %d, max operations = %d", observed.writeCalls.Load(), observed.maxWriteOperations.Load())
		}
	})

	t.Run("delete", func(t *testing.T) {
		observed := &countingStorage{backend: memory.New()}
		server := newBatchingTestServer(t, observed, nil, 3)
		defer server.Close()
		requests := []*sink.DeleteRequest{
			{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations: []*sink.DeleteOperation{
					{Address: protoAddress(batchTestKey(0))},
					{Address: protoAddress(batchTestKey(1))},
				},
			},
			{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations: []*sink.DeleteOperation{
					{Address: protoAddress(batchTestKey(2))},
				},
			},
		}
		results := make(chan deleteBatchResult, len(requests))
		start := make(chan struct{})
		for requestIndex, request := range requests {
			go func() {
				<-start
				response, err := server.Delete(t.Context(), request)
				result := deleteBatchResult{request: requestIndex, response: response, err: err}
				results <- result
			}()
		}
		close(start)
		for range requests {
			result := <-results
			if result.err != nil {
				t.Fatalf("Delete() error = %v", result.err)
			}
			if len(result.response.GetResults()) != len(requests[result.request].GetOperations()) {
				t.Fatalf("Delete() result count = %d", len(result.response.GetResults()))
			}
			for index, operationResult := range result.response.GetResults() {
				if operationResult.GetOperationIndex() != uint32(index) {
					t.Fatalf("Delete() operation index = %d, want %d", operationResult.GetOperationIndex(), index)
				}
			}
		}
		if observed.deleteCalls.Load() != 1 || observed.maxDeleteOperations.Load() != 3 {
			t.Fatalf("storage deletes = %d, max operations = %d", observed.deleteCalls.Load(), observed.maxDeleteOperations.Load())
		}
	})
}

func TestBatchingServerPromotesMixedCompletionModesToVisible(t *testing.T) {
	observed := &countingStorage{backend: memory.New()}
	server := newBatchingTestServer(t, observed, nil, 2)
	defer server.Close()

	requests := []*sink.WriteRequest{
		{
			CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
			Operations:     []*sink.WriteOperation{putWriteOperation("applied", "value")},
		},
		{
			CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
			Operations:     []*sink.WriteOperation{putWriteOperation("visible", "value")},
		},
	}
	errors := make(chan error, len(requests))
	start := make(chan struct{})
	for _, request := range requests {
		go func() {
			<-start
			response, err := server.Write(t.Context(), request)
			if err == nil && response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
				err = errUnexpectedStatus(response.GetResults()[0].GetStatus())
			}
			errors <- err
		}()
	}
	close(start)
	for range requests {
		if err := <-errors; err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if observed.writeCalls.Load() != 1 || !observed.writeWaitVisible.Load() {
		t.Fatalf("storage writes = %d, wait visible = %t", observed.writeCalls.Load(), observed.writeWaitVisible.Load())
	}
}

func TestBatchingServerBypassesAsynchronousMutations(t *testing.T) {
	publisher := &recordingPublisher{}
	observed := &countingStorage{backend: memory.New()}
	server := newLongWaitBatchingTestServer(t, observed, publisher)
	defer server.Close()

	writeRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.WriteOperation{putWriteOperation("write", "value")},
	}
	written, err := server.Write(t.Context(), writeRequest)
	if err != nil || written.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
		t.Fatalf("Write() response = %+v, error = %v", written, err)
	}
	deleteOperation := &sink.DeleteOperation{Address: protoAddress("delete")}
	deleteRequest := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.DeleteOperation{deleteOperation},
	}
	deleted, err := server.Delete(t.Context(), deleteRequest)
	if err != nil || deleted.GetResults()[0].GetStatus() != sink.DeleteStatus_DELETE_STATUS_ACCEPTED {
		t.Fatalf("Delete() response = %+v, error = %v", deleted, err)
	}
	if publisher.mutationCount() != 2 {
		t.Fatalf("published mutations = %d, want 2", publisher.mutationCount())
	}
	if observed.writeCalls.Load() != 0 || observed.deleteCalls.Load() != 0 {
		t.Fatalf("storage writes = %d, deletes = %d", observed.writeCalls.Load(), observed.deleteCalls.Load())
	}
}

func TestBatchingServerKeepsLuaDeclarationsRequestScoped(t *testing.T) {
	backend := memory.New()
	for _, key := range []string{"declared", "undeclared"} {
		seed := memory.SeedRequest{
			Address:  storageAddress(key),
			Document: storageJSONDocument(`{"value":0}`),
		}
		backend.Seed(seed)
	}
	observed := &countingStorage{backend: backend}
	server := newBatchingTestServer(t, observed, nil, 2)
	defer server.Close()

	declared := mergeWriteRequest("declared", "1")
	undeclared := mergeWriteRequest("undeclared", "1")
	undeclared.LuaPrograms = nil
	requests := []*sink.WriteRequest{declared, undeclared}
	responses := make(chan *sink.WriteResponse, len(requests))
	errors := make(chan error, len(requests))
	start := make(chan struct{})
	for _, request := range requests {
		go func() {
			<-start
			response, err := server.Write(t.Context(), request)
			responses <- response
			errors <- err
		}()
	}
	close(start)

	statuses := make(map[sink.WriteStatus]int)
	for range requests {
		if err := <-errors; err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		response := <-responses
		statuses[response.GetResults()[0].GetStatus()]++
	}
	if statuses[sink.WriteStatus_WRITE_STATUS_APPLIED] != 1 || statuses[sink.WriteStatus_WRITE_STATUS_FAILED] != 1 {
		t.Fatalf("write statuses = %v", statuses)
	}
}

func TestBatchingServerPreservesConcurrentMergeUpdates(t *testing.T) {
	const writerCount = 50
	backend := memory.New()
	seed := memory.SeedRequest{
		Address:  storageAddress("counter"),
		Document: storageJSONDocument(`{"value":0}`),
	}
	backend.Seed(seed)
	server := newBatchingTestServer(t, backend, nil, writerCount)
	defer server.Close()

	var waitGroup sync.WaitGroup
	errors := make(chan error, writerCount)
	start := make(chan struct{})
	waitGroup.Add(writerCount)
	for range writerCount {
		go func() {
			defer waitGroup.Done()
			<-start
			response, err := server.Write(t.Context(), mergeWriteRequest("counter", "1"))
			if err == nil && response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
				err = errUnexpectedStatus(response.GetResults()[0].GetStatus())
			}
			errors <- err
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errors)
	assertNoBatchErrors(t, errors)

	readResponse, err := server.Read(t.Context(), readRequest("counter"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	var document struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(readResponse.GetResults()[0].GetDocument().GetJson(), &document); err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if document.Value != writerCount {
		t.Fatalf("counter = %d, want %d", document.Value, writerCount)
	}
}

func BenchmarkSingleOperationReads(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		backend := &latencyStorage{
			backend:     memory.New(),
			delay:       500 * time.Microsecond,
			concurrency: make(chan struct{}, 8),
		}
		observed := &countingStorage{backend: backend}
		server := newTestServer(b, observed, nil)
		request := readRequest("benchmark")
		b.SetParallelism(16)
		b.ResetTimer()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				if _, err := server.Read(context.Background(), request); err != nil {
					b.Errorf("Read() error = %v", err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(observed.readCalls.Load())/float64(b.N), "storage_calls/op")
	})

	b.Run("server_batched", func(b *testing.B) {
		backend := &latencyStorage{
			backend:     memory.New(),
			delay:       500 * time.Microsecond,
			concurrency: make(chan struct{}, 8),
		}
		observed := &countingStorage{backend: backend}
		core := newTestServer(b, observed, nil)
		options := service.BatchingOptions{
			MaxWait:             100 * time.Microsecond,
			MaxOperations:       1000,
			MaxBytes:            16 << 20,
			MaxQueuedOperations: 10_000,
			MaxQueuedBytes:      128 << 20,
		}
		server, err := service.NewBatchingServer(core, options)
		if err != nil {
			b.Fatalf("NewBatchingServer() error = %v", err)
		}
		defer server.Close()
		request := readRequest("benchmark")
		b.SetParallelism(16)
		b.ResetTimer()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				if _, err := server.Read(context.Background(), request); err != nil {
					b.Errorf("Read() error = %v", err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(observed.readCalls.Load())/float64(b.N), "storage_calls/op")
	})
}

func errUnexpectedStatus(value any) error {
	err := fmt.Errorf("unexpected operation status %v", value)
	return err
}

func newBatchingTestServer(
	t *testing.T,
	store storage.Storage,
	publisher queue.Publisher,
	maxOperations int,
) *service.BatchingServer {
	t.Helper()
	core := newTestServer(t, store, publisher)
	options := service.BatchingOptions{
		MaxWait:             100 * time.Millisecond,
		MaxOperations:       maxOperations,
		MaxBytes:            16 << 20,
		MaxQueuedOperations: 1000,
		MaxQueuedBytes:      128 << 20,
	}
	server, err := service.NewBatchingServer(core, options)
	if err != nil {
		t.Fatalf("NewBatchingServer() error = %v", err)
	}
	return server
}

func newLongWaitBatchingTestServer(
	t *testing.T,
	store storage.Storage,
	publisher queue.Publisher,
) *service.BatchingServer {
	t.Helper()
	core := newTestServer(t, store, publisher)
	options := service.BatchingOptions{
		MaxWait:             time.Hour,
		MaxOperations:       1000,
		MaxBytes:            16 << 20,
		MaxQueuedOperations: 1000,
		MaxQueuedBytes:      128 << 20,
	}
	server, err := service.NewBatchingServer(core, options)
	if err != nil {
		t.Fatalf("NewBatchingServer() error = %v", err)
	}
	return server
}

func batchTestKey(index int) string {
	return fmt.Sprintf("batch-%d", index)
}

func assertNoBatchErrors(t *testing.T, errors <-chan error) {
	t.Helper()
	for err := range errors {
		if err != nil {
			t.Fatalf("batched RPC error = %v", err)
		}
	}
}
