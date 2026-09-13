package service

import (
	"context"
	"testing"
	"testing/synctest"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionPublisher struct {
	entered chan struct{}
	release chan struct{}
	calls   int
}

func (p *admissionPublisher) Publish(ctx context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	response := queue.PublishResponse{Results: make([]queue.PublishResult, len(req.Mutations))}
	p.calls++
	if p.entered != nil {
		p.entered <- struct{}{}
		select {
		case <-p.release:
		case <-ctx.Done():
			return response, ctx.Err()
		}
	}
	for index := range response.Results {
		response.Results[index].Status = queue.PublishStatusAccepted
	}
	return response, nil
}

func TestPublishingContinuesBehindSynchronousByteWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New())
		publisher := &admissionPublisher{}
		server.server.publisher = publisher
		server.server.maxInFlightBytes = 100
		initial := admissionRequest{encodedBytes: 80}
		_, release, err := server.server.admitRequest(t.Context(), initial)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		waiting := admissionRequest{encodedBytes: 90, wait: true}
		done := make(chan error, 1)
		go func() {
			_, cleanup, err := server.server.admitRequest(ctx, waiting)
			if cleanup != nil {
				cleanup()
			}
			done <- err
		}()
		synctest.Wait()
		for range 200 {
			operation := completionMerge("async", 1)
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sink.WriteOperation{operation}}
			response, err := server.Write(t.Context(), request)
			if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
				t.Fatalf("synchronous waiter blocked publishing: response=%v error=%v", response, err)
			}
		}
		address := completionPut("async", 1).Address
		operation := &sink.DeleteOperation{Address: address}
		request := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sink.DeleteOperation{operation}}
		response, err := server.Delete(t.Context(), request)
		if err != nil || response.GetResults()[0].GetStatus() != sink.DeleteStatus_DELETE_STATUS_ACCEPTED {
			t.Fatalf("synchronous waiter blocked delete publishing: response=%v error=%v", response, err)
		}
		cancel()
		if status.Code(<-done) != codes.Canceled || publisher.calls != 201 {
			t.Fatal("waiter cancellation or publish count changed")
		}
		if server.server.publishAdmission.inFlightBytes != 0 || server.server.publishAdmission.inFlightRequests != 0 {
			t.Fatal("completed publishes leaked capacity")
		}
	})
}

func TestPublishSaturationPreservesDurabilityAndSynchronousCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New())
		publisher := &admissionPublisher{entered: make(chan struct{}, 1), release: make(chan struct{})}
		server.server.publisher = publisher
		server.server.publishAdmission.maxInFlightRequests = 1
		operation := completionPut("async", 1)
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sink.WriteOperation{operation}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := server.Write(ctx, request)
			done <- err
		}()
		<-publisher.entered
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("acknowledged before the publisher completed")
		}
		_, err := server.Write(t.Context(), request)
		if status.Code(err) != codes.ResourceExhausted || publisher.calls != 1 {
			t.Fatalf("publish limit did not reject before enqueue: %v", err)
		}
		synchronous := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
		response, err := server.Write(t.Context(), synchronous)
		if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("stalled publisher blocked synchronous storage: response=%v error=%v", response, err)
		}
		cancel()
		if <-done == nil {
			t.Fatal("cancelled publish was acknowledged")
		}
		if server.server.publishAdmission.inFlightBytes != 0 || server.server.publishAdmission.inFlightRequests != 0 {
			t.Fatal("cancelled publish leaked capacity")
		}
		close(publisher.release)
		response, err = server.Write(t.Context(), request)
		if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
			t.Fatalf("publish capacity did not recover: response=%v error=%v", response, err)
		}
	})
}

func TestPublishByteBudgetRejectsBeforeEnqueue(t *testing.T) {
	server := completionServer(t, memory.New())
	publisher := &admissionPublisher{}
	server.server.publisher = publisher
	operation := completionPut("async", 1)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sink.WriteOperation{operation}}
	server.server.publishAdmission.maxInFlightBytes = request.SizeVT() - 1
	_, err := server.Write(t.Context(), request)
	if status.Code(err) != codes.ResourceExhausted || publisher.calls != 0 {
		t.Fatalf("publish exceeded byte budget: calls=%d error=%v", publisher.calls, err)
	}
}

type admissionWriteStorage struct {
	storage.Storage
	batches int
}

func (s *admissionWriteStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.batches++
	return s.Storage.Write(ctx, req)
}

func TestMixedWriteBatchReservesOnlyReturningCallers(t *testing.T) {
	backend := &admissionWriteStorage{Storage: memory.New()}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 1024
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{
		completionWriteCall(t.Context(), mode, completionPut("returned", 1)),
		completionWriteCall(t.Context(), mode, completionPut("plain-a", 2)),
		completionWriteCall(t.Context(), mode, completionPut("plain-b", 3)),
		completionWriteCall(t.Context(), mode, completionPut("plain-c", 4)),
	}
	calls[0].request.Operations[0].ReturnDocument = true
	combined := combinedWriteRequest(calls)
	server.server.maxInFlightBytes = combined.SizeVT() + server.server.maxReadBytes
	server.executeWriteBatch(t.Context(), calls)
	for index, call := range calls {
		response := requireWriteResult(t, call)
		if (response.Results[0].GetDocument() != nil) != (index == 0) {
			t.Fatalf("caller %d received the wrong returned-document behavior", index)
		}
	}
	if backend.batches != 1 {
		t.Fatalf("non-returning callers reserved response space and split the batch: batches=%d", backend.batches)
	}
}
