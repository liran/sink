package service

import (
	"errors"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitReadResponseRejectsInvalidResultCount(t *testing.T) {
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{{}}}
	call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{
		request: request,
		result:  make(chan batchResult[*sink.ReadResponse], 1),
	}
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{call}
	response := &sink.ReadResponse{}
	splitReadResponse(calls, response, nil)
	result := <-call.result
	if result.response != nil || status.Code(result.err) != codes.Internal {
		t.Fatalf("split result = %+v", result)
	}
}

func TestSplitWriteResponsePropagatesExecutionError(t *testing.T) {
	request := &sink.WriteRequest{Operations: []*sink.WriteOperation{{}}}
	call := &batchCall[*sink.WriteRequest, *sink.WriteResponse]{
		request: request,
		result:  make(chan batchResult[*sink.WriteResponse], 1),
	}
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{call}
	executionErr := errors.New("write failed")
	splitWriteResponse(calls, nil, executionErr)
	result := <-call.result
	if result.response != nil || !errors.Is(result.err, executionErr) {
		t.Fatalf("split result = %+v", result)
	}
}

func TestSplitDeleteResponseRejectsInvalidResultCount(t *testing.T) {
	request := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{{}}}
	call := &batchCall[*sink.DeleteRequest, *sink.DeleteResponse]{
		request: request,
		result:  make(chan batchResult[*sink.DeleteResponse], 1),
	}
	calls := []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]{call}
	response := &sink.DeleteResponse{}
	splitDeleteResponse(calls, response, nil)
	result := <-call.result
	if result.response != nil || status.Code(result.err) != codes.Internal {
		t.Fatalf("split result = %+v", result)
	}
}

func TestRequestStoreClassification(t *testing.T) {
	primary := &sink.RecordAddress{Store: "primary"}
	secondary := &sink.RecordAddress{Store: "secondary"}
	readRequest := &sink.ReadRequest{
		Operations: []*sink.ReadOperation{
			{Address: primary},
			{Address: primary},
		},
	}
	store, same := requestStore(readRequest.GetOperations())
	if !same || store != "primary" {
		t.Fatalf("requestStore(Read) = %q, %t", store, same)
	}

	writeRequest := &sink.WriteRequest{
		Operations: []*sink.WriteOperation{
			{Address: primary},
			{Address: secondary},
		},
	}
	store, same = requestStore(writeRequest.GetOperations())
	if same || store != "" {
		t.Fatalf("requestStore(Write) = %q, %t", store, same)
	}

	deleteRequest := &sink.DeleteRequest{
		Operations: []*sink.DeleteOperation{nil},
	}
	store, same = requestStore(deleteRequest.GetOperations())
	if same || store != "" {
		t.Fatalf("requestStore(Delete) = %q, %t", store, same)
	}
}
