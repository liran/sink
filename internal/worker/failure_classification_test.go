package worker

import (
	"context"
	"errors"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
)

type failingApplier struct {
	failure *sink.Failure
	calls   int
}

func (a *failingApplier) Write(_ context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	a.calls++
	result := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
	if a.calls == 1 {
		result.Status, result.Failure = sink.WriteStatus_WRITE_STATUS_FAILED, a.failure
	}
	response := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
	return response, nil
}

func (a *failingApplier) Delete(context.Context, *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	return nil, errors.New("unexpected delete")
}

func TestUnknownFailuresRetainSameRecordBarrier(t *testing.T) {
	for _, code := range []sink.FailureCode{sink.FailureCode_FAILURE_CODE_UNSPECIFIED, sink.FailureCode_FAILURE_CODE_INTERNAL,
		sink.FailureCode_FAILURE_CODE_UNAVAILABLE, sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED,
		sink.FailureCode_FAILURE_CODE_CONFLICT, 99, -1} {
		t.Run(code.String(), func(t *testing.T) {
			failure := &sink.Failure{Code: code, Message: "unknown/environment failure", Retryable: false}
			assertFailureBarrier(t, failure, true)
		})
	}
	t.Run("missing details", func(t *testing.T) { assertFailureBarrier(t, nil, true) })
}

func TestConfirmedPermanentFailureReleasesSameRecordBarrier(t *testing.T) {
	for _, code := range []sink.FailureCode{sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT,
		sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED, sink.FailureCode_FAILURE_CODE_NOT_FOUND,
		sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED} {
		t.Run(code.String(), func(t *testing.T) {
			failure := &sink.Failure{Code: code, Message: "confirmed invalid record", Retryable: false}
			assertFailureBarrier(t, failure, false)
			failure.Retryable = true
			assertFailureBarrier(t, failure, true)
		})
	}
}

func assertFailureBarrier(t *testing.T, failure *sink.Failure, retain bool) {
	t.Helper()
	applier := &failingApplier{failure: failure}
	processor, err := NewProcessor(applier)
	if err != nil {
		t.Fatal(err)
	}
	key := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: "same-record"}}
	address := &sink.RecordAddress{Store: "primary", Namespace: "catalog", Dataset: "records", Key: key}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{}`)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	write := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	mutation := queue.Mutation{Write: write}
	results := processor.HandleBatch(t.Context(), []queue.Mutation{mutation, mutation})
	var first *ApplyError
	if len(results) != 2 || !errors.As(results[0], &first) || first.Retryable() != retain {
		t.Fatalf("incorrect first failure: %v", results)
	}
	if retain {
		var following *ApplyError
		if applier.calls != 1 || !errors.As(results[1], &following) || !following.Retryable() {
			t.Fatalf("unknown failure lost same-record barrier: calls=%d results=%v", applier.calls, results)
		}
	} else if applier.calls != 2 || results[1] != nil {
		t.Fatalf("permanent failure blocked following valid record: calls=%d results=%v", applier.calls, results)
	}
}
