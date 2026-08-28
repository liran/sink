package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

type retryHandler struct {
	calls     int
	retryable bool
}

func (h *retryHandler) HandleBatch(_ context.Context, mutations []queue.Mutation) []error {
	h.calls++
	results := make([]error, len(mutations))
	for index := range results {
		results[index] = retryHandlerError{retryable: h.retryable}
	}
	return results
}

type retryHandlerError struct {
	retryable bool
}

func (e retryHandlerError) Error() string {
	return "handler failed"
}

func (e retryHandlerError) Retryable() bool {
	return e.retryable
}

func TestHandleWithRetryIsBounded(t *testing.T) {
	handler := &retryHandler{retryable: true}
	worker := &Worker{
		handler:          handler,
		maxRetryAttempts: 3,
		retryBackoff:     time.Nanosecond,
		maxRetryBackoff:  time.Nanosecond,
	}
	mutations := []queue.Mutation{{}, {}}
	results := worker.handleWithRetry(t.Context(), mutations)
	if handler.calls != 3 {
		t.Fatalf("HandleBatch() calls = %d, want 3", handler.calls)
	}
	for index, result := range results {
		if result == nil {
			t.Fatalf("result[%d] = nil", index)
		}
	}
}

func TestHandleWithRetryDoesNotRetryPermanentErrors(t *testing.T) {
	handler := &retryHandler{}
	worker := &Worker{
		handler:          handler,
		maxRetryAttempts: 3,
		retryBackoff:     time.Nanosecond,
		maxRetryBackoff:  time.Nanosecond,
	}
	mutations := []queue.Mutation{{}}
	results := worker.handleWithRetry(t.Context(), mutations)
	if handler.calls != 1 {
		t.Fatalf("HandleBatch() calls = %d, want 1", handler.calls)
	}
	if len(results) != 1 || results[0] == nil {
		t.Fatalf("results = %#v", results)
	}
}

func TestHandleFetchesContinuesAfterRecoverableFetchError(t *testing.T) {
	worker := &Worker{}
	partition := kgo.FetchPartition{Partition: 2, Err: errors.New("temporary fetch failure")}
	topic := kgo.FetchTopic{Topic: "mutations", Partitions: []kgo.FetchPartition{partition}}
	fetch := kgo.Fetch{Topics: []kgo.FetchTopic{topic}}
	fetches := kgo.Fetches{fetch}
	if err := worker.handleFetches(t.Context(), fetches); err != nil {
		t.Fatalf("handleFetches() error = %v", err)
	}
}

func TestHandleFetchesRejectsClosedClient(t *testing.T) {
	worker := &Worker{}
	partition := kgo.FetchPartition{Err: kgo.ErrClientClosed}
	topic := kgo.FetchTopic{Partitions: []kgo.FetchPartition{partition}}
	fetch := kgo.Fetch{Topics: []kgo.FetchTopic{topic}}
	fetches := kgo.Fetches{fetch}
	if err := worker.handleFetches(t.Context(), fetches); err == nil {
		t.Fatal("handleFetches() error = nil")
	}
}
