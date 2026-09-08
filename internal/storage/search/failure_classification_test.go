package search

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestStorageFailuresRemainRetryable(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 408, 413, 429, 500, 502, 503, 504} {
		for _, operation := range []string{"write", "delete", "read"} {
			t.Run(fmt.Sprintf("http-%d/%s", status, operation), func(t *testing.T) {
				response := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: status,
					responseBody: `{"error":{"type":"environment_failure","reason":"injected dependency failure"}}`}
				if operation == "read" {
					response.path = "/_mget"
				}
				assertStorageFailure(t, operation, response, true)
			})
		}
	}
	for _, status := range []int{200, 400, 401, 403, 404, 409, 429, 500, 503} {
		for _, operation := range []string{"write", "delete"} {
			t.Run(fmt.Sprintf("item-%d/%s", status, operation), func(t *testing.T) {
				action := "index"
				if operation == "delete" {
					action = "delete"
				}
				response := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200,
					responseBody: fmt.Sprintf(`{"items":[{%q:{"status":%d,"error":{"type":"cluster_block_exception","reason":"injected block"}}}]}`, action, status)}
				assertStorageFailure(t, operation, response, true)
			})
		}
	}
}

func TestMalformedBackendResponsesCannotAcknowledgeOrQuarantine(t *testing.T) {
	for _, body := range []string{`{`, `{"items":[]}`, `{"items":[{}]}`, `{"items":[{"index":{"status":200}}]}`} {
		t.Run(body, func(t *testing.T) {
			response := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, responseBody: body}
			assertStorageFailure(t, "write", response, true)
		})
	}
	for _, body := range []string{`{`, `{"docs":[]}`, `{"docs":[{}]}`, `{"docs":[{"found":true}]}`, `{"docs":[{"error":{"type":"unknown_backend_error"}}]}`} {
		t.Run(body, func(t *testing.T) {
			response := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: 200, responseBody: body}
			assertStorageFailure(t, "read", response, true)
		})
	}
}

func TestOnlyConfirmedDocumentErrorsArePermanent(t *testing.T) {
	for _, kind := range []string{"mapper_parsing_exception", "document_parsing_exception", "strict_dynamic_mapping_exception"} {
		t.Run(kind, func(t *testing.T) {
			response := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200,
				responseBody: fmt.Sprintf(`{"items":[{"index":{"status":400,"error":{"type":%q,"reason":"invalid document"}}}]}`, kind)}
			assertStorageFailure(t, "write", response, false)
		})
	}
	response := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 400,
		responseBody: `{"error":{"type":"mapper_parsing_exception","reason":"whole request failed"}}`}
	assertStorageFailure(t, "write", response, true)
}

func assertStorageFailure(t *testing.T, operation string, reply expectedRequest, retryable bool) {
	t.Helper()
	store, handler := newScriptedStore(t, []expectedRequest{reply})
	defer handler.verify()
	address := testAddress("record")
	var failure error
	switch operation {
	case "write":
		op := storage.WriteOperation{Address: address, Document: testDocument(`{"counter":1}`)}
		req := storage.WriteRequest{Operations: []storage.WriteOperation{op}}
		resp, err := store.Write(t.Context(), req)
		if err != nil || len(resp.Results) != 1 || resp.Results[0].Status != storage.WriteStatusFailed {
			t.Fatalf("backend failure acknowledged as write success/precondition: %+v, %v", resp, err)
		}
		failure = resp.Results[0].Err
	case "delete":
		op := storage.DeleteOperation{Address: address}
		req := storage.DeleteRequest{Operations: []storage.DeleteOperation{op}}
		resp, err := store.Delete(t.Context(), req)
		if err != nil || len(resp.Results) != 1 || resp.Results[0].Status != storage.DeleteStatusFailed {
			t.Fatalf("backend failure acknowledged as delete success: %+v, %v", resp, err)
		}
		failure = resp.Results[0].Err
	case "read":
		op := storage.ReadOperation{Address: address}
		req := storage.ReadRequest{Operations: []storage.ReadOperation{op}}
		resp, err := store.Read(t.Context(), req)
		if err != nil || len(resp.Results) != 1 || resp.Results[0].Status != storage.ReadStatusFailed {
			t.Fatalf("backend failure acknowledged as read success/absence: %+v, %v", resp, err)
		}
		failure = resp.Results[0].Err
	}
	code, actual := storage.ErrorDetails(failure)
	if failure == nil || actual != retryable || (!retryable && code != storage.ErrorCodeInvalidArgument) {
		t.Fatalf("incorrect storage failure classification: code=%v retryable=%t want=%t error=%v", code, actual, retryable, failure)
	}
}
