package search

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestRegressionPrettyJSONProducesValidBulkFraming(t *testing.T) {
	store, handler := newScriptedStore(t, nil)
	defer handler.verify()
	for _, payload := range []string{`{"value":1}`, "{\n  \"value\": 1\n}"} {
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(payload)}
		op := storage.WriteOperation{Address: testAddress("pretty"), Document: document}
		work, err := store.prepareWrite(0, op)
		if err != nil {
			t.Fatalf("valid JSON was rejected: %v", err)
		}
		works := []writeWork{work}
		bulk, err := buildWriteBulk(works)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSuffix(bulk, []byte{'\n'}), []byte{'\n'})
		if len(lines) != 2 || !json.Valid(lines[0]) || !json.Valid(lines[1]) {
			t.Errorf("accepted JSON creates invalid NDJSON: expected 2 complete JSON lines, got %d: %q", len(lines), bulk)
		}
	}
}

func TestRegressionMultilineJSONDoesNotFailCompactSibling(t *testing.T) {
	// This HTTP fixture enforces Bulk's NDJSON framing, not a mock Storage API.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		lines := bytes.Split(bytes.TrimSuffix(body, []byte{'\n'}), []byte{'\n'})
		for _, line := range lines {
			if !json.Valid(line) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"type":"illegal_argument_exception","reason":"malformed NDJSON"},"status":400}`)
				return
			}
		}
		_, _ = io.WriteString(w, `{"items":[{"index":{"status":201,"_seq_no":1,"_primary_term":1}},{"index":{"status":201,"_seq_no":2,"_primary_term":1}}]}`)
	})
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{endpoint.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	compact := testDocument(`{"value":1}`)
	pretty := testDocument("{\n  \"value\": 2\n}")
	first := storage.WriteOperation{Address: testAddress("compact"), Document: compact}
	second := storage.WriteOperation{Address: testAddress("pretty"), Document: pretty}
	req := storage.WriteRequest{Operations: []storage.WriteOperation{first, second}}
	response, err := store.Write(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Errorf("valid sibling %d failed: status=%v error=%v", index, result.Status, result.Err)
		}
	}
}

func TestReplaceRetriesOnlyInternalRevisionConflicts(t *testing.T) {
	read := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: 200, responseBody: `{"docs":[{"found":true,"_seq_no":1,"_primary_term":1,"_source":{"value":1}}]}`}
	conflict := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, bodyContains: []string{`"if_seq_no":1`}, responseBody: `{"items":[{"index":{"status":409}}]}`}
	reread := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: 200, responseBody: `{"docs":[{"found":true,"_seq_no":2,"_primary_term":1,"_source":{"value":2}}]}`}
	success := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, bodyContains: []string{`"if_seq_no":2`}, responseBody: `{"items":[{"index":{"status":200,"_seq_no":3,"_primary_term":1}}]}`}
	absent := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: 200, responseBody: `{"docs":[{"found":false}]}`}
	lost := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 503, responseBody: `{"error":{"type":"unavailable","reason":"unknown commit outcome"}}`}
	for _, scenario := range []string{"rebase", "deleted", "exhausted", "explicit_revision", "lost_ack"} {
		t.Run(scenario, func(t *testing.T) {
			condition := storage.Precondition{Kind: storage.PreconditionRecordExists}
			requests := []expectedRequest{read, conflict}
			want := storage.WriteStatusApplied
			switch scenario {
			case "rebase":
				requests = append(requests, reread, success)
			case "deleted":
				requests = append(requests, absent)
				want = storage.WriteStatusPreconditionFailed
			case "exhausted":
				requests = append(requests, read, conflict, read, conflict)
				want = storage.WriteStatusFailed
			case "explicit_revision":
				revision, err := encodeRevision(1, 1)
				if err != nil {
					t.Fatal(err)
				}
				condition.Kind = storage.PreconditionRevisionMatches
				condition.Revision = revision
				requests = []expectedRequest{conflict}
				want = storage.WriteStatusPreconditionFailed
			case "lost_ack":
				requests = []expectedRequest{read, lost}
				want = storage.WriteStatusFailed
			}
			store, handler := newScriptedStore(t, requests)
			defer handler.verify()
			operation := storage.WriteOperation{Address: testAddress("existing"), Document: testDocument(`{"value":3}`), Precondition: condition}
			request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			response, err := store.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := response.Results[0]
			if result.Status != want {
				t.Fatalf("result: %+v, want %v", result, want)
			}
			if scenario == "exhausted" {
				code, retryable := storage.ErrorDetails(result.Err)
				if code != storage.ErrorCodeConflict || !retryable {
					t.Fatalf("conflict classification: %v, %v", code, retryable)
				}
			}
		})
	}
}

func TestReplaceRetryDoesNotReplaySuccessfulSibling(t *testing.T) {
	read := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: 200, responseBody: `{"docs":[{"found":true,"_seq_no":1,"_primary_term":1,"_source":{}}]}`}
	first := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, responseBody: `{"items":[{"index":{"status":409}},{"index":{"status":201,"_seq_no":10,"_primary_term":1}}]}`}
	last := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, responseBody: `{"items":[{"index":{"status":200,"_seq_no":2,"_primary_term":1}}]}`}
	requests := []expectedRequest{read, first, read, last}
	store, handler := newScriptedStore(t, requests)
	defer handler.verify()
	condition := storage.Precondition{Kind: storage.PreconditionRecordExists}
	replace := storage.WriteOperation{Address: testAddress("existing"), Document: testDocument(`{}`), Precondition: condition}
	sibling := storage.WriteOperation{Address: testAddress("sibling"), Document: testDocument(`{}`)}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{replace, sibling}}
	response, err := store.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatal(result)
		}
	}
	revision, err := decodeRevision(response.Results[1].Revision)
	if err != nil || revision.sequenceNumber != 10 {
		t.Fatalf("successful sibling changed: %v, %v", revision, err)
	}
}
