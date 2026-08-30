package search

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

type expectedRequest struct {
	method        string
	path          string
	rawQuery      string
	contentType   string
	authorization string
	bodyContains  []string
	statusCode    int
	responseBody  string
}

type scriptedHandler struct {
	t        *testing.T
	mu       sync.Mutex
	requests []expectedRequest
	next     int
}

type failingRoundTripper struct {
	err error
}

func (f failingRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, f.err
}

type failingReadCloser struct {
	err error
}

func (f failingReadCloser) Read(_ []byte) (int, error) {
	return 0, f.err
}

func (f failingReadCloser) Close() error {
	return nil
}

type responseRoundTripper struct {
	body io.ReadCloser
}

func (r responseRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       r.body,
		Request:    request,
	}
	return response, nil
}

func (h *scriptedHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.next >= len(h.requests) {
		h.t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	expected := h.requests[h.next]
	h.next++
	body, err := io.ReadAll(request.Body)
	if err != nil {
		h.t.Errorf("read request body: %v", err)
	}
	if request.Method != expected.method || request.URL.Path != expected.path {
		h.t.Errorf("request = %s %s, expected %s %s", request.Method, request.URL.Path, expected.method, expected.path)
	}
	if request.URL.RawQuery != expected.rawQuery {
		h.t.Errorf("query = %q, expected %q", request.URL.RawQuery, expected.rawQuery)
	}
	if expected.contentType != "" && request.Header.Get("Content-Type") != expected.contentType {
		h.t.Errorf("Content-Type = %q, expected %q", request.Header.Get("Content-Type"), expected.contentType)
	}
	if expected.authorization != "" && request.Header.Get("Authorization") != expected.authorization {
		h.t.Errorf("Authorization = %q, expected %q", request.Header.Get("Authorization"), expected.authorization)
	}
	for _, fragment := range expected.bodyContains {
		if !bytes.Contains(body, []byte(fragment)) {
			h.t.Errorf("request body %q does not contain %q", body, fragment)
		}
	}
	writer.Header().Set("Content-Type", ContentTypeJSON)
	writer.WriteHeader(expected.statusCode)
	if _, err := writer.Write([]byte(expected.responseBody)); err != nil {
		h.t.Errorf("write response: %v", err)
	}
}

func (h *scriptedHandler) verify() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.next != len(h.requests) {
		h.t.Errorf("handled %d requests, expected %d", h.next, len(h.requests))
	}
}

func TestDocumentIDPreservesLegacyStringsAndSeparatesTypedKeys(t *testing.T) {
	plain, err := documentID(storage.Key{Type: "string", Data: []byte("legacy-id")})
	if err != nil || plain != "legacy-id" {
		t.Fatalf("documentID(string) = %q, %v", plain, err)
	}
	escaped, err := documentID(storage.Key{Type: "string", Data: []byte("~sink~int64~1")})
	if err != nil || !strings.HasPrefix(escaped, "~sink~string~") {
		t.Fatalf("documentID(escaped string) = %q, %v", escaped, err)
	}
	integerData := make([]byte, 8)
	binary.BigEndian.PutUint64(integerData, 1)
	integer, err := documentID(storage.Key{Type: "int64", Data: integerData})
	if err != nil || integer != "~sink~int64~1" || integer == escaped {
		t.Fatalf("documentID(int64) = %q, %v", integer, err)
	}
	bytesID, err := documentID(storage.Key{Type: "bytes", Data: []byte("legacy-id")})
	if err != nil || bytesID == plain {
		t.Fatalf("documentID(bytes) = %q, %v", bytesID, err)
	}
}

func TestStoreReadsExistingAndMissingDocuments(t *testing.T) {
	requests := []expectedRequest{
		{
			method:        http.MethodPost,
			path:          "/_mget",
			contentType:   ContentTypeJSON,
			authorization: "ApiKey test-key",
			bodyContains:  []string{`"_index":"legacy-records"`, `"_id":"legacy-id"`, `"_id":"missing"`},
			statusCode:    http.StatusOK,
			responseBody:  `{"docs":[{"_index":"legacy-records","_id":"legacy-id","found":true,"_seq_no":7,"_primary_term":2,"_source":{"name":"legacy"}},{"_index":"legacy-records","_id":"missing","found":false}]}`,
		},
	}
	store, handler := newScriptedStore(t, requests)
	readRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{
			{Address: testAddress("legacy-id")},
			{Address: testAddress("missing")},
		},
	}
	response, err := store.Read(t.Context(), readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if response.Results[0].Status != storage.ReadStatusFound || response.Results[1].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read() statuses = %v, %v", response.Results[0].Status, response.Results[1].Status)
	}
	if string(response.Results[0].Document.JSON) != `{"name":"legacy"}` {
		t.Fatalf("Read() document = %s", response.Results[0].Document.JSON)
	}
	revision, err := decodeRevision(response.Results[0].Revision)
	if err != nil || revision.sequenceNumber != 7 || revision.primaryTerm != 2 {
		t.Fatalf("Read() revision = %#v, %v", revision, err)
	}
	handler.verify()
}

func TestStoreWritesCreateAndExistingWithBulkCAS(t *testing.T) {
	requests := []expectedRequest{
		{
			method:       http.MethodPost,
			path:         "/_mget",
			contentType:  ContentTypeJSON,
			bodyContains: []string{`"_id":"existing"`},
			statusCode:   http.StatusOK,
			responseBody: `{"docs":[{"_index":"legacy-records","_id":"existing","found":true,"_seq_no":5,"_primary_term":3,"_source":{"value":"old"}}]}`,
		},
		{
			method:      http.MethodPost,
			path:        "/_bulk",
			contentType: "application/x-ndjson",
			bodyContains: []string{
				`"create":{"_index":"legacy-records","_id":"created"}`,
				`"index":{"_index":"legacy-records","_id":"existing","if_seq_no":5,"if_primary_term":3}`,
				`{"value":"new"}`,
			},
			statusCode:   http.StatusOK,
			responseBody: `{"errors":false,"items":[{"create":{"status":201,"_seq_no":0,"_primary_term":1}},{"index":{"status":200,"_seq_no":6,"_primary_term":3}}]}`,
		},
	}
	store, handler := newScriptedStore(t, requests)
	operations := []storage.WriteOperation{
		{
			Address:  testAddress("created"),
			Document: testDocument(`{"value":"new"}`),
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordNotExists,
			},
		},
		{
			Address:  testAddress("existing"),
			Document: testDocument(`{"value":"new"}`),
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordExists,
			},
		},
	}
	writeRequest := storage.WriteRequest{Operations: operations}
	response, err := store.Write(t.Context(), writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	for index, result := range response.Results {
		if result.Status != storage.WriteStatusApplied || len(result.Revision.Data) != revisionSize {
			t.Fatalf("Write() result[%d] = %#v", index, result)
		}
	}
	handler.verify()
}

func TestStoreMapsBulkConflictsAndMissingDeletes(t *testing.T) {
	requests := []expectedRequest{
		{
			method:       http.MethodPost,
			path:         "/_bulk",
			rawQuery:     "refresh=wait_for",
			contentType:  "application/x-ndjson",
			bodyContains: []string{`"if_seq_no":1`, `"if_primary_term":1`},
			statusCode:   http.StatusOK,
			responseBody: `{"errors":true,"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"stale"}}}]}`,
		},
		{
			method:       http.MethodPost,
			path:         "/_bulk",
			rawQuery:     "refresh=wait_for",
			contentType:  "application/x-ndjson",
			bodyContains: []string{`"delete":{"_index":"legacy-records","_id":"missing"}`},
			statusCode:   http.StatusOK,
			responseBody: `{"errors":false,"items":[{"delete":{"status":404}}]}`,
		},
	}
	store, handler := newScriptedStore(t, requests)
	revision, err := encodeRevision(1, 1)
	if err != nil {
		t.Fatalf("encodeRevision() error = %v", err)
	}
	writeOperation := storage.WriteOperation{
		Address:  testAddress("stale"),
		Document: testDocument(`{"value":"new"}`),
		Precondition: storage.Precondition{
			Kind:     storage.PreconditionRevisionMatches,
			Revision: revision,
		},
	}
	writeRequest := storage.WriteRequest{
		Operations:       []storage.WriteOperation{writeOperation},
		WaitUntilVisible: true,
	}
	written, err := store.Write(t.Context(), writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write() status = %v", written.Results[0].Status)
	}
	deleteOperation := storage.DeleteOperation{Address: testAddress("missing")}
	deleteRequest := storage.DeleteRequest{
		Operations:       []storage.DeleteOperation{deleteOperation},
		WaitUntilVisible: true,
	}
	deleted, err := store.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.Results[0].Status != storage.DeleteStatusApplied {
		t.Fatalf("Delete() status = %v", deleted.Results[0].Status)
	}
	handler.verify()
}

func TestStoreClassifiesBulkTransportFailureAsRetryableUnavailable(t *testing.T) {
	transport := failingRoundTripper{err: io.EOF}
	client := &http.Client{Transport: transport}
	opts := Options{
		Driver:     DriverOpenSearch,
		Endpoints:  []string{"http://search.example"},
		Store:      "primary",
		HTTPClient: client,
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	operation := storage.DeleteOperation{Address: testAddress("record")}
	request := storage.DeleteRequest{
		Operations:       []storage.DeleteOperation{operation},
		WaitUntilVisible: true,
	}
	response, err := store.Delete(t.Context(), request)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	result := response.Results[0]
	if result.Status != storage.DeleteStatusFailed {
		t.Fatalf("Delete() status = %v", result.Status)
	}
	code, retryable := storage.ErrorDetails(result.Err)
	if code != storage.ErrorCodeUnavailable || !retryable {
		t.Fatalf("Delete() error details = %s, retryable %t, error %v", code, retryable, result.Err)
	}
}

func TestStoreClassifiesResponseReadFailureAsRetryableUnavailable(t *testing.T) {
	body := failingReadCloser{err: io.ErrUnexpectedEOF}
	transport := responseRoundTripper{body: body}
	client := &http.Client{Transport: transport}
	opts := Options{
		Driver:     DriverElasticsearch,
		Endpoints:  []string{"http://search.example"},
		Store:      "primary",
		HTTPClient: client,
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	operation := storage.DeleteOperation{Address: testAddress("record")}
	request := storage.DeleteRequest{Operations: []storage.DeleteOperation{operation}}
	response, err := store.Delete(t.Context(), request)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	result := response.Results[0]
	if result.Status != storage.DeleteStatusFailed {
		t.Fatalf("Delete() status = %v", result.Status)
	}
	code, retryable := storage.ErrorDetails(result.Err)
	if code != storage.ErrorCodeUnavailable || !retryable {
		t.Fatalf("Delete() error details = %s, retryable %t, error %v", code, retryable, result.Err)
	}
}

func TestStoreRejectsInvalidDocumentsWithoutSendingRequests(t *testing.T) {
	store, handler := newScriptedStore(t, nil)
	operation := storage.WriteOperation{
		Address:  testAddress("invalid"),
		Document: testDocument(`[]`),
	}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	response, err := store.Write(t.Context(), request)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if response.Results[0].Status != storage.WriteStatusFailed {
		t.Fatalf("Write() status = %v", response.Results[0].Status)
	}
	handler.verify()
}

func newScriptedStore(t *testing.T, requests []expectedRequest) (*Store, *scriptedHandler) {
	t.Helper()
	handler := &scriptedHandler{t: t, requests: requests}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
	})
	opts := Options{
		Driver:    DriverElasticsearch,
		Endpoints: []string{server.URL},
		Store:     "primary",
		APIKey:    "test-key",
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store, handler
}

func testAddress(id string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "legacy-records",
		Key:       storage.Key{Type: "string", Data: []byte(id)},
	}
	return address
}

func testDocument(raw string) storage.Document {
	document := storage.Document{JSON: []byte(raw)}
	return document
}

func TestMultiGetRequestIsValidJSON(t *testing.T) {
	references := []multiGetDocumentReference{{Index: "index", ID: "id"}}
	request := multiGetRequest{Documents: references}
	encoded, err := json.Marshal(request)
	if err != nil || !json.Valid(encoded) {
		t.Fatalf("json.Marshal() = %s, %v", encoded, err)
	}
}

func TestReadFailsOverFromUnavailableEndpoint(t *testing.T) {
	var firstCalls atomic.Int64
	firstHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	})
	first := httptest.NewServer(firstHandler)
	t.Cleanup(first.Close)

	var secondCalls atomic.Int64
	secondHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		writer.Header().Set("Content-Type", ContentTypeJSON)
		_, err := writer.Write([]byte(`{"docs":[{"_index":"legacy-records","_id":"record","found":true,"_seq_no":1,"_primary_term":1,"_source":{"value":"ok"}}]}`))
		if err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	second := httptest.NewServer(secondHandler)
	t.Cleanup(second.Close)

	opts := Options{
		Driver:    DriverElasticsearch,
		Endpoints: []string{first.URL, second.URL},
		Store:     "primary",
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: testAddress("record")}},
	}
	response, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if response.Results[0].Status != storage.ReadStatusFound {
		t.Fatalf("Read() result = %+v", response.Results[0])
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("endpoint calls = first %d, second %d", firstCalls.Load(), secondCalls.Load())
	}
}
