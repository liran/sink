package search

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestNativeExecutePreservesErrorBodyAndHeadersWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	payload := "{\"error\": {\"type\":\"test\"},\"status\":429}\n"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != "{}\n{}\n" || r.Header.Get("Content-Type") != "application/x-ndjson" || len(r.URL.Query()["q"]) != 2 {
			t.Errorf("request lost native framing or repeated parameters: %s %s", body, r.URL.RawQuery)
		}
		w.Header().Add("Warning", "first")
		w.Header().Add("Warning", "second")
		w.Header().Set("Content-Type", ContentTypeJSON)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(payload))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL, server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := &storage.SearchCommand{Method: "POST", Path: "/products/_msearch", Query: "q=a&q=b", Body: []byte("{}\n{}\n")}
	req := storage.NativeRequest{Store: "search", Search: command, MaxBytes: 4096}
	response, err := store.Execute(t.Context(), req)
	if err != nil || response.Success || response.StatusCode != 429 || string(response.Payload) != payload || len(response.Headers.Values("Warning")) != 2 || calls.Load() != 1 {
		t.Fatalf("response=%+v calls=%d err=%v", response, calls.Load(), err)
	}
}

func TestNativePathsRejectWritesAndConnectionOverrides(t *testing.T) {
	tests := []storage.SearchCommand{
		{Method: "POST", Path: "/_bulk"},
		{Method: "POST", Path: "/products/_update/id"},
		{Method: "PUT", Path: "/products/_doc/id"},
		{Method: "DELETE", Path: "/products"},
		{Method: "GET", Path: "http://other/_search"},
		{Method: "GET", Path: "/products/../_search"},
		{Method: "GET", Path: "/products/%2e%2e/_search"},
		{Method: "GET", Path: "//_search"},
		{Method: "GET", Path: "/products/_search/"},
		{Method: "POST", Path: "/_aliases", Body: []byte(`{"actions":[{"remove_index":{"index":"products"}}]}`)},
		{Method: "GET", Path: "/products/_search", Headers: http.Header{"Authorization": {"override"}}},
	}
	for _, command := range tests {
		req := storage.NativeRequest{Search: &command}
		if _, err := nativeOptions(req); err == nil {
			t.Errorf("accepted %+v", command)
		}
	}
}

func TestNativeAliasManagementPreservesSupportedActions(t *testing.T) {
	payload := []byte(`{"actions":[{"remove":{"index":"old","alias":"live"}},{"add":{"index":"new","alias":"live"}}]}`)
	command := &storage.SearchCommand{Method: "POST", Path: "/_aliases", Body: payload}
	req := storage.NativeRequest{Search: command}
	opts, err := nativeOptions(req)
	if err != nil || string(opts.payload) != string(payload) {
		t.Fatalf("alias command changed: %s, %v", opts.payload, err)
	}
}

func TestNativeScanSplitsPagesWithinDocumentBudget(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete || calls.Add(1) > 1 {
			_, _ = w.Write([]byte(`{"hits":{"hits":[]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"_scroll_id":"cursor","hits":{"hits":[{"_id":"1"},{"_id":"2"},{"_id":"3"}]}}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := &storage.SearchCommand{Method: "POST", Path: "/products/_search"}
	native := storage.NativeRequest{Store: "search", Search: command, MaxBytes: 200}
	req := storage.ScanRequest{Request: native, BatchSize: 2}
	seen := 0
	visit := func(documents []storage.Document) error {
		if len(documents) != 1 {
			t.Errorf("byte budget did not split page: %d", len(documents))
		}
		seen += len(documents)
		return nil
	}
	if err := store.Scan(t.Context(), req, visit); err != nil || seen != 3 {
		t.Fatalf("seen=%d err=%v", seen, err)
	}
}

func TestNativeScanClosesLatestCursorAfterCallbackCancellation(t *testing.T) {
	var calls atomic.Int32
	var closed atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			body, _ := io.ReadAll(r.Body)
			closed.Store(strings.Contains(string(body), "second"))
			_, _ = w.Write([]byte(`{"succeeded":true}`))
			return
		}
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"_scroll_id":"first","hits":{"hits":[{"_id":"1","_source":{"value":1}}]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"_scroll_id":"second","hits":{"hits":[{"_id":"2","_source":{"value":2}}]}}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := &storage.SearchCommand{Method: "POST", Path: "/products/_search", Body: []byte(`{}`)}
	native := storage.NativeRequest{Store: "search", Search: command, MaxBytes: 4096}
	req := storage.ScanRequest{Request: native, BatchSize: 1}
	seen := 0
	stop := errors.New("stop after two")
	visit := func(documents []storage.Document) error {
		seen += len(documents)
		if seen == 2 {
			cancel()
			return stop
		}
		return nil
	}
	err = store.Scan(ctx, req, visit)
	if !errors.Is(err, stop) || seen != 2 || !closed.Load() || calls.Load() != 2 {
		t.Fatalf("seen=%d closed=%v calls=%d err=%v", seen, closed.Load(), calls.Load(), err)
	}
}

func TestNativeScanRejectsPartialSearchResults(t *testing.T) {
	payloads := []string{
		`{"_scroll_id":"cursor","timed_out":true,"hits":{"hits":[]}}`,
		`{"_scroll_id":"cursor","hits":{}}`,
		`{"_scroll_id":"cursor","_shards":{"failed":1},"hits":{"hits":[]}}`,
		`{"hits":{"hits":[{"_id":"1"}]}}`,
	}
	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) { testIncompleteScanPage(t, payload) })
	}
}

func testIncompleteScanPage(t *testing.T, payload string) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(payload)) })
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := &storage.SearchCommand{Method: "POST", Path: "/products/_search"}
	native := storage.NativeRequest{Store: "search", Search: command, MaxBytes: 4096}
	req := storage.ScanRequest{Request: native, BatchSize: 1}
	visit := func(_ []storage.Document) error { t.Error("partial results were delivered"); return nil }
	if err := store.Scan(t.Context(), req, visit); err == nil {
		t.Fatal("partial scan succeeded")
	}
}

func TestNativeExecuteCapsResponseAndDoesNotFollowRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("redirect") {
			w.Header().Set("Location", target.URL)
			w.WriteHeader(http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("x", 200)))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := &storage.SearchCommand{Method: "GET", Path: "/products/_search"}
	req := storage.NativeRequest{Store: "search", Search: command, MaxBytes: 100}
	_, err = store.Execute(t.Context(), req)
	code, _ := storage.ErrorDetails(err)
	if code != storage.ErrorCodeResourceExhausted {
		t.Fatalf("oversize error=%v", err)
	}
	command.Query = "redirect=1"
	result, err := store.Execute(t.Context(), req)
	if err != nil || result.StatusCode != 302 || targetCalls.Load() != 0 {
		t.Fatalf("redirect response=%+v err=%v target=%d", result, err, targetCalls.Load())
	}
}
