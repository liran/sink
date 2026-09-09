package search

import (
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
	req := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_msearch", Query: "q=a&q=b", ContentType: "application/x-ndjson", Payload: []byte("{}\n{}\n"), MaxBytes: 4096}
	response, err := store.Execute(t.Context(), req)
	if err != nil || response.Success || response.StatusCode != 429 || string(response.Payload) != payload || len(response.Headers.Values("Warning")) != 2 || calls.Load() != 1 {
		t.Fatalf("response=%+v calls=%d err=%v", response, calls.Load(), err)
	}
}

func TestNativePathsRejectConnectionOverrides(t *testing.T) {
	tests := []storage.NativeRequest{
		{Method: "GET", Path: "http://other/_search"},
		{Method: "GET", Path: "/products/../_search"},
		{Method: "GET", Path: "/products/%2e%2e/_search"},
		{Method: "GET", Path: "//_search"},
		{Method: "GET", Path: "/products/_search", Headers: http.Header{"Authorization": {"override"}}},
	}
	for _, command := range tests {
		req := command
		if _, err := nativeOptions(req); err == nil {
			t.Errorf("accepted %+v", command)
		}
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
	req := storage.NativeRequest{Store: "search", Method: "GET", Path: "/products/_search", MaxBytes: 100}
	_, err = store.Execute(t.Context(), req)
	code, _ := storage.ErrorDetails(err)
	if code != storage.ErrorCodeResourceExhausted {
		t.Fatalf("oversize error=%v", err)
	}
	req.Query = "redirect=1"
	result, err := store.Execute(t.Context(), req)
	if err != nil || result.StatusCode != 302 || targetCalls.Load() != 0 {
		t.Fatalf("redirect response=%+v err=%v target=%d", result, err, targetCalls.Load())
	}
}

func TestNativeExecutePreservesAllowedRequests(t *testing.T) {
	commands := []storage.NativeRequest{
		{Method: "POST", Path: "/_bulk", ContentType: "application/x-ndjson", Payload: []byte("{\"index\":{\"_index\":\"products\"}}\n{\"value\":1}\n")},
		{Method: "POST", Path: "/products/_update/id", ContentType: "application/json", Payload: []byte(`{"doc":{"value":2}}`)},
		{Method: "PUT", Path: "/products/_doc/a%2Fb%20c", ContentType: "application/json", Payload: []byte(`{"value":1}`)},
		{Method: "DELETE", Path: "/products/_doc/_aliases%2F_close"},
		{Method: "GET", Path: "/_plugins/future/endpoint/", ContentType: "text/plain", Payload: []byte("opaque")},
		{Method: "POST", Path: "/_all/_search", ContentType: "application/json", Payload: []byte(`{}`)},
		{Method: "PUT", Path: "/products/_mapping", ContentType: "application/json", Payload: []byte(`{"properties":{"title":{"type":"keyword"}}}`)},
	}
	for _, command := range commands {
		t.Run(command.Method+command.Path, func(t *testing.T) {
			command.Headers = http.Header{"X-Native-Option": {"value"}}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Method != command.Method || r.URL.EscapedPath() != "/prefix"+command.Path || string(body) != string(command.Payload) || r.Header.Get("X-Native-Option") != "value" {
					t.Errorf("request changed: %s %s %s", r.Method, r.URL.EscapedPath(), body)
				}
				if command.Path == "/_bulk" && r.Header.Get("Content-Type") != "application/x-ndjson" {
					t.Error("bulk lost NDJSON content type")
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("original backend error\n"))
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL + "/prefix"}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			req := command
			req.Store = "search"
			req.MaxBytes = 4096
			response, err := store.Execute(t.Context(), req)
			if err != nil || response.Success || response.StatusCode != 400 || string(response.Payload) != "original backend error\n" {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
	}
}
