package search

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestScanResumesOnAnotherServerWithHeadersAndExactSortValues(t *testing.T) {
	var failed atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/products/_search" || r.URL.Query().Get("routing") != "tenant" || r.URL.Query().Has("scroll") || r.Header.Get("Es-Security-Runas-User") != "reader" || len(r.Header.Values("X-Trace")) != 2 {
			t.Errorf("lost scope or headers: %s %v", r.URL, r.Header)
		}
		var body struct {
			After json.RawMessage `json:"search_after"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body.After) == 0 {
			_, _ = w.Write([]byte(`{"hits":{"hits":[{"_id":"1","sort":[9007199254740992]},{"_id":"2","sort":[9007199254740993]},{"_id":"3","sort":[9007199254740994]}]}}`))
			return
		}
		if string(body.After) != "[9007199254740993]" {
			t.Errorf("seek precision lost: %s", body.After)
		}
		if !failed.Swap(true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"hits":{"hits":[{"_id":"3","sort":[9007199254740994]}]}}`))
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	options := Options{Driver: DriverElasticsearch, Store: "search", Endpoints: []string{backend.URL}}
	first, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	headers := http.Header{"Es-Security-Runas-User": {"reader"}, "X-Trace": {"one", "two"}}
	command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", Query: "routing=tenant", Headers: headers, ContentType: ContentTypeJSON, Payload: []byte(`{"sort":[{"uid":"asc"}]}`), MaxBytes: 4096}
	request := storage.ScanRequest{Request: command, BatchSize: 2}
	page, err := first.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 2 || len(page.NextCursor) == 0 {
		t.Fatalf("first=%+v err=%v", page, err)
	}
	request.Cursor = page.NextCursor
	checkpoint := bytes.Clone(request.Cursor)
	page, err = second.Scan(t.Context(), request)
	if err == nil || len(page.Documents) != 0 || len(page.NextCursor) != 0 {
		t.Fatalf("failed page=%+v err=%v", page, err)
	}
	page, err = second.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) != 0 || !bytes.Equal(request.Cursor, checkpoint) {
		t.Fatalf("resumed=%+v err=%v", page, err)
	}
	if headers.Get("Content-Type") != "" || headers.Get("Accept") != "" {
		t.Fatal("caller headers were modified")
	}
}
