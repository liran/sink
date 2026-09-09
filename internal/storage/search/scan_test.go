package search

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestScanRejectsUnsafeOrdersBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	backend := httptest.NewServer(handler)
	defer backend.Close()
	options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`{}`, `{"sort":["_id"]}`, `{"sort":["_doc"]}`, `{"sort":["_shard_doc"]}`, `{"sort":["_score"]}`, `{"sort":["uid","uid"]}`, `{"sort":[{"uid":{"missing":"_last"}}]}`, `{"sort":["uid"],"pit":{"id":"x"}}`, `{"sort":["uid"],"collapse":{"field":"uid"}}`} {
		command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(payload), MaxBytes: 4096}
		request := storage.ScanRequest{Request: command, BatchSize: 2}
		if _, err := store.Scan(t.Context(), request); err == nil {
			t.Errorf("accepted %s", payload)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid scan reached backend")
	}
}

func TestScanRejectsPartialResultsAndAmbiguousBoundary(t *testing.T) {
	for _, payload := range []string{
		`{"timed_out":true,"hits":{"hits":[]}}`,
		`{"_shards":{"failed":1},"hits":{"hits":[]}}`,
		`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"_id":"1"}]}}`,
		`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"sort":[null]}]}}`,
		`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"sort":[1]},{"sort":[1]}]}}`,
		`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"sort":[1]},{"sort":[2]},{"sort":[2]}]}}`,
	} {
		t.Run(payload, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(payload)) })
			backend := httptest.NewServer(handler)
			defer backend.Close()
			options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
			store, err := New(options)
			if err != nil {
				t.Fatal(err)
			}
			command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 4096}
			request := storage.ScanRequest{Request: command, BatchSize: 2}
			page, err := store.Scan(t.Context(), request)
			if err == nil || len(page.Documents) != 0 || len(page.NextCursor) != 0 {
				t.Fatalf("partial page=%+v err=%v", page, err)
			}
		})
	}
}

func TestScanByteLimitedPageResumesWithoutSkipping(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			After []int `json:"search_after"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body.After) == 0 {
			_, _ = w.Write([]byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"sort":[1]},{"sort":[2]},{"sort":[3]}]}}`))
			return
		}
		if body.After[0] != 2 {
			t.Errorf("wrong checkpoint: %v", body.After)
		}
		_, _ = w.Write([]byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"sort":[3]}]}}`))
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 300}
	request := storage.ScanRequest{Request: command, BatchSize: 3}
	page, err := store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 2 || len(page.NextCursor) == 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	request.Cursor = page.NextCursor
	page, err = store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) != 0 {
		t.Fatalf("last=%+v err=%v", page, err)
	}
}
