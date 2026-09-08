package search

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestQueryAndCountApplyControlsWithoutOpeningCursor(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if r.URL.Query().Has("scroll") || r.URL.Query().Has("from") || r.URL.Query().Has("size") || r.Method != "POST" {
			t.Errorf("unexpected query state: %s %s", r.Method, r.URL)
		}
		if string(body["query"]) != `{"match_all":{}}` {
			t.Errorf("query filter changed: %s", body["query"])
		}
		if calls == 1 {
			if string(body["from"]) != "2" || string(body["size"]) != "3" || string(body["sort"]) != `[{"number":"desc"},{"uid":"asc"}]` || string(body["_source"]) != `{"includes":["number"]}` || r.URL.Query().Has("sort") || r.URL.Query().Has("_source") {
				t.Errorf("page controls not applied: %s %s", r.URL.RawQuery, body)
			}
			_, _ = w.Write([]byte(`{"hits":{"hits":[{"_id":"4","_source":{"number":4}},{"_id":"3","_source":{"number":3}},{"_id":"2","_source":{"number":2}}]}}`))
		} else {
			if string(body["size"]) != "0" || string(body["track_total_hits"]) != "true" || body["from"] != nil || body["aggs"] != nil || r.URL.Query().Has("track_total_hits") {
				t.Errorf("count did not request an exact unpaged total: %s", body)
			}
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":12001,"relation":"eq"},"hits":[]}}`))
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.NativeRequest{Store: "search", Method: "GET", Path: "/products/_search",
		Query: "from=90&size=90&sort=old&track_total_hits=false&_source=false", ContentType: "application/json", MaxBytes: 4096,
		Payload: []byte(`{"query":{"match_all":{}},"from":50,"size":50,"sort":["old"],"_source":false,"aggs":{"names":{"terms":{"field":"name"}}}}`)}
	projection := &storage.Projection{Fields: []string{"number"}}
	query := storage.QueryRequest{Request: command, Offset: 2, PageSize: 2,
		Sort: []storage.SortField{{Field: "number", Descending: true}, {Field: "uid"}}, Projection: projection}
	page, err := store.Query(t.Context(), query)
	if err != nil || len(page.Documents) != 2 || !page.HasMore || calls != 1 {
		t.Fatalf("page=%+v calls=%d err=%v", page, calls, err)
	}
	count, err := store.Count(t.Context(), command)
	if err != nil || count != 12001 || calls != 2 {
		t.Fatalf("count=%d calls=%d err=%v", count, calls, err)
	}
}

func TestCountRejectsPartialApproximateAndInvalidTotals(t *testing.T) {
	payloads := []string{
		`{"timed_out":true,"hits":{"total":{"value":3,"relation":"eq"},"hits":[]}}`,
		`{"_shards":{"failed":1},"hits":{"total":{"value":3,"relation":"eq"},"hits":[]}}`,
		`{"terminated_early":true,"hits":{"total":{"value":3,"relation":"eq"},"hits":[]}}`,
		`{"hits":{"total":{"value":10000,"relation":"gte"},"hits":[]}}`,
		`{"hits":{"total":{"value":-1,"relation":"eq"},"hits":[]}}`,
		`{"hits":{"total":{"relation":"eq"},"hits":[]}}`,
	}
	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(payload)) })
			server := httptest.NewServer(handler)
			defer server.Close()
			options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
			store, err := New(options)
			if err != nil {
				t.Fatal(err)
			}
			command := storage.NativeRequest{Store: "search", Method: "GET", Path: "/products/_search", MaxBytes: 4096}
			if count, err := store.Count(t.Context(), command); err == nil || count != 0 {
				t.Fatalf("accepted incomplete count %d: %v", count, err)
			}
		})
	}
}

func TestCommonCommandRejectsUnusedFieldsAndConflictingContentTypes(t *testing.T) {
	tests := []storage.NativeRequest{
		{Namespace: "database", Method: "GET", Path: "/_search"},
		{Method: "POST", Path: "/_search", Payload: []byte(`{}`)},
		{Method: "GET", Path: "/_search", Headers: http.Header{"content-type": {"application/json"}}},
		{Method: "GET", Path: "/_search", ContentType: "application/json\r\nX-Foo: bar"},
	}
	for _, command := range tests {
		if _, err := nativeOptions(command); err == nil {
			t.Fatalf("accepted invalid common command: %+v", command)
		}
	}
	for _, query := range []string{"scroll=2m", "source=%7B%7D", "filter_path=hits"} {
		command := storage.NativeRequest{Method: "GET", Path: "/_search", Query: query}
		if _, _, err := pageOptions(command); err == nil {
			t.Fatalf("accepted stateful or incomplete query %s", query)
		}
	}
}
