//go:build integration

package search_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestNativeSearchQueriesMSearchAndScan(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	operations := make([]storage.WriteOperation, 0, 7)
	for number := range 7 {
		document := jsonStorageDocument(fmt.Sprintf(`{"number":%d,"name":"item"}`, number))
		operation := storage.WriteOperation{Address: fixture.address(fmt.Sprint(number)), Document: document}
		operations = append(operations, operation)
	}
	writeRequest := storage.WriteRequest{Operations: operations, WaitUntilVisible: true}
	written, err := fixture.store.Write(ctx, writeRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatalf("seed write=%+v", result)
		}
	}
	native := storage.NativeRequest{Store: "primary", Method: "POST", Path: "/" + fixture.index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"query":{"range":{"number":{"gte":2}}},"sort":[{"number":"asc"}]}`), MaxBytes: 1 << 20}
	response, err := fixture.store.Execute(ctx, native)
	if err != nil || !response.Success {
		t.Fatalf("query=%+v err=%v", response, err)
	}
	var searchResponse struct {
		Hits struct {
			Hits []json.RawMessage `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(response.Payload, &searchResponse); err != nil || len(searchResponse.Hits.Hits) != 5 {
		t.Fatalf("query payload=%s err=%v", response.Payload, err)
	}
	projection := &storage.Projection{Fields: []string{"number"}}
	query := storage.QueryRequest{Request: native, Offset: 2, PageSize: 2,
		Sort: []storage.SortField{{Field: "number", Descending: true}}, Projection: projection}
	for _, offset := range []int64{2, 4, 6} {
		query.Offset = offset
		page, err := fixture.store.Query(ctx, query)
		expected := max(0, min(2, 5-int(offset)))
		if err != nil || len(page.Documents) != expected || page.HasMore != (offset == 2) {
			t.Fatalf("offset=%d page=%+v err=%v", offset, page, err)
		}
		for index, document := range page.Documents {
			var hit struct {
				ID     string         `json:"_id"`
				Source map[string]int `json:"_source"`
			}
			if err := json.Unmarshal(document.Payload, &hit); err != nil || len(hit.Source) != 1 || hit.Source["number"] != 6-int(offset)-index {
				t.Fatalf("sort or projection lost: %s err=%v", document.Payload, err)
			}
		}
	}
	if count, err := fixture.store.Count(ctx, native); err != nil || count != 5 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	scan := storage.ScanRequest{Request: native, BatchSize: 2}
	seen := 2
	visit := func(documents []storage.Document) error {
		for _, document := range documents {
			var hit struct {
				ID     string `json:"_id"`
				Source struct {
					Number int `json:"number"`
				} `json:"_source"`
			}
			if err := json.Unmarshal(document.Payload, &hit); err != nil {
				return err
			}
			if hit.ID != fmt.Sprint(seen) || hit.Source.Number != seen {
				return fmt.Errorf("scan lost native hit or sort order: %s", document.Payload)
			}
			seen++
		}
		return nil
	}
	if err := fixture.store.Scan(ctx, scan, visit); err != nil || seen != 7 {
		t.Fatalf("scan seen=%d err=%v", seen, err)
	}
	stop := errors.New("stop scan")
	canceled, cancel := context.WithCancel(ctx)
	visit = func(_ []storage.Document) error { cancel(); return stop }
	if err := fixture.store.Scan(canceled, scan, visit); !errors.Is(err, stop) {
		t.Fatalf("canceled scan=%v", err)
	}
	statusCode, payload := fixture.request(t, http.MethodGet, "/"+fixture.index+"/_stats/search", nil)
	var stats struct {
		All struct {
			Total struct {
				Search struct {
					OpenContexts int `json:"open_contexts"`
				} `json:"search"`
			} `json:"total"`
		} `json:"_all"`
	}
	if err := json.Unmarshal(payload, &stats); err != nil || statusCode != 200 || stats.All.Total.Search.OpenContexts != 0 {
		t.Fatalf("cursor leaked after cancellation: %s err=%v", payload, err)
	}
	native.Path = "/" + fixture.index + "/_msearch"
	native.ContentType = "application/x-ndjson"
	native.Payload = []byte("{}\n{\"query\":{\"match_all\":{}}}\n{}\n{\"query\":{\"term\":{\"number\":2}}}\n")
	response, err = fixture.store.Execute(ctx, native)
	var msearch struct {
		Responses []json.RawMessage `json:"responses"`
	}
	if err != nil || !response.Success {
		t.Fatalf("msearch=%+v err=%v", response, err)
	}
	if err := json.Unmarshal(response.Payload, &msearch); err != nil || len(msearch.Responses) != 2 {
		t.Fatalf("msearch payload=%s err=%v", response.Payload, err)
	}
	native.Path = "/" + fixture.index + "/_search"
	native.ContentType = "application/json"
	native.Payload = []byte(`{"query":{"not_a_query":{}}}`)
	response, err = fixture.store.Execute(ctx, native)
	if err != nil || response.Success || response.StatusCode != 400 || !json.Valid(response.Payload) {
		t.Fatalf("native database error lost: %+v err=%v", response, err)
	}
}

func TestNativeSearchIndexInitialization(t *testing.T) {
	fixture := newIntegrationFixture(t)
	index := fixture.index + "-native"
	t.Cleanup(func() { fixture.request(t, http.MethodDelete, "/"+index, nil) })
	native := storage.NativeRequest{Store: "primary", Method: http.MethodPut, Path: "/" + index,
		ContentType: "application/json", Payload: []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0}}`), MaxBytes: 1 << 20}
	response, err := fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("create index=%+v err=%v", response, err)
	}
	native.Path += "/_mapping"
	native.Payload = []byte(`{"properties":{"signature":{"type":"keyword"}}}`)
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("mapping=%+v err=%v", response, err)
	}
	native.Path = "/_aliases"
	native.Method = http.MethodPost
	native.Payload = fmt.Appendf(nil, `{"actions":[{"add":{"index":%q,"alias":%q}}]}`, index, index+"-alias")
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("alias=%+v err=%v", response, err)
	}
	native.Method = http.MethodHead
	native.Path = "/" + index + "-alias"
	native.Payload = nil
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success || response.StatusCode != 200 || len(response.Payload) != 0 {
		t.Fatalf("index existence=%+v err=%v", response, err)
	}
}

func TestNativeSearchWritesAndIndexDeletion(t *testing.T) {
	fixture := newIntegrationFixture(t)
	req := storage.NativeRequest{Store: "primary", Method: http.MethodPut, Path: "/" + fixture.index + "/_doc/native%2Fid",
		Query: "refresh=wait_for", ContentType: "application/json", Payload: []byte(`{"count":1}`), MaxBytes: 1 << 20}
	response, err := fixture.store.Execute(t.Context(), req)
	if err != nil || !response.Success || response.StatusCode != http.StatusCreated {
		t.Fatalf("native insert response=%+v err=%v", response, err)
	}
	req.Method = http.MethodPost
	req.Path = "/" + fixture.index + "/_update/native%2Fid"
	req.Payload = []byte(`{"doc":{"count":2}}`)
	response, err = fixture.store.Execute(t.Context(), req)
	if err != nil || !response.Success {
		t.Fatalf("native update response=%+v err=%v", response, err)
	}
	req.Method = http.MethodGet
	req.Path = "/" + fixture.index + "/_source/native%2Fid"
	req.Query = ""
	req.Payload = nil
	response, err = fixture.store.Execute(t.Context(), req)
	var document struct {
		Count int `json:"count"`
	}
	if err != nil || !response.Success {
		t.Fatalf("native get response=%+v err=%v", response, err)
	}
	if err := json.Unmarshal(response.Payload, &document); err != nil || document.Count != 2 {
		t.Fatalf("native update was not applied: %s err=%v", response.Payload, err)
	}
	req.Method = http.MethodPost
	req.Path = "/" + fixture.index + "/_bulk"
	req.ContentType = "application/x-ndjson"
	req.Payload = []byte("{\"index\":{\"_id\":\"bulk\"}}\n{\"count\":3}\n")
	response, err = fixture.store.Execute(t.Context(), req)
	var bulk struct {
		Errors bool              `json:"errors"`
		Items  []json.RawMessage `json:"items"`
	}
	if err != nil || !response.Success {
		t.Fatalf("native bulk response=%+v err=%v", response, err)
	}
	if err := json.Unmarshal(response.Payload, &bulk); err != nil || bulk.Errors || len(bulk.Items) != 1 {
		t.Fatalf("native bulk payload=%s err=%v", response.Payload, err)
	}
	req.Method = http.MethodDelete
	req.Path = "/" + fixture.index
	req.Payload = nil
	response, err = fixture.store.Execute(t.Context(), req)
	if err != nil || !response.Success {
		t.Fatalf("native index deletion response=%+v err=%v", response, err)
	}
}
