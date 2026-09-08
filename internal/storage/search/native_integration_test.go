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
	command := &storage.SearchCommand{Method: "POST", Path: "/" + fixture.index + "/_search",
		Body: []byte(`{"query":{"range":{"number":{"gte":2}}},"sort":[{"number":"asc"}]}`)}
	native := storage.NativeRequest{Store: "primary", Search: command, MaxBytes: 1 << 20}
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
	command.Path = "/" + fixture.index + "/_msearch"
	command.Body = []byte("{}\n{\"query\":{\"match_all\":{}}}\n{}\n{\"query\":{\"term\":{\"number\":2}}}\n")
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
	command.Path = "/" + fixture.index + "/_search"
	command.Body = []byte(`{"query":{"not_a_query":{}}}`)
	response, err = fixture.store.Execute(ctx, native)
	if err != nil || response.Success || response.StatusCode != 400 || !json.Valid(response.Payload) {
		t.Fatalf("native database error lost: %+v err=%v", response, err)
	}
}

func TestNativeSearchIndexInitialization(t *testing.T) {
	fixture := newIntegrationFixture(t)
	index := fixture.index + "-native"
	t.Cleanup(func() { fixture.request(t, http.MethodDelete, "/"+index, nil) })
	command := &storage.SearchCommand{Method: http.MethodPut, Path: "/" + index,
		Body: []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)}
	native := storage.NativeRequest{Store: "primary", Search: command, MaxBytes: 1 << 20}
	response, err := fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("create index=%+v err=%v", response, err)
	}
	command.Path += "/_mapping"
	command.Body = []byte(`{"properties":{"signature":{"type":"keyword"}}}`)
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("mapping=%+v err=%v", response, err)
	}
	command.Path = "/_aliases"
	command.Method = http.MethodPost
	command.Body = fmt.Appendf(nil, `{"actions":[{"add":{"index":%q,"alias":%q}}]}`, index, index+"-alias")
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success {
		t.Fatalf("alias=%+v err=%v", response, err)
	}
	command.Method = http.MethodHead
	command.Path = "/" + index + "-alias"
	command.Body = nil
	response, err = fixture.store.Execute(t.Context(), native)
	if err != nil || !response.Success || response.StatusCode != 200 || len(response.Payload) != 0 {
		t.Fatalf("index existence=%+v err=%v", response, err)
	}
}
