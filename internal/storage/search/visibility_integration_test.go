//go:build integration

package search_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/search"
)

func TestImmediateVisibilitySerialMergesWithPeriodicRefreshDisabled(t *testing.T) {
	fixture := newIntegrationFixture(t)
	settings := []byte(`{"index":{"refresh_interval":"-1"}}`)
	code, body := fixture.request(t, http.MethodPut, "/"+fixture.index+"/_settings", settings)
	if code != http.StatusOK {
		t.Fatalf("disable periodic refresh: HTTP %d: %s", code, body)
	}
	opts := search.Options{
		Driver: search.Driver(os.Getenv(searchTestDriver)), Endpoints: []string{fixture.endpoint},
		Store: "primary", HTTPClient: fixture.client, VisibleRefresh: search.VisibleRefreshImmediate,
	}
	store, err := search.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := service.Options{Storage: store, Lua: engine, RequestTimeout: 10 * time.Second, MaxReadBytes: 1 << 20}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	address := fixture.sinkAddress("shared-seller")
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming)
    current = current or json.object()
    current.counter = (current.counter or 0) + incoming.delta
    return current
end`)}
	operations := make([]*sink.WriteOperation, 0, 40)
	for range 40 {
		incoming := sinkDocument(`{"delta":1}`)
		mutation := &sink.MergeOperation{IncomingDocument: incoming, LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE}
		action := &sink.WriteOperation_Merge{Merge: mutation}
		operation := &sink.WriteOperation{Address: address, Action: action}
		operations = append(operations, operation)
	}
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: operations}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	response, err := server.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("merge %d: %+v", index, result)
		}
	}
	code, body = fixture.request(t, http.MethodGet, "/"+fixture.index+"/_search", nil)
	var found struct {
		Hits struct {
			Hits []struct {
				Source counterDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &found); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || len(found.Hits.Hits) != 1 || found.Hits.Hits[0].Source.Counter != 40 {
		t.Fatalf("immediate search: HTTP %d: %s", code, body)
	}
	deleteOperation := &sink.DeleteOperation{Address: address}
	deleteRequest := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.DeleteOperation{deleteOperation}}
	deleted, err := server.Delete(ctx, deleteRequest)
	if err != nil || deleted.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
		t.Fatalf("Delete() = %+v, %v", deleted, err)
	}
	code, body = fixture.request(t, http.MethodGet, "/"+fixture.index+"/_search", nil)
	if err := json.Unmarshal(body, &found); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || len(found.Hits.Hits) != 0 {
		t.Fatalf("search after delete: HTTP %d: %s", code, body)
	}
}
