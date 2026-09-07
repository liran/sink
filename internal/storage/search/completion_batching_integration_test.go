//go:build integration

package search_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
)

func TestSearchBatchingKeepsArchiveAppliedWithoutRefresh(t *testing.T) {
	product := newIntegrationFixture(t)
	archive := newIntegrationFixture(t)
	settings := []byte(`{"index":{"refresh_interval":"-1"}}`)
	code, body := archive.request(t, http.MethodPut, "/"+archive.index+"/_settings", settings)
	if code != http.StatusOK {
		t.Fatalf("disable archive refresh: %d %s", code, body)
	}
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: product.store, Lua: engine, RequestTimeout: 10 * time.Second}
	core, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := service.BatchingOptions{StoreNames: []string{"primary"}, MaxWait: time.Second, MaxOperations: 2}
	server, err := service.NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	archivePut := &sink.PutOperation{Document: sinkDocument(`{"value":1}`), Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	archiveAction := &sink.WriteOperation_Put{Put: archivePut}
	archiveOp := &sink.WriteOperation{Address: archive.sinkAddress("archive"), Action: archiveAction}
	archiveRequest := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{archiveOp}}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) return incoming end`)}
	mutation := &sink.MergeOperation{IncomingDocument: sinkDocument(`{"value":2}`), LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE}
	productAction := &sink.WriteOperation_Merge{Merge: mutation}
	productOp := &sink.WriteOperation{Address: product.sinkAddress("product"), Action: productAction}
	productRequest := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.WriteOperation{productOp}}
	requests := []*sink.WriteRequest{productRequest, archiveRequest}
	errors := make(chan error, len(requests))
	for _, req := range requests {
		go func() {
			response, err := server.Write(t.Context(), req)
			if err == nil && (len(response.Results) != 1 || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED) {
				err = fmt.Errorf("unexpected write response: %v", response)
			}
			errors <- err
		}()
	}
	for range requests {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	// Visible completion must remain a real search guarantee for the product.
	code, body = product.request(t, http.MethodGet, "/"+product.index+"/_count", nil)
	var count struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &count); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || count.Count != 1 {
		t.Fatalf("product not visible: %d %s", code, body)
	}
	// The archive is persisted even though its background refresh is disabled.
	code, body = archive.request(t, http.MethodGet, "/"+archive.index+"/_doc/archive", nil)
	if code != http.StatusOK {
		t.Fatalf("archive not persisted: %d %s", code, body)
	}

	archiveDeleteOp := &sink.DeleteOperation{Address: archive.sinkAddress("archive")}
	archiveDelete := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{archiveDeleteOp}}
	productDeleteOp := &sink.DeleteOperation{Address: product.sinkAddress("product")}
	productDelete := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.DeleteOperation{productDeleteOp}}
	deletes := []*sink.DeleteRequest{productDelete, archiveDelete}
	for _, req := range deletes {
		go func() {
			response, err := server.Delete(t.Context(), req)
			if err == nil && (len(response.Results) != 1 || response.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED) {
				err = fmt.Errorf("unexpected delete response: %v", response)
			}
			errors <- err
		}()
	}
	for range deletes {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	code, body = product.request(t, http.MethodGet, "/"+product.index+"/_count", nil)
	if err := json.Unmarshal(body, &count); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || count.Count != 0 {
		t.Fatalf("product delete not visible: %d %s", code, body)
	}
	code, body = archive.request(t, http.MethodGet, "/"+archive.index+"/_doc/archive", nil)
	if code != http.StatusNotFound {
		t.Fatalf("archive delete not applied: %d %s", code, body)
	}
}
