//go:build integration

package search_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSearchBatchingIsolatesIndependentVisibleDatasets(t *testing.T) {
	slow := newIntegrationFixture(t)
	fast := newIntegrationFixture(t)
	code, body := slow.request(t, http.MethodPut, "/"+slow.index+"/_settings", []byte(`{"index":{"refresh_interval":"-1"}}`))
	if code != http.StatusOK {
		t.Fatalf("disable refresh: %d %s", code, body)
	}
	code, body = fast.request(t, http.MethodPut, "/"+fast.index+"/_settings", []byte(`{"index":{"refresh_interval":"100ms"}}`))
	if code != http.StatusOK {
		t.Fatalf("set fast refresh: %d %s", code, body)
	}
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{Storage: slow.store, Lua: engine, StoreNames: []string{"primary"}, RequestTimeout: 10 * time.Second}
	core, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := service.BatchingOptions{StoreNames: []string{"primary"}, MaxWait: 200 * time.Millisecond, MaxOperations: 2}
	server, err := service.NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	listener := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = listener.Close() })
	transport := grpc.NewServer()
	sink.RegisterSinkServer(transport, server)
	t.Cleanup(transport.Stop)
	go func() { _ = transport.Serve(listener) }()
	dial := func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }
	connection, err := grpc.NewClient("passthrough:///completion-test", grpc.WithContextDialer(dial), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := sink.NewSinkClient(connection)
	type outcome struct {
		response *sink.WriteResponse
		err      error
	}
	results := []chan outcome{make(chan outcome, 1), make(chan outcome, 1)}
	for index, fixture := range []*integrationFixture{slow, fast} {
		put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: sinkDocument(`{"value":1}`)}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Address: fixture.sinkAddress("visible"), Action: action}
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.WriteOperation{operation}}
		go func() {
			response, err := client.Write(t.Context(), request)
			completed := outcome{response: response, err: err}
			results[index] <- completed
		}()
	}
	select {
	case completed := <-results[1]:
		if completed.err != nil || len(completed.response.GetResults()) != 1 || completed.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("fast index write: %+v", completed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fast index waited for unrelated index refresh")
	}
	// Real-time GET confirms that the slow write has reached OpenSearch/ES.
	deadline := time.Now().Add(2 * time.Second)
	for {
		code, body = slow.request(t, http.MethodGet, "/"+slow.index+"/_doc/visible", nil)
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow write did not reach backend: %d %s", code, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case completed := <-results[0]:
		t.Fatalf("slow index acknowledged before refresh: %+v", completed)
	default:
	}
	code, body = slow.request(t, http.MethodPost, "/"+slow.index+"/_refresh", nil)
	if code != http.StatusOK {
		t.Fatalf("manual refresh: %d %s", code, body)
	}
	select {
	case completed := <-results[0]:
		if completed.err != nil || len(completed.response.GetResults()) != 1 || completed.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("slow index write: %+v", completed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("visible write did not finish after refresh")
	}
	for _, fixture := range []*integrationFixture{slow, fast} {
		code, body = fixture.request(t, http.MethodGet, "/"+fixture.index+"/_count", nil)
		var count struct {
			Count int `json:"count"`
		}
		if err := json.Unmarshal(body, &count); err != nil {
			t.Fatal(err)
		}
		if code != http.StatusOK || count.Count != 1 {
			t.Fatalf("acknowledged visible write is not searchable: %d %s", code, body)
		}
	}
}

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
	mutation := &sink.MergeOperation{IncomingDocument: sinkDocument(`{"value":2}`), LuaProgram: program}
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
