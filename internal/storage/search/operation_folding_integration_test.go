//go:build integration

package search_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
)

func TestSearchFoldsPutsWithMergeAndRepeatedReadsDeletes(t *testing.T) {
	fixture := newIntegrationFixture(t)
	observed := &foldingSearchStorage{Storage: fixture.store}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: observed, Lua: lua}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	address := fixture.sinkAddress("folded-put")
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	modes := []sink.WriteMode{sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_REPLACE, sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_UPSERT}
	for index, mode := range modes {
		document := sinkDocument(fmt.Sprintf(`{"counter":%d}`, index+1))
		put := &sink.PutOperation{Mode: mode, Document: document}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Address: address, Action: action}
		request.Operations = append(request.Operations, operation)
	}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) current.counter=current.counter+incoming.delta return current end`)}
	mutation := &sink.MergeOperation{IncomingDocument: sinkDocument(`{"delta":1}`), LuaProgram: program}
	action := &sink.WriteOperation_Merge{Merge: mutation}
	operation := &sink.WriteOperation{Address: address, Action: action}
	request.Operations = append(request.Operations, operation)
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if index == 2 {
			if result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED {
				t.Fatalf("duplicate create: %v", result)
			}
			continue
		}
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatal(result)
		}
	}
	if observed.reads != 1 || observed.writes != 1 || observed.documents != 1 || !observed.visible {
		t.Fatalf("folded backend work: %+v", observed)
	}
	status, body := fixture.request(t, http.MethodGet, "/"+fixture.index+"/_search", []byte(`{"query":{"ids":{"values":["folded-put"]}}}`))
	var visible struct {
		Hits struct {
			Hits []struct {
				Source counterDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &visible); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || len(visible.Hits.Hits) != 1 || visible.Hits.Hits[0].Source.Counter != 5 {
		t.Fatalf("final folded state is not visible: %s", body)
	}
	read := &sink.ReadRequest{}
	remove := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	for range 16 {
		readOperation := &sink.ReadOperation{Address: address}
		deleteOperation := &sink.DeleteOperation{Address: address}
		read.Operations = append(read.Operations, readOperation)
		remove.Operations = append(remove.Operations, deleteOperation)
	}
	readResponse, err := server.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range readResponse.Results {
		if result.Status != sink.ReadStatus_READ_STATUS_FOUND || result.OperationIndex != uint32(index) {
			t.Fatal(result)
		}
	}
	deleted, err := server.Delete(t.Context(), remove)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range deleted.Results {
		if result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED || result.OperationIndex != uint32(index) {
			t.Fatal(result)
		}
	}
	if observed.reads != 2 || observed.deletes != 1 {
		t.Fatalf("repeated operations reached backend: %+v", observed)
	}
	status, body = fixture.request(t, http.MethodGet, "/"+fixture.index+"/_search", []byte(`{"query":{"ids":{"values":["folded-put"]}}}`))
	if err := json.Unmarshal(body, &visible); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || len(visible.Hits.Hits) != 0 {
		t.Fatalf("folded delete was acknowledged before visibility: %s", body)
	}
}
