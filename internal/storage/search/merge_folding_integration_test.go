//go:build integration

package search_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
)

type foldingSearchStorage struct {
	storage.Storage
	reads, writes, documents int
	deletes                  int
	visible                  bool
}

func (s *foldingSearchStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	s.deletes += len(req.Operations)
	return s.Storage.Delete(ctx, req)
}

func (s *foldingSearchStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads += len(req.Operations)
	return s.Storage.Read(ctx, req)
}

func (s *foldingSearchStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes++
	s.documents += len(req.Operations)
	s.visible = req.WaitUntilVisible
	return s.Storage.Write(ctx, req)
}

func TestSearchMergeFoldingCommitsAndMakesFinalStateVisibleOnce(t *testing.T) {
	fixture := newIntegrationFixture(t)
	observed := &foldingSearchStorage{Storage: fixture.store}
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{Storage: observed, Lua: engine}
	server, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	const source = `return function(current, incoming)
        current = current or {counter=0}
        current.counter = current.counter + incoming.delta
        return current
    end`
	const operations = 32
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
	for range operations {
		program := &sink.LuaProgram{Source: []byte(source)}
		mutation := &sink.MergeOperation{IncomingDocument: sinkDocument(`{"delta":1}`), LuaProgram: program, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE}
		action := &sink.WriteOperation_Merge{Merge: mutation}
		operation := &sink.WriteOperation{Address: fixture.sinkAddress("folded"), Action: action}
		request.Operations = append(request.Operations, operation)
	}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED || result.OperationIndex != uint32(index) || result.Failure != nil {
			t.Fatalf("result %d: %v", index, result)
		}
		if !bytes.Equal(result.GetRevision().GetData(), response.Results[0].GetRevision().GetData()) {
			t.Fatal("operations do not share a commit revision")
		}
	}
	if observed.reads != 1 || observed.writes != 1 || observed.documents != 1 || !observed.visible {
		t.Fatalf("backend work: %+v", observed)
	}
	code, body := fixture.request(t, http.MethodGet, "/"+fixture.index+"/_search", []byte(`{"query":{"ids":{"values":["folded"]}}}`))
	if code != http.StatusOK {
		t.Fatalf("search status %d: %s", code, body)
	}
	var searchResult struct {
		Hits struct {
			Hits []struct {
				Source counterDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &searchResult); err != nil {
		t.Fatal(err)
	}
	if len(searchResult.Hits.Hits) != 1 || searchResult.Hits.Hits[0].Source.Counter != operations {
		t.Fatalf("final state not searchable after acknowledgement: %s", body)
	}
}
