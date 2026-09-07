//go:build integration

package search_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/search"
)

func TestSearchBulkAcceptsMultilineJSONAlongsideCompactDocuments(t *testing.T) {
	fixture := newIntegrationFixture(t)
	pretty := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(" {\r\n \"counter\": 9007199254740993,\r\n \"text\": \"中文\\nnext\",\r\n \"nested\": {\"items\": [1, null, 2]}\r\n } \n")}
	compact := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"counter":1}`)}
	first := storage.WriteOperation{Address: fixture.address("pretty"), Document: pretty}
	second := storage.WriteOperation{Address: fixture.address("compact"), Document: compact}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{first, second}, WaitUntilVisible: true}
	response, err := fixture.store.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatal(result)
		}
	}
	readOperation := storage.ReadOperation{Address: first.Address}
	read := storage.ReadRequest{Operations: []storage.ReadOperation{readOperation}}
	stored, err := fixture.store.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Counter int64  `json:"counter"`
		Text    string `json:"text"`
	}
	if err := json.Unmarshal(stored.Results[0].Document.Payload, &document); err != nil {
		t.Fatal(err)
	}
	if document.Counter != 9007199254740993 || document.Text != "中文\nnext" {
		t.Fatalf("JSON semantics changed: %+v", document)
	}
}

type replaceRaceTransport struct {
	competitor *search.Store
	operation  storage.WriteOperation
	bulks      atomic.Int32
	reads      atomic.Int32
}

func (r *replaceRaceTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/_mget" {
		r.reads.Add(1)
	}
	if request.URL.Path == "/_bulk" && r.bulks.Add(1) == 1 {
		// Change the real record after Replace's snapshot but before its first CAS.
		mutation := storage.WriteRequest{Operations: []storage.WriteOperation{r.operation}}
		response, err := r.competitor.Write(request.Context(), mutation)
		if err != nil {
			return nil, err
		}
		if response.Results[0].Status != storage.WriteStatusApplied {
			return nil, fmt.Errorf("contending write: %+v", response.Results[0])
		}
	}
	return http.DefaultTransport.RoundTrip(request)
}

func TestSearchReplaceRebasesARealConcurrentRevisionChange(t *testing.T) {
	fixture := newIntegrationFixture(t)
	document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"counter":1}`)}
	operation := storage.WriteOperation{Address: fixture.address("existing"), Document: document}
	seed := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	response, err := fixture.store.Write(t.Context(), seed)
	if err != nil || response.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("seed: %v, %v", response, err)
	}
	race := &replaceRaceTransport{competitor: fixture.store, operation: operation}
	client := &http.Client{Transport: race, Timeout: 10 * time.Second}
	opts := search.Options{Driver: fixture.driver, Store: "primary", Endpoints: []string{fixture.endpoint}, HTTPClient: client}
	store, err := search.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	operation.Document.Payload = []byte(`{"counter":3}`)
	operation.Precondition.Kind = storage.PreconditionRecordExists
	request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}, WaitUntilVisible: true}
	response, err = store.Write(t.Context(), request)
	if err != nil || response.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("replace: %v, %v", response, err)
	}
	if race.bulks.Load() != 2 || race.reads.Load() != 2 {
		t.Fatalf("unexpected retry work: %d reads, %d bulks", race.reads.Load(), race.bulks.Load())
	}
	code, body := fixture.request(t, http.MethodGet, "/"+fixture.index+"/_doc/existing", nil)
	var stored struct {
		Source struct {
			Counter int `json:"counter"`
		} `json:"_source"`
	}
	if err := json.Unmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || stored.Source.Counter != 3 {
		t.Fatalf("final replace state: %d %s", code, body)
	}
}

func TestSearchAppliedCallsPassAnEarlierBatchWaitingForRefresh(t *testing.T) {
	product := newIntegrationFixture(t)
	archive := newIntegrationFixture(t)
	settings := []byte(`{"index":{"refresh_interval":"-1"}}`)
	for _, fixture := range []*integrationFixture{product, archive} {
		code, body := fixture.request(t, http.MethodPut, "/"+fixture.index+"/_settings", settings)
		if code != http.StatusOK {
			t.Fatalf("disable refresh: %d %s", code, body)
		}
	}
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: product.store, Lua: lua, RequestTimeout: 10 * time.Second, StoreNames: []string{"primary"}, MaxStoreRequests: 2, MaxInFlightRequests: 2}
	core, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	batchOpts := service.BatchingOptions{StoreNames: []string{"primary"}, MaxOperations: 1, MaxWait: time.Millisecond}
	server, err := service.NewBatchingServer(core, batchOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for _, method := range []string{"Write", "Delete"} {
		submit := func(fixture *integrationFixture, mode sink.CompletionMode) <-chan error {
			done := make(chan error, 1)
			go func() {
				if method == "Write" {
					put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: sinkDocument(`{"counter":1}`)}
					action := &sink.WriteOperation_Put{Put: put}
					operation := &sink.WriteOperation{Address: fixture.sinkAddress("record"), Action: action}
					request := &sink.WriteRequest{CompletionMode: mode, Operations: []*sink.WriteOperation{operation}}
					response, callErr := server.Write(ctx, request)
					if callErr == nil && response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
						callErr = fmt.Errorf("write: %v", response)
					}
					done <- callErr
				} else {
					operation := &sink.DeleteOperation{Address: fixture.sinkAddress("record")}
					request := &sink.DeleteRequest{CompletionMode: mode, Operations: []*sink.DeleteOperation{operation}}
					response, callErr := server.Delete(ctx, request)
					if callErr == nil && response.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
						callErr = fmt.Errorf("delete: %v", response)
					}
					done <- callErr
				}
			}()
			return done
		}
		visible := submit(product, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE)
		want := http.StatusOK
		if method == "Delete" {
			want = http.StatusNotFound
		}
		for {
			code, _ := product.request(t, http.MethodGet, "/"+product.index+"/_doc/record", nil)
			if code == want {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("visible request never applied")
			case <-time.After(10 * time.Millisecond):
			}
		}
		applied := submit(archive, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED)
		select {
		case err := <-applied:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("later applied batch blocked on refresh")
		}
		select {
		case err := <-visible:
			t.Fatalf("visible returned before refresh: %v", err)
		default:
		}
		code, body := product.request(t, http.MethodPost, "/"+product.index+"/_refresh", nil)
		if code != http.StatusOK {
			t.Fatalf("manual refresh: %d %s", code, body)
		}
		select {
		case err := <-visible:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("visible request did not finish after refresh")
		}
	}
}
