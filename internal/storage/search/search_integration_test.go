//go:build integration

package search_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/search"
)

const (
	searchTestDriver   = "SINK_SEARCH_TEST_DRIVER"
	searchTestEndpoint = "SINK_SEARCH_TEST_ENDPOINT"
)

type integrationFixture struct {
	endpoint string
	index    string
	client   *http.Client
	store    *search.Store
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	driver := search.Driver(strings.TrimSpace(os.Getenv(searchTestDriver)))
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(searchTestEndpoint)), "/")
	if driver == "" || endpoint == "" {
		t.Skipf("%s and %s must be set", searchTestDriver, searchTestEndpoint)
	}
	if driver != search.DriverElasticsearch && driver != search.DriverOpenSearch {
		t.Fatalf("%s = %q", searchTestDriver, driver)
	}

	index := "sink-existing-records-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	client := &http.Client{Timeout: 30 * time.Second}
	opts := search.Options{
		Driver:     driver,
		Endpoints:  []string{endpoint},
		Store:      "primary",
		HTTPClient: client,
	}
	store, err := search.New(opts)
	if err != nil {
		t.Fatalf("search.New() error = %v", err)
	}
	if err := store.Ping(t.Context()); err != nil {
		t.Fatalf("store.Ping() error = %v", err)
	}
	fixture := &integrationFixture{
		endpoint: endpoint,
		index:    index,
		client:   client,
		store:    store,
	}
	settings := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
	status, body := fixture.request(t, http.MethodPut, "/"+index, settings)
	if status < 200 || status >= 300 {
		t.Fatalf("create test index returned HTTP %d: %s", status, body)
	}
	t.Cleanup(func() {
		cleanupStatus, _ := fixture.request(t, http.MethodDelete, "/"+index, nil)
		if cleanupStatus != http.StatusOK && cleanupStatus != http.StatusNotFound {
			t.Errorf("delete test index returned HTTP %d", cleanupStatus)
		}
	})
	return fixture
}

func (f *integrationFixture) request(t *testing.T, method string, path string, body []byte) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, f.endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", search.ContentTypeJSON)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	return response.StatusCode, responseBody
}

func TestSearchStorageLifecycleAndLegacyDocuments(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	first := fixture.address("first")
	second := fixture.address("second")
	createOperations := []storage.WriteOperation{
		{
			Address:  first,
			Document: jsonStorageDocument(`{"value":"one"}`),
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordNotExists,
			},
		},
		{
			Address:  second,
			Document: jsonStorageDocument(`{"value":"two"}`),
			Precondition: storage.Precondition{
				Kind: storage.PreconditionRecordNotExists,
			},
		},
	}
	createRequest := storage.WriteRequest{Operations: createOperations}
	created, err := fixture.store.Write(ctx, createRequest)
	if err != nil {
		t.Fatalf("Write(create batch) error = %v", err)
	}
	for index, result := range created.Results {
		if result.Status != storage.WriteStatusApplied || len(result.Revision.Data) != 16 {
			t.Fatalf("Write(create batch) result[%d] = %#v", index, result)
		}
	}

	readOperations := []storage.ReadOperation{
		{Address: second},
		{Address: fixture.address("missing")},
		{Address: first},
		{Address: fixture.datasetAddress(fixture.index+"-missing", "missing")},
	}
	readRequest := storage.ReadRequest{Operations: readOperations}
	read, err := fixture.store.Read(ctx, readRequest)
	if err != nil {
		t.Fatalf("Read(batch) error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusFound ||
		read.Results[1].Status != storage.ReadStatusNotFound ||
		read.Results[2].Status != storage.ReadStatusFound ||
		read.Results[3].Status != storage.ReadStatusNotFound {
		t.Fatalf(
			"Read(batch) statuses = %v, %v, %v, %v",
			read.Results[0].Status,
			read.Results[1].Status,
			read.Results[2].Status,
			read.Results[3].Status,
		)
	}

	replaceOperation := storage.WriteOperation{
		Address:  first,
		Document: jsonStorageDocument(`{"value":"updated"}`),
		Precondition: storage.Precondition{
			Kind:     storage.PreconditionRevisionMatches,
			Revision: created.Results[0].Revision,
		},
	}
	replaceRequest := storage.WriteRequest{Operations: []storage.WriteOperation{replaceOperation}}
	replaced, err := fixture.store.Write(ctx, replaceRequest)
	if err != nil {
		t.Fatalf("Write(revision match) error = %v", err)
	}
	if replaced.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(revision match) result = %#v", replaced.Results[0])
	}
	stale, err := fixture.store.Write(ctx, replaceRequest)
	if err != nil {
		t.Fatalf("Write(stale revision) error = %v", err)
	}
	if stale.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write(stale revision) status = %v", stale.Results[0].Status)
	}

	createConflict := storage.WriteOperation{
		Address:  first,
		Document: jsonStorageDocument(`{"value":"duplicate"}`),
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRecordNotExists,
		},
	}
	existsReplacement := storage.WriteOperation{
		Address:  second,
		Document: jsonStorageDocument(`{"value":"replaced"}`),
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRecordExists,
		},
	}
	missingReplacement := storage.WriteOperation{
		Address:  fixture.address("missing"),
		Document: jsonStorageDocument(`{"value":"not-written"}`),
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRecordExists,
		},
	}
	conditionOperations := []storage.WriteOperation{createConflict, existsReplacement, missingReplacement}
	conditionRequest := storage.WriteRequest{Operations: conditionOperations}
	conditions, err := fixture.store.Write(ctx, conditionRequest)
	if err != nil {
		t.Fatalf("Write(conditions) error = %v", err)
	}
	if conditions.Results[0].Status != storage.WriteStatusPreconditionFailed ||
		conditions.Results[1].Status != storage.WriteStatusApplied ||
		conditions.Results[2].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write(conditions) statuses = %v, %v, %v", conditions.Results[0].Status, conditions.Results[1].Status, conditions.Results[2].Status)
	}

	ordered := fixture.address("ordered")
	orderedOperations := []storage.WriteOperation{
		{Address: ordered, Document: jsonStorageDocument(`{"step":1}`)},
		{Address: ordered, Document: jsonStorageDocument(`{"step":2}`)},
	}
	orderedRequest := storage.WriteRequest{Operations: orderedOperations}
	orderedWrite, err := fixture.store.Write(ctx, orderedRequest)
	if err != nil {
		t.Fatalf("Write(ordered) error = %v", err)
	}
	if orderedWrite.Results[0].Status != storage.WriteStatusApplied || orderedWrite.Results[1].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(ordered) statuses = %v, %v", orderedWrite.Results[0].Status, orderedWrite.Results[1].Status)
	}
	orderedReadOperation := storage.ReadOperation{Address: ordered}
	orderedReadRequest := storage.ReadRequest{Operations: []storage.ReadOperation{orderedReadOperation}}
	orderedRead, err := fixture.store.Read(ctx, orderedReadRequest)
	if err != nil {
		t.Fatalf("Read(ordered) error = %v", err)
	}
	assertJSONEqual(t, orderedRead.Results[0].Document.Data, []byte(`{"step":2}`))

	legacyBody := []byte(`{"name":"legacy","tags":["old"]}`)
	legacyPath := "/" + fixture.index + "/_doc/legacy?refresh=true"
	status, responseBody := fixture.request(t, http.MethodPut, legacyPath, legacyBody)
	if status < 200 || status >= 300 {
		t.Fatalf("write legacy document returned HTTP %d: %s", status, responseBody)
	}
	legacyOperation := storage.ReadOperation{Address: fixture.address("legacy")}
	legacyRequest := storage.ReadRequest{Operations: []storage.ReadOperation{legacyOperation}}
	legacyRead, err := fixture.store.Read(ctx, legacyRequest)
	if err != nil {
		t.Fatalf("Read(legacy) error = %v", err)
	}
	if legacyRead.Results[0].Status != storage.ReadStatusFound || len(legacyRead.Results[0].Revision.Data) != 16 {
		t.Fatalf("Read(legacy) result = %#v", legacyRead.Results[0])
	}
	assertJSONEqual(t, legacyRead.Results[0].Document.Data, legacyBody)

	deleteOperations := []storage.DeleteOperation{
		{Address: first},
		{Address: fixture.address("absent")},
	}
	deleteRequest := storage.DeleteRequest{Operations: deleteOperations}
	deleted, err := fixture.store.Delete(ctx, deleteRequest)
	if err != nil {
		t.Fatalf("Delete(batch) error = %v", err)
	}
	for index, result := range deleted.Results {
		if result.Status != storage.DeleteStatusApplied {
			t.Fatalf("Delete(batch) result[%d] = %#v", index, result)
		}
	}
	deletedReadOperation := storage.ReadOperation{Address: first}
	deletedReadRequest := storage.ReadRequest{Operations: []storage.ReadOperation{deletedReadOperation}}
	deletedRead, err := fixture.store.Read(ctx, deletedReadRequest)
	if err != nil {
		t.Fatalf("Read(deleted) error = %v", err)
	}
	if deletedRead.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read(deleted) status = %v", deletedRead.Results[0].Status)
	}
}

type counterDocument struct {
	Counter int64 `json:"counter"`
}

type deltaDocument struct {
	Delta int64 `json:"delta"`
}

type counterMerger struct{}

func (counterMerger) Merge(_ context.Context, req merge.Request) (merge.Result, error) {
	current := counterDocument{}
	if req.Current != nil {
		if err := json.Unmarshal(req.Current.Data, &current); err != nil {
			var empty merge.Result
			return empty, fmt.Errorf("decode current counter: %w", err)
		}
	}
	var incoming deltaDocument
	if err := json.Unmarshal(req.Incoming.Data, &incoming); err != nil {
		var empty merge.Result
		return empty, fmt.Errorf("decode counter delta: %w", err)
	}
	current.Counter += incoming.Delta
	encoded, err := json.Marshal(current)
	if err != nil {
		var empty merge.Result
		return empty, fmt.Errorf("encode counter: %w", err)
	}
	document := storage.Document{ContentType: search.ContentTypeJSON, Data: encoded}
	result := merge.Result{Document: document}
	return result, nil
}

func TestSearchServiceConcurrentMergeIsAtomic(t *testing.T) {
	fixture := newIntegrationFixture(t)
	registry := merge.NewRegistry()
	profile := merge.Profile{Name: "counter", Version: 1}
	merger := counterMerger{}
	if err := registry.Register(profile, merger); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	serverOptions := service.Options{
		Storage:          fixture.store,
		Merges:           registry,
		MaxMergeAttempts: 200,
	}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	address := fixture.sinkAddress("counter")
	initial := sinkDocument(`{"counter":0}`)
	put := &sink.PutOperation{Document: initial, Mode: sink.WriteMode_WRITE_MODE_CREATE}
	putAction := &sink.WriteOperation_Put{Put: put}
	putOperation := &sink.WriteOperation{Address: address, Action: putAction}
	putRequest := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     []*sink.WriteOperation{putOperation},
	}
	putResponse, err := server.Write(t.Context(), putRequest)
	if err != nil {
		t.Fatalf("Write(initial counter) error = %v", err)
	}
	if putResponse.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("Write(initial counter) result = %#v", putResponse.Results[0])
	}

	const mutations = 32
	incoming := sinkDocument(`{"delta":1}`)
	statuses := make(chan sink.WriteStatus, mutations)
	errorsChannel := make(chan error, mutations)
	var workers sync.WaitGroup
	workers.Add(mutations)
	for range mutations {
		go func() {
			defer workers.Done()
			mergeProfile := &sink.MergeProfile{Name: "counter", Version: 1}
			mergeOperation := &sink.MergeOperation{
				IncomingDocument:    incoming,
				Profile:             mergeProfile,
				MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
			}
			mergeAction := &sink.WriteOperation_Merge{Merge: mergeOperation}
			operation := &sink.WriteOperation{Address: address, Action: mergeAction}
			request := &sink.WriteRequest{
				CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
				Operations:     []*sink.WriteOperation{operation},
			}
			response, writeErr := server.Write(context.Background(), request)
			if writeErr != nil {
				errorsChannel <- writeErr
				return
			}
			if response.Results[0].Failure != nil {
				errorsChannel <- fmt.Errorf("merge failed: %s", response.Results[0].Failure.Message)
				return
			}
			statuses <- response.Results[0].Status
		}()
	}
	workers.Wait()
	close(statuses)
	close(errorsChannel)
	for writeErr := range errorsChannel {
		t.Errorf("Write(concurrent merge) error = %v", writeErr)
	}
	for status := range statuses {
		if status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Errorf("Write(concurrent merge) status = %v", status)
		}
	}
	if t.Failed() {
		return
	}

	readOperation := storage.ReadOperation{Address: fixture.address("counter")}
	readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{readOperation}}
	read, err := fixture.store.Read(t.Context(), readRequest)
	if err != nil {
		t.Fatalf("Read(final counter) error = %v", err)
	}
	var final counterDocument
	if err := json.Unmarshal(read.Results[0].Document.Data, &final); err != nil {
		t.Fatalf("json.Unmarshal(final counter) error = %v", err)
	}
	if final.Counter != mutations {
		t.Fatalf("final counter = %d, want %d", final.Counter, mutations)
	}
}

func (f *integrationFixture) address(key string) storage.Address {
	return f.datasetAddress(f.index, key)
}

func (f *integrationFixture) datasetAddress(index string, key string) storage.Address {
	recordKey := storage.Key{Type: "string", Data: []byte(key)}
	address := storage.Address{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   index,
		Key:       recordKey,
	}
	return address
}

func (f *integrationFixture) sinkAddress(key string) *sink.RecordAddress {
	keyValue := &sink.RecordKey_StringValue{StringValue: key}
	recordKey := &sink.RecordKey{Kind: keyValue}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   f.index,
		Key:       recordKey,
	}
	return address
}

func jsonStorageDocument(raw string) storage.Document {
	document := storage.Document{ContentType: search.ContentTypeJSON, Data: []byte(raw)}
	return document
}

func sinkDocument(raw string) *sink.Document {
	document := &sink.Document{ContentType: search.ContentTypeJSON, Data: []byte(raw)}
	return document
}

func assertJSONEqual(t *testing.T, actual []byte, expected []byte) {
	t.Helper()
	var actualValue any
	if err := json.Unmarshal(actual, &actualValue); err != nil {
		t.Fatalf("decode actual JSON: %v", err)
	}
	var expectedValue any
	if err := json.Unmarshal(expected, &expectedValue); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	actualJSON, err := json.Marshal(actualValue)
	if err != nil {
		t.Fatalf("encode actual JSON: %v", err)
	}
	expectedJSON, err := json.Marshal(expectedValue)
	if err != nil {
		t.Fatalf("encode expected JSON: %v", err)
	}
	if !bytes.Equal(actualJSON, expectedJSON) {
		t.Fatalf("JSON = %s, want %s", actualJSON, expectedJSON)
	}
}
