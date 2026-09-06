package search

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink/internal/storage"
)

func TestImmediateVisibleWritesDoNotWaitForPeriodicRefresh(t *testing.T) {
	var writes atomic.Int64
	var deletes atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("refresh") != "true" {
			// Model an index whose periodic refresh cannot fit in the request budget.
			<-r.Context().Done()
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", ContentTypeJSON)
		if bytes.Contains(body, []byte(`"delete"`)) {
			deletes.Add(1)
			_, _ = w.Write([]byte(`{"items":[{"delete":{"status":200}}]}`))
			return
		}
		sequence := writes.Add(1)
		_, _ = fmt.Fprintf(w, `{"items":[{"index":{"status":200,"_seq_no":%d,"_primary_term":1}}]}`, sequence)
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}, VisibleRefresh: VisibleRefreshImmediate}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	operations := make([]storage.WriteOperation, 0, 40)
	for index := range 40 {
		document := testDocument(fmt.Sprintf(`{"value":%d}`, index))
		operation := storage.WriteOperation{Address: testAddress("shared-seller"), Document: document}
		operations = append(operations, operation)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request := storage.WriteRequest{Operations: operations, WaitUntilVisible: true}
	response, err := store.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range response.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatalf("write %d = %+v", index, result)
		}
	}
	deleteOperation := storage.DeleteOperation{Address: testAddress("shared-seller")}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{deleteOperation}, WaitUntilVisible: true}
	deleted, err := store.Delete(ctx, deleteRequest)
	if err != nil || deleted.Results[0].Status != storage.DeleteStatusApplied {
		t.Fatalf("Delete() = %+v, %v", deleted, err)
	}
	if writes.Load() != 40 || deletes.Load() != 1 {
		t.Fatalf("writes=%d deletes=%d", writes.Load(), deletes.Load())
	}
}

func TestNewRejectsInvalidVisibleRefresh(t *testing.T) {
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{"http://localhost:9200"}, VisibleRefresh: "false"}
	if _, err := New(opts); err == nil {
		t.Fatal("New() accepted a mode that does not guarantee visibility")
	}
}
