package search

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestNativeExecuteRejectsLifecycleChangesBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{server.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	commands := []storage.NativeRequest{
		{Method: "PUT", Path: "/products"},
		{Method: "DELETE", Path: "/products"},
		{Method: "DELETE", Path: "/products,other/"},
		{Method: "DELETE", Path: "/_all"},
		{Method: "DELETE", Path: "/*"},
		{Method: "POST", Path: "/_aliases", Payload: []byte(`{"actions":[{"add":{"index":"new","alias":"live"}},{"remove_index":{"index":"old"}}]}`)},
		{Method: "POST", Path: "/%5faliases/"},
		{Method: "PUT", Path: "/products/_alias/live"},
		{Method: "DELETE", Path: "/products/_aliases/live"},
		{Method: "POST", Path: "/products/_rollover/next"},
		{Method: "POST", Path: "/products/_close"},
		{Method: "POST", Path: "/products/%5fopen"},
		{Method: "POST", Path: "/products/_clone/copy"},
		{Method: "POST", Path: "/products/_split/copy"},
		{Method: "POST", Path: "/products/_shrink/copy"},
		{Method: "POST", Path: "/_snapshot/repository/snapshot/_restore"},
		{Method: "POST", Path: "/_snapshot/repository/snapshot/_mount"},
		{Method: "PUT", Path: "/_data_stream/logs"},
		{Method: "DELETE", Path: "/_data_stream/logs"},
		{Method: "POST", Path: "/_data_stream/_modify"},
		{Method: "PUT", Path: "/_data_stream/logs/_lifecycle"},
		{Method: "PUT", Path: "/products/_settings"},
		{Method: "PUT", Path: "/_settings"},
		{Method: "PUT", Path: "/_cluster/settings"},
		{Method: "PUT", Path: "/_template/automatic-delete"},
		{Method: "PUT", Path: "/_index_template/automatic-delete"},
		{Method: "PUT", Path: "/_component_template/automatic-delete"},
		{Method: "PUT", Path: "/_ilm/policy/automatic-delete"},
		{Method: "POST", Path: "/products/_ilm/retry"},
		{Method: "POST", Path: "/_ilm/start"},
		{Method: "POST", Path: "/_plugins/_ism/add/products"},
		{Method: "POST", Path: "/_opendistro/_ism/change_policy/products"},
		{Method: "PUT", Path: "/products/_ccr/follow"},
		{Method: "PUT", Path: "/_plugins/_replication/products/_start"},
		{Method: "PATCH", Path: "/_plugins/future/endpoint/"},
		{Method: "POST", Path: "/products/_search/_aliases"},
	}
	for _, command := range commands {
		t.Run(command.Method+command.Path, func(t *testing.T) {
			command.Store = "search"
			command.ContentType = ContentTypeJSON
			command.MaxBytes = 4096
			response, err := store.Execute(t.Context(), command)
			code, retryable := storage.ErrorDetails(err)
			if code != storage.ErrorCodeInvalidArgument || retryable || len(response.Payload) != 0 || calls.Load() != 0 {
				t.Fatalf("lifecycle request reached backend: calls=%d response=%+v error=%v", calls.Load(), response, err)
			}
		})
	}
}
