package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestSearchVisibleRefreshConfigurationReachesBackend(t *testing.T) {
	cases := []struct {
		name    string
		setting string
		query   string
	}{
		{name: "default", query: "wait_for"},
		{name: "scheduled", setting: "visible_refresh: wait_for", query: "wait_for"},
		{name: "immediate", setting: "visible_refresh: immediate", query: "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queries := make(chan string, 2)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				queries <- r.URL.Query().Get("refresh")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"items":[{"index":{"status":200,"_seq_no":1,"_primary_term":1}}]}`))
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			contents := fmt.Sprintf("storages:\n  - name: primary\n    driver: opensearch\n    search:\n      endpoints: [%s]\n      %s\n", backend.URL, tc.setting)
			filename := writeConfig(t, contents)
			loaded, err := loadConfig(filename)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := openSearchStorage(t.Context(), loaded.storages[0])
			if err != nil {
				t.Fatal(err)
			}
			key := storage.Key{Type: "string", Data: []byte("record")}
			address := storage.Address{Store: "primary", Namespace: "test", Dataset: "records", Key: key}
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"value":1}`)}
			operation := storage.WriteOperation{Address: address, Document: document}
			for _, visible := range []bool{true, false} {
				request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}, WaitUntilVisible: visible}
				response, err := opened.value.Write(t.Context(), request)
				if err != nil || response.Results[0].Status != storage.WriteStatusApplied {
					t.Fatalf("Write() = %+v, %v", response, err)
				}
				want := ""
				if visible {
					want = tc.query
				}
				if got := <-queries; got != want {
					t.Fatalf("refresh = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestLoadConfigRejectsInvalidVisibleRefresh(t *testing.T) {
	contents := "storages:\n  - name: primary\n    driver: opensearch\n    search:\n      endpoints: [http://localhost:9200]\n      visible_refresh: false\n"
	filename := writeConfig(t, contents)
	_, err := loadConfig(filename)
	if err == nil || !strings.Contains(err.Error(), "visible_refresh must be wait_for or immediate") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}
