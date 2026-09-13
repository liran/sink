package search

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestReplaceReadsOnlyMetadataAndSplitsOversizedMetadata(t *testing.T) {
	for _, largeMetadata := range []bool{false, true} {
		name := "large_source"
		if largeMetadata {
			name = "large_metadata"
		}
		t.Run(name, func(t *testing.T) {
			var reads, bulks atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/_mget":
					reads.Add(1)
					var body multiGetRequest
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					docs := make([]map[string]any, 0, len(body.Documents))
					for _, reference := range body.Documents {
						result := map[string]any{"found": true, "_seq_no": 1, "_primary_term": 1}
						if reference.Source == nil || *reference.Source {
							t.Error("Replace requested the old source")
							result["_source"] = map[string]string{"value": strings.Repeat("x", 2048)}
						}
						if largeMetadata {
							result["_routing"] = strings.Repeat("r", 600)
						}
						docs = append(docs, result)
					}
					response := map[string]any{"docs": docs}
					if err := json.NewEncoder(w).Encode(response); err != nil {
						t.Error(err)
					}
				case "/_bulk":
					bulks.Add(1)
					payload, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					count := bytes.Count(payload, []byte{'\n'}) / 2
					items := make([]map[string]any, count)
					for i := range items {
						items[i] = map[string]any{"index": map[string]any{"status": 200, "_seq_no": 2, "_primary_term": 1}}
					}
					response := map[string]any{"items": items}
					if err := json.NewEncoder(w).Encode(response); err != nil {
						t.Error(err)
					}
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			})
			endpoint := httptest.NewServer(handler)
			defer endpoint.Close()
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{endpoint.URL}, MaxResponseSize: 1024}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			document := testDocument(`{"value":"new"}`)
			condition := storage.Precondition{Kind: storage.PreconditionRecordExists}
			a := storage.WriteOperation{Address: testAddress("a"), Document: document, Precondition: condition}
			b := storage.WriteOperation{Address: testAddress("b"), Document: document, Precondition: condition}
			req := storage.WriteRequest{Operations: []storage.WriteOperation{a, b}}
			response, err := store.Write(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range response.Results {
				if result.Status != storage.WriteStatusApplied {
					t.Fatal(result)
				}
			}
			wantReads := int32(1)
			if largeMetadata {
				wantReads = 3
			}
			if reads.Load() != wantReads || bulks.Load() != 1 {
				t.Fatalf("reads=%d bulks=%d", reads.Load(), bulks.Load())
			}
		})
	}
}
