//go:build integration

package search_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestSearchScanShrinksLargeResponses(t *testing.T) {
	fixture := newIntegrationFixture(t)
	seed := storage.WriteRequest{WaitUntilVisible: true}
	for index := range 9 {
		document := jsonStorageDocument(fmt.Sprintf(`{"number":%d,"padding":"%s"}`, index, strings.Repeat("x", 50<<10)))
		operation := storage.WriteOperation{Address: fixture.address(fmt.Sprint(index)), Document: document}
		seed.Operations = append(seed.Operations, operation)
	}
	written, err := fixture.store.Write(t.Context(), seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatal(result)
		}
	}
	command := storage.NativeRequest{Store: "primary", Method: "POST", Path: "/" + fixture.index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"sort":["number"]}`), MaxBytes: 64 << 10}
	request := storage.ScanRequest{Request: command, BatchSize: 16}
	for index := range 9 {
		page, err := fixture.store.Scan(t.Context(), request)
		if err != nil || len(page.Documents) != 1 {
			t.Fatalf("page %d: documents=%d, %v", index, len(page.Documents), err)
		}
		var hit struct {
			Source struct {
				Number int `json:"number"`
			} `json:"_source"`
		}
		if err := json.Unmarshal(page.Documents[0].Payload, &hit); err != nil {
			t.Fatal(err)
		}
		if hit.Source.Number != index {
			t.Fatalf("page %d returned record %d", index, hit.Source.Number)
		}
		if (len(page.NextCursor) != 0) != (index < 8) {
			t.Fatalf("wrong cursor on page %d", index)
		}
		request.Cursor = page.NextCursor
	}
}
