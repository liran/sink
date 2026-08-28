package merge_test

import (
	"context"
	"testing"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
)

func TestJSONMergePatchMergesNestedObjectsAndDeletesNulls(t *testing.T) {
	merger := merge.JSONMergePatch{}
	current := storage.Document{
		ContentType: "application/json",
		Data:        []byte(`{"name":"sink","nested":{"keep":1,"replace":1},"remove":true}`),
	}
	incoming := storage.Document{
		ContentType: "application/json",
		Data:        []byte(`{"nested":{"replace":2,"add":3},"remove":null}`),
	}
	request := merge.Request{Current: &current, Incoming: incoming}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"name":"sink","nested":{"add":3,"keep":1,"replace":2}}`
	if string(result.Document.Data) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.Data, want)
	}
}

func TestRegisterBuiltinsRegistersJSONMergePatch(t *testing.T) {
	registry := merge.NewRegistry()
	if err := merge.RegisterBuiltins(registry); err != nil {
		t.Fatalf("RegisterBuiltins() error = %v", err)
	}
	profile := merge.Profile{Name: merge.JSONMergePatchProfileName, Version: 1}
	if _, err := registry.Resolve(profile); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
}

func TestJSONMergePatchRejectsTrailingContent(t *testing.T) {
	merger := merge.JSONMergePatch{}
	incoming := storage.Document{
		ContentType: "application/json",
		Data:        []byte(`{} {}`),
	}
	request := merge.Request{Incoming: incoming}
	if _, err := merger.Merge(context.Background(), request); err == nil {
		t.Fatal("Merge() error = nil")
	}
}
