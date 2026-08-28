package merge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/liran/sink/internal/storage"
)

const JSONMergePatchProfileName = "json-merge-patch"

const jsonContentType = "application/json"

type JSONMergePatch struct{}

func RegisterBuiltins(registry *Registry) error {
	if registry == nil {
		return errors.New("register built-in merge profiles: registry is required")
	}
	profile := Profile{Name: JSONMergePatchProfileName, Version: 1}
	merger := JSONMergePatch{}
	return registry.Register(profile, merger)
}

func (JSONMergePatch) Merge(_ context.Context, req Request) (Result, error) {
	var empty Result
	patch, err := decodeJSONObject("incoming", req.Incoming)
	if err != nil {
		return empty, err
	}
	current := make(map[string]any)
	if req.Current != nil {
		current, err = decodeJSONObject("current", *req.Current)
		if err != nil {
			return empty, err
		}
	}
	merged := mergeJSONObject(current, patch)
	encoded, err := json.Marshal(merged)
	if err != nil {
		return empty, fmt.Errorf("encode JSON merge patch result: %w", err)
	}
	document := storage.Document{ContentType: jsonContentType, Data: encoded}
	result := Result{Document: document}
	return result, nil
}

func decodeJSONObject(label string, document storage.Document) (map[string]any, error) {
	if document.ContentType != jsonContentType {
		return nil, fmt.Errorf("%s merge document requires content type %q", label, jsonContentType)
	}
	decoder := json.NewDecoder(bytes.NewReader(document.Data))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode %s JSON merge document: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode %s JSON merge document: unexpected trailing content", label)
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s JSON merge document must be an object", label)
	}
	return object, nil
}

func mergeJSONObject(current map[string]any, patch map[string]any) map[string]any {
	for key, patchValue := range patch {
		if patchValue == nil {
			delete(current, key)
			continue
		}
		patchObject, patchIsObject := patchValue.(map[string]any)
		if !patchIsObject {
			current[key] = patchValue
			continue
		}
		currentObject, currentIsObject := current[key].(map[string]any)
		if !currentIsObject {
			currentObject = make(map[string]any)
		}
		current[key] = mergeJSONObject(currentObject, patchObject)
	}
	return current
}
