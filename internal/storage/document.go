package storage

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

func CloneDocument(document Document) Document {
	cloned := Document{
		JSON:          bytes.Clone(document.JSON),
		DateTimePaths: append([]string(nil), document.DateTimePaths...),
	}
	return cloned
}

func DecodeDateTimeValues(document Document) (map[string]struct{}, error) {
	if len(document.DateTimePaths) == 0 {
		return make(map[string]struct{}), nil
	}
	decoder := json.NewDecoder(bytes.NewReader(document.JSON))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode JSON document: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode JSON document: unexpected trailing content")
	}
	return DateTimeValues(decoded, document.DateTimePaths)
}

func DateTimeValues(value any, paths []string) (map[string]struct{}, error) {
	values := make(map[string]struct{}, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, rawPath := range paths {
		if _, exists := seen[rawPath]; exists {
			return nil, fmt.Errorf("date-time path %q is duplicated", rawPath)
		}
		seen[rawPath] = struct{}{}
		pointer := jsontext.Pointer(rawPath)
		if !pointer.IsValid() {
			return nil, fmt.Errorf("date-time path %q is not a valid JSON Pointer", rawPath)
		}
		located, err := valueAtJSONPointer(value, pointer)
		if err != nil {
			return nil, fmt.Errorf("date-time path %q: %w", rawPath, err)
		}
		encoded, ok := located.(string)
		if !ok {
			return nil, fmt.Errorf("date-time path %q identifies %T, not a string", rawPath, located)
		}
		if _, err := time.Parse(time.RFC3339Nano, encoded); err != nil {
			return nil, fmt.Errorf("date-time path %q has an invalid RFC3339 value: %w", rawPath, err)
		}
		values[encoded] = struct{}{}
	}
	return values, nil
}

func valueAtJSONPointer(value any, pointer jsontext.Pointer) (any, error) {
	current := value
	for token := range pointer.Tokens() {
		switch typed := current.(type) {
		case map[string]any:
			next, exists := typed[token]
			if !exists {
				return nil, fmt.Errorf("object member %q does not exist", token)
			}
			current = next
		case []any:
			index, err := jsonArrayIndex(token, len(typed))
			if err != nil {
				return nil, err
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("cannot traverse %T with token %q", current, token)
		}
	}
	return current, nil
}

func jsonArrayIndex(token string, length int) (int, error) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	index, err := strconv.Atoi(token)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	if index >= length {
		return 0, fmt.Errorf("array index %d is out of bounds", index)
	}
	return index, nil
}
