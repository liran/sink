package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liran/sink/internal/storage"
)

func scanSort(body map[string]json.RawMessage) (int, error) {
	var fields []json.RawMessage
	if err := json.Unmarshal(body["sort"], &fields); err != nil || len(fields) == 0 {
		return 0, errors.New("Scan requires a sort array ending in a unique immutable field with doc_values")
	}
	seen := make(map[string]bool)
	for _, raw := range fields {
		var name string
		if err := json.Unmarshal(raw, &name); err != nil {
			var field map[string]json.RawMessage
			if err := json.Unmarshal(raw, &field); err != nil || len(field) != 1 {
				return 0, errors.New("invalid Scan sort field")
			}
			for key, value := range field {
				name = key
				var direction string
				if err := json.Unmarshal(value, &direction); err != nil || (direction != "asc" && direction != "desc") {
					return 0, errors.New("Scan sort fields require asc or desc; sort scripts, modes and missing-value substitutions are unsupported")
				}
			}
		}
		if name == "" || strings.HasPrefix(name, "_") || seen[name] {
			return 0, errors.New("Scan requires distinct ordinary sort fields; _id, _doc, _shard_doc and _score cannot provide a live seek order")
		}
		seen[name] = true
	}
	return len(fields), nil
}

func scanSortValues(raw json.RawMessage, count int) ([]byte, error) {
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || len(values) != count {
		return nil, errors.New("Scan hit or cursor omitted its complete sort values")
	}
	for _, value := range values {
		var scalar any
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		if err := decoder.Decode(&scalar); err != nil {
			return nil, err
		}
		switch scalar.(type) {
		case string, json.Number, bool:
		default:
			return nil, errors.New("Scan sort fields must have non-null scalar values")
		}
	}
	return json.Marshal(values)
}

func (s *Store) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	var empty storage.ScanResponse
	if req.Request.Store != s.logicalStore {
		return empty, storage.InvalidArgumentError(errors.New("search store does not match request"))
	}
	seek, err := req.Resume()
	if err != nil {
		return empty, err
	}
	opts, body, err := pageOptions(req.Request)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	if opts.query.Has("sort") {
		return empty, storage.InvalidArgumentError(errors.New("Scan requires sort in the JSON body"))
	}
	for _, field := range []string{"aggs", "aggregations", "collapse", "rescore", "suggest", "terminate_after", "knn", "retriever"} {
		if _, exists := body[field]; exists {
			return empty, storage.InvalidArgumentError(fmt.Errorf("Scan does not support %q", field))
		}
	}
	if opts.query.Has("terminate_after") {
		return empty, storage.InvalidArgumentError(errors.New("Scan does not support terminate_after"))
	}
	count, err := scanSort(body)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	if len(seek.Position) != 0 {
		position, err := scanSortValues(seek.Position, count)
		if err != nil {
			return empty, storage.InvalidArgumentError(err)
		}
		body["search_after"] = position
	}
	body["size"] = json.RawMessage(fmt.Sprint(req.BatchSize + 1))
	body["track_total_hits"] = json.RawMessage("false")
	opts.query.Del("track_total_hits")
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	page, err := s.performQuery(ctx, opts)
	if err != nil {
		return empty, err
	}
	if len(page.Hits.Hits) > req.BatchSize+1 {
		return empty, errors.New("search Scan exceeded its requested result count")
	}
	documents := make([]storage.Document, 0, req.BatchSize)
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	previous := seek.Position
	var position []byte
	more := false
	for _, hit := range page.Hits.Hits {
		var metadata struct {
			Sort json.RawMessage `json:"sort"`
		}
		if err := json.Unmarshal(hit, &metadata); err != nil {
			return empty, err
		}
		next, err := scanSortValues(metadata.Sort, count)
		if err != nil {
			return empty, err
		}
		if bytes.Equal(previous, next) {
			return empty, errors.New("Scan sort is not unique; add a unique immutable tie-breaker field")
		}
		previous = next
		if len(documents) == req.BatchSize {
			more = true
			break
		}
		if err := budget.Reserve(len(hit)); err != nil {
			if len(documents) == 0 {
				return empty, err
			}
			more = true
			break
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: hit}
		documents = append(documents, document)
		position = next
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if !more {
		position = nil
	}
	return seek.Page(documents, position)
}
