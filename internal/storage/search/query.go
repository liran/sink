package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/liran/sink/internal/storage"
)

func pageOptions(req storage.NativeRequest) (requestOptions, map[string]json.RawMessage, error) {
	opts, err := nativeOptions(req)
	if err != nil {
		return opts, nil, err
	}
	if !strings.HasSuffix(opts.path, "/_search") || (opts.method != http.MethodGet && opts.method != http.MethodPost) {
		return opts, nil, errors.New("Query and Count require a _search request")
	}
	mediaType, _, _ := mime.ParseMediaType(opts.contentType)
	if opts.contentType != "" && mediaType != ContentTypeJSON && !strings.HasSuffix(mediaType, "+json") {
		return opts, nil, errors.New("Query and Count require a JSON content_type")
	}
	for _, name := range []string{"source", "filter_path", "scroll", "scroll_id", "search_after", "pit"} {
		if opts.query.Has(name) {
			return opts, nil, fmt.Errorf("Query and Count do not support parameter %q", name)
		}
	}
	body := make(map[string]json.RawMessage)
	if len(opts.payload) > 0 {
		if err := json.Unmarshal(opts.payload, &body); err != nil || body == nil {
			return opts, nil, errors.New("Query and Count require a JSON object body")
		}
	}
	for _, name := range []string{"search_after", "pit"} {
		if _, exists := body[name]; exists {
			return opts, nil, fmt.Errorf("Query and Count do not support %q; use Execute for manual pagination", name)
		}
	}
	delete(body, "from")
	delete(body, "size")
	opts.query.Del("from")
	opts.query.Del("size")
	opts.method = http.MethodPost
	opts.contentType = ContentTypeJSON
	opts.headers.Set("Accept", ContentTypeJSON)
	return opts, body, nil
}

func (s *Store) performQuery(ctx context.Context, opts requestOptions) (scanPage, error) {
	var page scanPage
	response, err := s.perform(ctx, opts)
	if err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return page, storage.ResourceExhaustedError(err)
		}
		return page, err
	}
	if response.statusCode < 200 || response.statusCode >= 300 {
		return page, responseError(s.driver, response)
	}
	if err := json.Unmarshal(response.body, &page); err != nil {
		return page, fmt.Errorf("decode search query: %w", err)
	}
	if page.ScrollID != "" || page.Hits == nil || page.Hits.Hits == nil || page.TimedOut || page.TerminatedEarly || page.Shards.Failed != 0 {
		return page, errors.New("search query returned incomplete results, failed shards or an unexpected cursor")
	}
	return page, nil
}

func (s *Store) Query(ctx context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	var empty storage.QueryResponse
	if req.Request.Store != s.logicalStore {
		return empty, storage.InvalidArgumentError(errors.New("search store does not match request"))
	}
	if err := req.Validate(); err != nil {
		return empty, err
	}
	opts, body, err := pageOptions(req.Request)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	body["from"] = json.RawMessage(fmt.Sprint(req.Offset))
	body["size"] = json.RawMessage(fmt.Sprint(req.PageSize + 1))
	if len(req.Sort) > 0 {
		sort := make([]map[string]string, 0, len(req.Sort))
		for _, field := range req.Sort {
			direction := "asc"
			if field.Descending {
				direction = "desc"
			}
			item := map[string]string{field.Field: direction}
			sort = append(sort, item)
		}
		body["sort"], err = json.Marshal(sort)
		if err != nil {
			return empty, storage.InvalidArgumentError(err)
		}
		opts.query.Del("sort")
	}
	if req.Projection != nil {
		body["_source"] = json.RawMessage("true")
		if len(req.Projection.Fields) > 0 {
			mode := "includes"
			if req.Projection.Exclude {
				mode = "excludes"
			}
			projection := map[string][]string{mode: req.Projection.Fields}
			body["_source"], err = json.Marshal(projection)
			if err != nil {
				return empty, storage.InvalidArgumentError(err)
			}
		}
		for _, name := range []string{"_source", "_source_includes", "_source_excludes"} {
			opts.query.Del(name)
		}
	}
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	page, err := s.performQuery(ctx, opts)
	if err != nil {
		return empty, err
	}
	if len(page.Hits.Hits) > req.PageSize+1 {
		return empty, errors.New("search query exceeded its requested result count")
	}
	result := storage.QueryResponse{HasMore: len(page.Hits.Hits) > req.PageSize}
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	for _, hit := range page.Hits.Hits[:min(len(page.Hits.Hits), req.PageSize)] {
		if err := budget.Reserve(len(hit)); err != nil {
			return empty, err
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: hit}
		result.Documents = append(result.Documents, document)
	}
	return result, nil
}

func (s *Store) Count(ctx context.Context, req storage.NativeRequest) (uint64, error) {
	if req.Store != s.logicalStore {
		return 0, storage.InvalidArgumentError(errors.New("search store does not match request"))
	}
	opts, body, err := pageOptions(req)
	if err != nil {
		return 0, storage.InvalidArgumentError(err)
	}
	// Count needs matching-document totals, without computing hit presentation,
	// aggregations or collapsing the hits returned by a normal search.
	for _, name := range []string{"sort", "_source", "fields", "docvalue_fields", "stored_fields", "highlight", "script_fields", "aggs", "aggregations", "collapse", "rescore"} {
		delete(body, name)
	}
	for _, name := range []string{"sort", "_source", "_source_includes", "_source_excludes", "stored_fields", "docvalue_fields"} {
		opts.query.Del(name)
	}
	body["size"] = json.RawMessage("0")
	body["track_total_hits"] = json.RawMessage("true")
	opts.query.Del("track_total_hits")
	opts.query.Set("rest_total_hits_as_int", "false")
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return 0, storage.InvalidArgumentError(err)
	}
	page, err := s.performQuery(ctx, opts)
	if err != nil {
		return 0, err
	}
	var total struct {
		Value    *int64 `json:"value"`
		Relation string `json:"relation"`
	}
	err = json.Unmarshal(page.Hits.Total, &total)
	if err != nil || total.Value == nil || *total.Value < 0 || total.Relation != "eq" || len(page.Hits.Hits) != 0 {
		return 0, errors.New("search count omitted an exact nonnegative total")
	}
	return uint64(*total.Value), nil
}
