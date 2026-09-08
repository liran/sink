package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liran/sink/internal/storage"
	"golang.org/x/net/http/httpguts"
)

func nativeOptions(req storage.NativeRequest) (requestOptions, error) {
	var opts requestOptions
	if req.Search == nil || req.MongoDB != nil {
		return opts, errors.New("search storage requires an HTTP command")
	}
	command := req.Search
	path := command.Path
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.ContainsAny(path, "?#\\\x00\r\n") {
		return opts, errors.New("native path must be an absolute endpoint path without a host, query, or fragment")
	}
	decoded, err := url.PathUnescape(path)
	if err != nil {
		return opts, fmt.Errorf("decode native path: %w", err)
	}
	if strings.HasPrefix(decoded, "//") || strings.ContainsAny(decoded, "\\\x00\r\n") {
		return opts, errors.New("invalid native endpoint path")
	}
	for _, part := range strings.Split(decoded, "/") {
		if part == "." || part == ".." {
			return opts, errors.New("native path cannot contain dot segments")
		}
	}
	if command.Method == "" || !httpguts.ValidHeaderFieldName(command.Method) {
		return opts, errors.New("native HTTP method must be a nonempty token")
	}
	query, err := url.ParseQuery(command.Query)
	if err != nil {
		return opts, fmt.Errorf("parse native query parameters: %w", err)
	}
	headers := make(http.Header)
	for name, values := range command.Headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return opts, errors.New("invalid HTTP header name")
		}
		switch strings.ToLower(name) {
		case "authorization", "proxy-authorization", "host", "connection", "proxy-connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade", "content-length":
			return opts, fmt.Errorf("request header %q is managed by Sink's transport", name)
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return opts, errors.New("invalid HTTP header value")
			}
			headers.Add(name, value)
		}
	}
	contentType := ContentTypeJSON
	if strings.HasSuffix(decoded, "/_msearch") || strings.HasSuffix(decoded, "/_bulk") {
		contentType = "application/x-ndjson"
	}
	opts = requestOptions{method: command.Method, path: decoded, rawPath: path, payload: command.Body,
		query: query, headers: headers, contentType: contentType, maxBytes: int64(req.MaxBytes), native: true}
	return opts, nil
}

func (s *Store) Execute(ctx context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	var empty storage.NativeResponse
	if req.Store != s.logicalStore {
		return empty, storage.InvalidArgumentError(errors.New("search store does not match request"))
	}
	opts, err := nativeOptions(req)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	response, err := s.perform(ctx, opts)
	if err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return empty, storage.ResourceExhaustedError(err)
		}
		return empty, err
	}
	result := storage.NativeResponse{ContentType: response.headers.Get("Content-Type"),
		Payload: response.body, StatusCode: response.statusCode, Headers: response.headers,
		Success: response.statusCode >= 200 && response.statusCode < 300}
	return result, nil
}

type scanPage struct {
	ScrollID        string    `json:"_scroll_id"`
	Hits            *scanHits `json:"hits"`
	TimedOut        bool      `json:"timed_out"`
	TerminatedEarly bool      `json:"terminated_early"`
	Shards          struct {
		Failed int `json:"failed"`
	} `json:"_shards"`
}

type scanHits struct {
	Hits []json.RawMessage `json:"hits"`
}

func (s *Store) Scan(ctx context.Context, req storage.ScanRequest, send func([]storage.Document) error) error {
	if req.BatchSize < 1 || req.BatchSize > 1000 {
		return storage.InvalidArgumentError(errors.New("scan batch size must be between 1 and 1000"))
	}
	if req.Request.Store != s.logicalStore {
		return storage.InvalidArgumentError(errors.New("search store does not match request"))
	}
	opts, err := nativeOptions(req.Request)
	if err != nil {
		return storage.InvalidArgumentError(err)
	}
	if !strings.HasSuffix(opts.path, "/_search") || (opts.method != http.MethodGet && opts.method != http.MethodPost) {
		return storage.InvalidArgumentError(errors.New("search Scan requires a _search request"))
	}
	if opts.query.Has("source") {
		return storage.InvalidArgumentError(errors.New("Scan requires the search body instead of the source parameter"))
	}
	if opts.query.Has("filter_path") {
		return storage.InvalidArgumentError(errors.New("Scan requires complete cursor and failure metadata; filter_path belongs to Execute"))
	}
	body := make(map[string]json.RawMessage)
	if len(opts.payload) > 0 {
		if err := json.Unmarshal(opts.payload, &body); err != nil || body == nil {
			return storage.InvalidArgumentError(errors.New("Scan requires a JSON object body"))
		}
	}
	if _, exists := body["search_after"]; exists {
		return storage.InvalidArgumentError(errors.New("Scan owns pagination; search_after belongs to Execute"))
	}
	delete(body, "from")
	body["size"] = json.RawMessage(fmt.Sprintf("%d", req.BatchSize))
	if _, exists := body["sort"]; !exists {
		body["sort"] = json.RawMessage(`["_doc"]`)
	}
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return err
	}
	opts.query.Del("from")
	opts.query.Del("size")
	opts.query.Set("scroll", "2m")
	opts.method = http.MethodPost
	opts.headers.Set("Accept", ContentTypeJSON)
	opts.headers.Set("Content-Type", ContentTypeJSON)
	scrollID := ""
	defer func() {
		if scrollID == "" {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		payload, _ := json.Marshal(map[string][]string{"scroll_id": {scrollID}})
		closeOptions := requestOptions{method: http.MethodDelete, path: "/_search/scroll", payload: payload,
			contentType: ContentTypeJSON, maxBytes: int64(req.Request.MaxBytes), native: true}
		_, _ = s.perform(cleanup, closeOptions)
	}()
	for {
		response, err := s.perform(ctx, opts)
		if err != nil {
			if errors.Is(err, errResponseTooLarge) {
				return storage.ResourceExhaustedError(err)
			}
			return err
		}
		if response.statusCode < 200 || response.statusCode >= 300 {
			return responseError(s.driver, response)
		}
		var page scanPage
		if err := json.Unmarshal(response.body, &page); err != nil {
			return fmt.Errorf("decode search scan page: %w", err)
		}
		if page.ScrollID != "" {
			scrollID = page.ScrollID
		}
		if page.Hits == nil || page.Hits.Hits == nil || page.TimedOut || page.TerminatedEarly || page.Shards.Failed != 0 {
			return errors.New("search scan returned incomplete results or failed shards")
		}
		if len(page.Hits.Hits) == 0 {
			return nil
		}
		if scrollID == "" {
			return errors.New("search scan response omitted its continuation cursor")
		}
		budget := storage.NewReadBudget(req.Request.MaxBytes)
		documents := make([]storage.Document, 0, len(page.Hits.Hits))
		for _, hit := range page.Hits.Hits {
			if err := budget.Reserve(len(hit)); err != nil {
				if len(documents) == 0 {
					return err
				}
				if err := send(documents); err != nil {
					return err
				}
				documents = make([]storage.Document, 0, req.BatchSize)
				budget = storage.NewReadBudget(req.Request.MaxBytes)
				if err := budget.Reserve(len(hit)); err != nil {
					return err
				}
			}
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: hit}
			documents = append(documents, document)
			if len(documents) == req.BatchSize {
				if err := send(documents); err != nil {
					return err
				}
				documents = make([]storage.Document, 0, req.BatchSize)
				budget = storage.NewReadBudget(req.Request.MaxBytes)
			}
		}
		if len(documents) > 0 {
			if err := send(documents); err != nil {
				return err
			}
		}
		payload, _ := json.Marshal(map[string]string{"scroll": "2m", "scroll_id": scrollID})
		opts = requestOptions{method: http.MethodPost, path: "/_search/scroll", payload: payload,
			contentType: ContentTypeJSON, maxBytes: int64(req.Request.MaxBytes), native: true}
	}
}
