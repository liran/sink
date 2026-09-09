package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/liran/sink/internal/storage"
	"golang.org/x/net/http/httpguts"
)

func nativeOptions(req storage.NativeRequest) (requestOptions, error) {
	var opts requestOptions
	if req.Namespace != "" {
		return opts, errors.New("search native requests do not use namespace; select resources with path")
	}
	command := req
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
		case "content-type":
			return opts, errors.New("set content_type directly, not in headers")
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
	if len(command.Payload) > 0 && command.ContentType == "" {
		return opts, errors.New("native payload requires content_type")
	}
	if command.ContentType != "" {
		_, _, err := mime.ParseMediaType(command.ContentType)
		if err != nil || !httpguts.ValidHeaderFieldValue(command.ContentType) {
			return opts, errors.New("invalid native content_type")
		}
	}
	opts = requestOptions{method: command.Method, path: decoded, rawPath: path, payload: command.Payload,
		query: query, headers: headers, contentType: command.ContentType, maxBytes: int64(req.MaxBytes), native: true}
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
	if err := validateNativeExecution(opts); err != nil {
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
	Hits  []json.RawMessage `json:"hits"`
	Total json.RawMessage   `json:"total"`
}
