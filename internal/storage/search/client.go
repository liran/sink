package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type requestOptions struct {
	method      string
	path        string
	contentType string
	payload     []byte
}

type apiResponse struct {
	statusCode int
	body       []byte
}

type errorDetail struct {
	Type     string       `json:"type"`
	Reason   string       `json:"reason"`
	CausedBy *errorDetail `json:"caused_by"`
}

type errorEnvelope struct {
	Error  json.RawMessage `json:"error"`
	Status int             `json:"status"`
}

func (s *Store) Ping(ctx context.Context) error {
	opts := requestOptions{method: http.MethodGet, path: "/"}
	response, err := s.perform(ctx, opts)
	if err != nil {
		return fmt.Errorf("ping %s: %w", s.driver, err)
	}
	if response.statusCode < 200 || response.statusCode >= 300 {
		return responseError(s.driver, response)
	}
	return nil
}

func (s *Store) perform(ctx context.Context, opts requestOptions) (apiResponse, error) {
	var empty apiResponse
	endpoint := s.endpoint(opts.path)
	request, err := http.NewRequestWithContext(ctx, opts.method, endpoint.String(), bytes.NewReader(opts.payload))
	if err != nil {
		return empty, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "sink/"+string(s.driver))
	if opts.contentType != "" {
		request.Header.Set("Content-Type", opts.contentType)
	}
	if s.apiKey != "" {
		request.Header.Set("Authorization", "ApiKey "+s.apiKey)
	} else if s.username != "" {
		request.SetBasicAuth(s.username, s.password)
	}

	httpResponse, err := s.client.Do(request)
	if err != nil {
		return empty, err
	}
	defer httpResponse.Body.Close()
	limited := io.LimitReader(httpResponse.Body, s.maxResponseSize+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return empty, err
	}
	if int64(len(body)) > s.maxResponseSize {
		return empty, fmt.Errorf("%s response exceeds %d bytes", s.driver, s.maxResponseSize)
	}
	response := apiResponse{statusCode: httpResponse.StatusCode, body: body}
	return response, nil
}

func (s *Store) endpoint(requestPath string) *url.URL {
	position := s.nextEndpoint.Add(1) - 1
	selected := s.endpoints[position%uint64(len(s.endpoints))]
	endpoint := *selected
	endpoint.Path = strings.TrimRight(selected.Path, "/") + requestPath
	endpoint.RawPath = ""
	return &endpoint
}

func responseError(driver Driver, response apiResponse) error {
	detail := decodeResponseError(response.body)
	if detail != nil {
		return fmt.Errorf("%s request returned HTTP %d: %s", driver, response.statusCode, detail.Error())
	}
	return fmt.Errorf("%s request returned HTTP %d", driver, response.statusCode)
}

func decodeResponseError(body []byte) error {
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Error) == 0 {
		return nil
	}
	var message string
	if err := json.Unmarshal(envelope.Error, &message); err == nil && message != "" {
		return errors.New(message)
	}
	var detail errorDetail
	if err := json.Unmarshal(envelope.Error, &detail); err != nil || (detail.Type == "" && detail.Reason == "") {
		return nil
	}
	return detail
}

func (e errorDetail) Error() string {
	if e.Type == "" {
		return e.Reason
	}
	if e.Reason == "" {
		return e.Type
	}
	return e.Type + ": " + e.Reason
}

func isIndexNotFound(detail *errorDetail) bool {
	for detail != nil {
		if detail.Type == "index_not_found_exception" {
			return true
		}
		detail = detail.CausedBy
	}
	return false
}
