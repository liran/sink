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
	"time"

	"github.com/liran/sink/internal/storage"
)

var errResponseTooLarge = errors.New("search response exceeds configured byte limit")

type requestOptions struct {
	method      string
	path        string
	contentType string
	payload     []byte
	query       url.Values
	retrySafe   bool
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
	opts := requestOptions{method: http.MethodGet, path: "/", retrySafe: true}
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
	attempts := 1
	if opts.retrySafe {
		attempts = len(s.endpoints)
	}
	var lastErr error
	for range attempts {
		state, endpoint := s.endpoint(opts.path)
		endpoint.RawQuery = opts.query.Encode()
		response, err := s.performOnce(ctx, opts, endpoint)
		if err != nil {
			state.retryAfter.Store(time.Now().Add(defaultEndpointCooldown).UnixNano())
			lastErr = err
			if opts.retrySafe {
				continue
			}
			return empty, err
		}
		if retryableSearchStatus(response.statusCode) {
			state.retryAfter.Store(time.Now().Add(defaultEndpointCooldown).UnixNano())
			lastErr = responseError(s.driver, response)
			if opts.retrySafe {
				continue
			}
		}
		return response, nil
	}
	return empty, lastErr
}

func (s *Store) performOnce(ctx context.Context, opts requestOptions, endpoint *url.URL) (apiResponse, error) {
	var empty apiResponse
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
		return empty, storage.BackendError(err)
	}
	defer httpResponse.Body.Close()
	limited := io.LimitReader(httpResponse.Body, s.maxResponseSize+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return empty, storage.BackendError(err)
	}
	if int64(len(body)) > s.maxResponseSize {
		return empty, fmt.Errorf("%w: %s maximum is %d bytes", errResponseTooLarge, s.driver, s.maxResponseSize)
	}
	response := apiResponse{statusCode: httpResponse.StatusCode, body: body}
	return response, nil
}

func (s *Store) endpoint(requestPath string) (*endpointState, *url.URL) {
	start := s.nextEndpoint.Add(1) - 1
	now := time.Now().UnixNano()
	selected := s.endpoints[start%uint64(len(s.endpoints))]
	for offset := range len(s.endpoints) {
		position := (start + uint64(offset)) % uint64(len(s.endpoints))
		candidate := s.endpoints[position]
		if candidate.retryAfter.Load() <= now {
			selected = candidate
			break
		}
	}
	endpoint := *selected.value
	endpoint.Path = strings.TrimRight(selected.value.Path, "/") + requestPath
	endpoint.RawPath = ""
	return selected, &endpoint
}

func responseError(driver Driver, response apiResponse) error {
	detail := decodeResponseError(response.body)
	var cause error
	if detail != nil {
		cause = fmt.Errorf("%s request returned HTTP %d: %s", driver, response.statusCode, detail.Error())
	} else {
		cause = fmt.Errorf("%s request returned HTTP %d", driver, response.statusCode)
	}
	var structured *errorDetail
	if errors.As(detail, &structured) && isRetryableSearchError(structured) {
		return storage.BackendError(cause)
	}
	switch response.statusCode {
	case http.StatusRequestTimeout, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return storage.BackendError(cause)
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		return storage.ResourceExhaustedError(cause)
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return storage.InvalidArgumentError(cause)
	default:
		return cause
	}
}

func classifySearchError(detail *errorDetail) error {
	if isRetryableSearchError(detail) {
		return storage.BackendError(detail)
	}
	return detail
}

func isRetryableSearchError(detail *errorDetail) bool {
	for detail != nil {
		switch detail.Type {
		case "no_shard_available_action_exception", "unavailable_shards_exception":
			return true
		}
		detail = detail.CausedBy
	}
	return false
}

func retryableSearchStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
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
	return &detail
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
