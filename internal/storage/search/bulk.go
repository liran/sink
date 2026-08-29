package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type bulkItem struct {
	Status      int          `json:"status"`
	Sequence    *int64       `json:"_seq_no"`
	PrimaryTerm *int64       `json:"_primary_term"`
	Error       *errorDetail `json:"error"`
}

type bulkResponse struct {
	Errors bool                  `json:"errors"`
	Items  []map[string]bulkItem `json:"items"`
}

func (s *Store) performBulk(
	ctx context.Context,
	payload []byte,
	expectedItems int,
	waitUntilVisible bool,
) ([]bulkItem, error) {
	query := make(url.Values)
	if waitUntilVisible {
		query.Set("refresh", "wait_for")
	}
	opts := requestOptions{
		method:      http.MethodPost,
		path:        "/_bulk",
		contentType: "application/x-ndjson",
		payload:     payload,
		query:       query,
	}
	response, err := s.perform(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("execute search bulk request: %w", err)
	}
	if response.statusCode < 200 || response.statusCode >= 300 {
		return nil, responseError(s.driver, response)
	}
	var decoded bulkResponse
	if err := json.Unmarshal(response.body, &decoded); err != nil {
		return nil, fmt.Errorf("decode search bulk response: %w", err)
	}
	if len(decoded.Items) != expectedItems {
		return nil, fmt.Errorf("search returned %d bulk results for %d operations", len(decoded.Items), expectedItems)
	}
	items := make([]bulkItem, 0, len(decoded.Items))
	for index, action := range decoded.Items {
		if len(action) != 1 {
			return nil, fmt.Errorf("search bulk result %d contains %d actions", index, len(action))
		}
		for _, item := range action {
			items = append(items, item)
		}
	}
	return items, nil
}
