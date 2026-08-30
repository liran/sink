package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/liran/sink/internal/storage"
)

type readWork struct {
	resultIndex int
	document    resolvedDocument
}

type multiGetDocumentReference struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

type multiGetRequest struct {
	Documents []multiGetDocumentReference `json:"docs"`
}

type multiGetDocument struct {
	Index       string          `json:"_index"`
	ID          string          `json:"_id"`
	Found       bool            `json:"found"`
	Source      json.RawMessage `json:"_source"`
	Sequence    *int64          `json:"_seq_no"`
	PrimaryTerm *int64          `json:"_primary_term"`
	Error       *errorDetail    `json:"error"`
}

type multiGetResponse struct {
	Documents []multiGetDocument `json:"docs"`
}

func (s *Store) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	response := storage.ReadResponse{Results: make([]storage.ReadResult, len(req.Operations))}
	works := make([]readWork, 0, len(req.Operations))
	for index, operation := range req.Operations {
		document, err := s.resolve(operation.Address)
		if err != nil {
			setReadError(&response.Results[index], err)
			continue
		}
		work := readWork{resultIndex: index, document: document}
		works = append(works, work)
	}
	if len(works) == 0 {
		return response, nil
	}

	documents, err := s.multiGet(ctx, works)
	if err != nil {
		for _, work := range works {
			setReadError(&response.Results[work.resultIndex], err)
		}
		return response, nil
	}
	for index, document := range documents {
		result := &response.Results[works[index].resultIndex]
		applyMultiGetDocument(result, document)
	}
	return response, nil
}

func (s *Store) multiGet(ctx context.Context, works []readWork) ([]multiGetDocument, error) {
	references := make([]multiGetDocumentReference, 0, len(works))
	for _, work := range works {
		reference := multiGetDocumentReference{Index: work.document.index, ID: work.document.id}
		references = append(references, reference)
	}
	requestBody := multiGetRequest{Documents: references}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("encode search multi-get request: %w", err)
	}
	opts := requestOptions{
		method:      http.MethodPost,
		path:        "/_mget",
		contentType: ContentTypeJSON,
		payload:     payload,
		retrySafe:   true,
	}
	response, err := s.perform(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("read search documents: %w", err)
	}
	if response.statusCode < 200 || response.statusCode >= 300 {
		return nil, responseError(s.driver, response)
	}
	var decoded multiGetResponse
	if err := json.Unmarshal(response.body, &decoded); err != nil {
		return nil, fmt.Errorf("decode search multi-get response: %w", err)
	}
	if len(decoded.Documents) != len(works) {
		return nil, fmt.Errorf("search returned %d multi-get results for %d operations", len(decoded.Documents), len(works))
	}
	return decoded.Documents, nil
}

func applyMultiGetDocument(result *storage.ReadResult, document multiGetDocument) {
	if document.Error != nil {
		if isIndexNotFound(document.Error) {
			result.Status = storage.ReadStatusNotFound
			return
		}
		setReadError(result, classifySearchError(document.Error))
		return
	}
	if !document.Found {
		result.Status = storage.ReadStatusNotFound
		return
	}
	if len(document.Source) == 0 || bytes.Equal(bytes.TrimSpace(document.Source), []byte("null")) {
		setReadError(result, errors.New("search document has no _source"))
		return
	}
	if document.Sequence == nil || document.PrimaryTerm == nil {
		setReadError(result, errors.New("search document has no sequence number or primary term"))
		return
	}
	revision, err := encodeRevision(*document.Sequence, *document.PrimaryTerm)
	if err != nil {
		setReadError(result, err)
		return
	}
	result.Status = storage.ReadStatusFound
	result.Document = storage.Document{JSON: bytes.Clone(document.Source)}
	result.Revision = revision
}

func setReadError(result *storage.ReadResult, err error) {
	result.Status = storage.ReadStatusFailed
	result.Err = err
}
