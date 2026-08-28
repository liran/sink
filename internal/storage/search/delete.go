package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/liran/sink/internal/storage"
)

type deleteWork struct {
	resultIndex int
	document    resolvedDocument
}

func (s *Store) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{Results: make([]storage.DeleteResult, len(req.Operations))}
	works := make([]deleteWork, 0, len(req.Operations))
	for index, operation := range req.Operations {
		document, err := s.resolve(operation.Address)
		if err != nil {
			setDeleteError(&response.Results[index], err)
			continue
		}
		work := deleteWork{resultIndex: index, document: document}
		works = append(works, work)
	}
	if len(works) == 0 {
		return response, nil
	}
	payload, err := buildDeleteBulk(works)
	if err != nil {
		for _, work := range works {
			setDeleteError(&response.Results[work.resultIndex], err)
		}
		return response, nil
	}
	items, err := s.performBulk(ctx, payload, len(works))
	if err != nil {
		for _, work := range works {
			setDeleteError(&response.Results[work.resultIndex], err)
		}
		return response, nil
	}
	for index, item := range items {
		result := &response.Results[works[index].resultIndex]
		if (item.Status >= 200 && item.Status < 300) || item.Status == 404 {
			result.Status = storage.DeleteStatusApplied
			continue
		}
		if item.Error != nil {
			setDeleteError(result, classifySearchStatus(item.Status, item.Error))
			continue
		}
		err := fmt.Errorf("search bulk delete returned HTTP %d", item.Status)
		setDeleteError(result, classifySearchStatus(item.Status, err))
	}
	return response, nil
}

func buildDeleteBulk(works []deleteWork) ([]byte, error) {
	var payload bytes.Buffer
	for _, work := range works {
		metadata := bulkActionMetadata{Index: work.document.index, ID: work.document.id}
		action := make(map[string]bulkActionMetadata, 1)
		action["delete"] = metadata
		encodedAction, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("encode search bulk delete action: %w", err)
		}
		payload.Write(encodedAction)
		payload.WriteByte('\n')
	}
	return payload.Bytes(), nil
}

func setDeleteError(result *storage.DeleteResult, err error) {
	result.Status = storage.DeleteStatusFailed
	result.Err = err
}
