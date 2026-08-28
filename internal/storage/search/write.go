package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liran/sink/internal/storage"
)

type writeWork struct {
	resultIndex  int
	routingKey   string
	document     resolvedDocument
	source       []byte
	precondition storage.Precondition
}

type bulkActionMetadata struct {
	Index       string `json:"_index"`
	ID          string `json:"_id"`
	Sequence    *int64 `json:"if_seq_no,omitempty"`
	PrimaryTerm *int64 `json:"if_primary_term,omitempty"`
}

func (s *Store) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	prepared := make([]writeWork, 0, len(req.Operations))
	for index, operation := range req.Operations {
		work, err := s.prepareWrite(index, operation)
		if err != nil {
			setWriteError(&response.Results[index], err)
			continue
		}
		prepared = append(prepared, work)
	}

	waves := buildWriteWaves(prepared)
	for _, wave := range waves {
		s.writeWave(ctx, wave, response.Results)
	}
	return response, nil
}

func (s *Store) prepareWrite(index int, operation storage.WriteOperation) (writeWork, error) {
	var empty writeWork
	document, err := s.resolve(operation.Address)
	if err != nil {
		return empty, err
	}
	if err := validateJSONDocument(operation.Document); err != nil {
		return empty, err
	}
	if err := validatePrecondition(operation.Precondition); err != nil {
		return empty, err
	}
	work := writeWork{
		resultIndex:  index,
		routingKey:   operation.Address.RoutingKey(),
		document:     document,
		source:       bytes.Clone(operation.Document.Data),
		precondition: operation.Precondition,
	}
	return work, nil
}

func validateJSONDocument(document storage.Document) error {
	if document.ContentType != ContentTypeJSON {
		return fmt.Errorf("search storage requires content type %q", ContentTypeJSON)
	}
	trimmed := bytes.TrimSpace(document.Data)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
		return errors.New("search storage requires a valid JSON object")
	}
	return nil
}

func validatePrecondition(precondition storage.Precondition) error {
	switch precondition.Kind {
	case storage.PreconditionNone,
		storage.PreconditionRecordExists,
		storage.PreconditionRecordNotExists,
		storage.PreconditionRevisionAbsent:
		return nil
	case storage.PreconditionRevisionMatches:
		_, err := decodeRevision(precondition.Revision)
		return err
	default:
		return fmt.Errorf("unsupported precondition kind %d", precondition.Kind)
	}
}

func buildWriteWaves(prepared []writeWork) [][]writeWork {
	waves := make([][]writeWork, 0)
	occurrences := make(map[string]int)
	for _, work := range prepared {
		waveIndex := occurrences[work.routingKey]
		occurrences[work.routingKey]++
		for len(waves) <= waveIndex {
			waves = append(waves, nil)
		}
		waves[waveIndex] = append(waves[waveIndex], work)
	}
	return waves
}

func (s *Store) writeWave(ctx context.Context, wave []writeWork, results []storage.WriteResult) {
	eligible := make([]bool, len(wave))
	existsWorks := make([]readWork, 0)
	existsIndexes := make([]int, 0)
	for index, work := range wave {
		switch work.precondition.Kind {
		case storage.PreconditionRevisionAbsent:
			results[work.resultIndex].Status = storage.WriteStatusPreconditionFailed
		case storage.PreconditionRecordExists:
			read := readWork{resultIndex: index, document: work.document}
			existsWorks = append(existsWorks, read)
			existsIndexes = append(existsIndexes, index)
		default:
			eligible[index] = true
		}
	}
	if len(existsWorks) > 0 {
		documents, err := s.multiGet(ctx, existsWorks)
		if err != nil {
			for _, waveIndex := range existsIndexes {
				setWriteError(&results[wave[waveIndex].resultIndex], err)
			}
		} else {
			for resultIndex, document := range documents {
				waveIndex := existsIndexes[resultIndex]
				work := &wave[waveIndex]
				s.prepareExistingWrite(work, document, &results[work.resultIndex], &eligible[waveIndex])
			}
		}
	}

	ready := make([]writeWork, 0, len(wave))
	for index, work := range wave {
		if eligible[index] {
			ready = append(ready, work)
		}
	}
	if len(ready) == 0 {
		return
	}
	payload, err := buildWriteBulk(ready)
	if err != nil {
		for _, work := range ready {
			setWriteError(&results[work.resultIndex], err)
		}
		return
	}
	items, err := s.performBulk(ctx, payload, len(ready))
	if err != nil {
		for _, work := range ready {
			setWriteError(&results[work.resultIndex], err)
		}
		return
	}
	for index, item := range items {
		applyWriteItem(&results[ready[index].resultIndex], item)
	}
}

func (s *Store) prepareExistingWrite(
	work *writeWork,
	document multiGetDocument,
	result *storage.WriteResult,
	eligible *bool,
) {
	if document.Error != nil {
		if isIndexNotFound(document.Error) {
			result.Status = storage.WriteStatusPreconditionFailed
			return
		}
		setWriteError(result, document.Error)
		return
	}
	if !document.Found {
		result.Status = storage.WriteStatusPreconditionFailed
		return
	}
	if document.Sequence == nil || document.PrimaryTerm == nil {
		setWriteError(result, errors.New("search document has no sequence number or primary term"))
		return
	}
	revision, err := encodeRevision(*document.Sequence, *document.PrimaryTerm)
	if err != nil {
		setWriteError(result, err)
		return
	}
	work.precondition.Kind = storage.PreconditionRevisionMatches
	work.precondition.Revision = revision
	*eligible = true
}

func buildWriteBulk(works []writeWork) ([]byte, error) {
	var payload bytes.Buffer
	for _, work := range works {
		actionName := "index"
		metadata := bulkActionMetadata{Index: work.document.index, ID: work.document.id}
		switch work.precondition.Kind {
		case storage.PreconditionNone:
		case storage.PreconditionRecordNotExists:
			actionName = "create"
		case storage.PreconditionRevisionMatches:
			revision, err := decodeRevision(work.precondition.Revision)
			if err != nil {
				return nil, err
			}
			sequence := revision.sequenceNumber
			primaryTerm := revision.primaryTerm
			metadata.Sequence = &sequence
			metadata.PrimaryTerm = &primaryTerm
		default:
			return nil, fmt.Errorf("precondition kind %d cannot use a search bulk write", work.precondition.Kind)
		}
		action := make(map[string]bulkActionMetadata, 1)
		action[actionName] = metadata
		encodedAction, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("encode search bulk action: %w", err)
		}
		payload.Write(encodedAction)
		payload.WriteByte('\n')
		payload.Write(bytes.TrimSpace(work.source))
		payload.WriteByte('\n')
	}
	return payload.Bytes(), nil
}

func applyWriteItem(result *storage.WriteResult, item bulkItem) {
	if item.Status == 409 {
		result.Status = storage.WriteStatusPreconditionFailed
		return
	}
	if item.Status < 200 || item.Status >= 300 || item.Error != nil {
		if item.Error != nil {
			setWriteError(result, item.Error)
			return
		}
		setWriteError(result, fmt.Errorf("search bulk write returned HTTP %d", item.Status))
		return
	}
	if item.Sequence == nil || item.PrimaryTerm == nil {
		setWriteError(result, errors.New("search bulk write returned no sequence number or primary term"))
		return
	}
	revision, err := encodeRevision(*item.Sequence, *item.PrimaryTerm)
	if err != nil {
		setWriteError(result, err)
		return
	}
	result.Status = storage.WriteStatusApplied
	result.Revision = revision
}

func setWriteError(result *storage.WriteResult, err error) {
	result.Status = storage.WriteStatusFailed
	result.Err = err
}
