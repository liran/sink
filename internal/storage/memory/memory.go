// Package memory provides an in-memory Storage implementation for tests and
// local development.
package memory

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/liran/sink/internal/storage"
)

type record struct {
	document storage.Document
	revision storage.Revision
}

type Store struct {
	mu           sync.RWMutex
	records      map[string]record
	nextRevision uint64
}

func New() *Store {
	store := &Store{
		records: make(map[string]record),
	}
	return store
}

func (s *Store) Ping(context.Context) error {
	return nil
}

type SeedRequest struct {
	Address  storage.Address
	Document storage.Document
	Revision storage.Revision
}

// Seed inserts or replaces a record without generating a revision. It exists
// to model legacy records in tests.
func (s *Store) Seed(req SeedRequest) {
	s.mu.Lock()
	s.records[req.Address.RoutingKey()] = record{
		document: cloneDocument(req.Document),
		revision: cloneRevision(req.Revision),
	}
	s.mu.Unlock()
}

func (s *Store) Read(_ context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	if req.Budget == nil {
		req.Budget = storage.NewReadBudget(storage.DefaultMaxReadBytes)
	}
	response := storage.ReadResponse{
		Results: make([]storage.ReadResult, len(req.Operations)),
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for index, operation := range req.Operations {
		stored, exists := s.records[operation.Address.RoutingKey()]
		if !exists {
			response.Results[index].Status = storage.ReadStatusNotFound
			continue
		}
		if err := req.Budget.Reserve(len(stored.document.Payload)); err != nil {
			response.Results[index] = storage.ReadResult{Status: storage.ReadStatusFailed, Err: err}
			continue
		}
		response.Results[index] = storage.ReadResult{
			Status:   storage.ReadStatusFound,
			Document: cloneDocument(stored.document),
			Revision: cloneRevision(stored.revision),
		}
	}
	return response, nil
}

func (s *Store) Write(_ context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{
		Results: make([]storage.WriteResult, len(req.Operations)),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for index, operation := range req.Operations {
		key := operation.Address.RoutingKey()
		stored, exists := s.records[key]
		matches, err := preconditionMatches(operation.Precondition, stored, exists)
		if err != nil {
			response.Results[index] = storage.WriteResult{
				Status: storage.WriteStatusFailed,
				Err:    err,
			}
			continue
		}
		if !matches {
			response.Results[index].Status = storage.WriteStatusPreconditionFailed
			continue
		}

		revision := s.newRevisionLocked()
		s.records[key] = record{
			document: cloneDocument(operation.Document),
			revision: revision,
		}
		response.Results[index] = storage.WriteResult{
			Status:   storage.WriteStatusApplied,
			Revision: cloneRevision(revision),
		}
	}
	return response, nil
}

func (s *Store) Delete(_ context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{
		Results: make([]storage.DeleteResult, len(req.Operations)),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for index, operation := range req.Operations {
		delete(s.records, operation.Address.RoutingKey())
		response.Results[index].Status = storage.DeleteStatusApplied
	}
	return response, nil
}

func (s *Store) newRevisionLocked() storage.Revision {
	s.nextRevision++
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, s.nextRevision)
	revision := storage.Revision{Data: data}
	return revision
}

func preconditionMatches(condition storage.Precondition, stored record, exists bool) (bool, error) {
	switch condition.Kind {
	case storage.PreconditionNone:
		return true, nil
	case storage.PreconditionRecordExists:
		return exists, nil
	case storage.PreconditionRecordNotExists:
		return !exists, nil
	case storage.PreconditionRevisionMatches:
		return exists && bytes.Equal(stored.revision.Data, condition.Revision.Data), nil
	case storage.PreconditionRevisionAbsent:
		return exists && len(stored.revision.Data) == 0, nil
	default:
		return false, fmt.Errorf("unsupported precondition kind %d", condition.Kind)
	}
}

func cloneDocument(document storage.Document) storage.Document {
	return storage.CloneDocument(document)
}

func cloneRevision(revision storage.Revision) storage.Revision {
	cloned := storage.Revision{
		Data: bytes.Clone(revision.Data),
	}
	return cloned
}
