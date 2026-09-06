package service

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
)

type mergeGroupPreparation struct {
	group   writeGroup
	stored  storage.ReadResult
	results []*sink.WriteResult
}

func (s *Server) prepareMergeGroup(ctx context.Context, input mergeGroupPreparation) (mergeGroupCandidate, bool) {
	candidate := mergeGroupCandidate{group: input.group}
	current := input.stored
	include := false
	for _, operation := range input.group.operations {
		// Every retry evaluates all operations against the new base, including
		// failures whose outcome depended on an uncommitted predecessor.
		result := input.results[operation.index]
		result.Status = sink.WriteStatus_WRITE_STATUS_UNSPECIFIED
		result.Revision = nil
		result.Failure = nil
		merged, ok := s.prepareMergeOperation(ctx, operation, current, result)
		if !ok {
			continue
		}
		if !include {
			candidate.operation = merged
			include = true
		} else {
			candidate.operation.Document = merged.Document
		}
		current.Status = storage.ReadStatusFound
		current.Document = merged.Document
	}
	return candidate, include
}

func applyMergeGroupResult(group writeGroup, results []*sink.WriteResult, stored storage.WriteResult) {
	for _, operation := range group.operations {
		result := results[operation.index]
		// Lua failures become final only when the state they were evaluated
		// against commits. A failed commit leaves the entire chain unresolved.
		if stored.Status == storage.WriteStatusApplied && result.Failure != nil {
			continue
		}
		result.Failure = nil
		applyWriteResult(result, stored)
	}
}
