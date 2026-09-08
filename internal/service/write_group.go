package service

import (
	"context"
	"errors"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
)

type writeGroupPreparation struct {
	group   writeGroup
	stored  storage.ReadResult
	results []*sink.WriteResult
}

func (s *Server) prepareWriteGroup(ctx context.Context, input writeGroupPreparation) (writeGroupCandidate, bool) {
	candidate := writeGroupCandidate{group: input.group}
	current := input.stored
	include := false
	for _, operation := range input.group.operations {
		// Every retry evaluates all operations against the new base, including
		// failures whose outcome depended on an uncommitted predecessor.
		result := input.results[operation.index]
		result.Status = sink.WriteStatus_WRITE_STATUS_UNSPECIFIED
		result.Revision = nil
		result.Failure = nil
		result.Document = nil
		var merged storage.WriteOperation
		var ok bool
		if operation.put != nil {
			merged, ok = prepareFoldedPut(operation, current, result)
		} else {
			merged, ok = s.prepareMergeOperation(ctx, operation, current, result)
		}
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

func prepareFoldedPut(operation parsedWrite, current storage.ReadResult, result *sink.WriteResult) (storage.WriteOperation, bool) {
	var candidate storage.WriteOperation
	if current.Status != storage.ReadStatusFound && current.Status != storage.ReadStatusNotFound {
		cause := current.Err
		if cause == nil {
			cause = errors.New("storage returned an invalid conditional write snapshot")
		}
		code, retryable := storageFailureDetails(cause)
		setWriteFailure(result, code, cause, retryable)
		return candidate, false
	}
	exists := current.Status == storage.ReadStatusFound
	condition := operation.put.precondition.Kind
	if (condition == storage.PreconditionRecordExists && !exists) ||
		(condition == storage.PreconditionRecordNotExists && exists) {
		failure := storage.WriteResult{Status: storage.WriteStatusPreconditionFailed}
		applyWriteResult(result, failure)
		return candidate, false
	}
	candidate.Address = operation.address
	candidate.Document = operation.put.document
	switch {
	case !exists:
		candidate.Precondition.Kind = storage.PreconditionRecordNotExists
	case len(current.Revision.Data) == 0:
		candidate.Precondition.Kind = storage.PreconditionRevisionAbsent
	default:
		candidate.Precondition.Kind = storage.PreconditionRevisionMatches
		candidate.Precondition.Revision = current.Revision
	}
	return candidate, true
}

func applyWriteGroupResult(group writeGroup, results []*sink.WriteResult, stored storage.WriteResult) {
	for _, operation := range group.operations {
		result := results[operation.index]
		result.Document = nil
		// Conditional and Lua failures become final only when their speculative
		// state commits. A failed commit leaves the entire chain unresolved.
		if stored.Status == storage.WriteStatusApplied && result.Failure != nil {
			continue
		}
		result.Failure = nil
		applyWriteResult(result, stored)
	}
}

type batchedWritePreparation struct {
	writeGroupPreparation
	budgets *requestBudgets
	inputs  []*storage.ReadBudget
	outputs []*storage.ReadBudget
}

func (s *Server) prepareBatchedWriteGroup(ctx context.Context, input batchedWritePreparation) (writeGroupCandidate, bool) {
	candidate := writeGroupCandidate{group: input.group}
	current := input.stored
	include := false
	operations := input.group.operations
	for start := 0; start < len(operations); {
		owner := input.budgets.owner(operations[start].index)
		end := start + 1
		for end < len(operations) && input.budgets.owner(operations[end].index) == owner {
			end++
		}
		part := writeGroup{operations: operations[start:end]}
		start = end
		if current.Status == storage.ReadStatusFound {
			if err := input.inputs[owner].Reserve(len(current.Document.Payload)); err != nil {
				failure := storage.WriteResult{Status: storage.WriteStatusFailed, Err: err}
				applyWriteGroupResult(part, input.results, failure)
				continue
			}
		}
		preparation := writeGroupPreparation{group: part, stored: current, results: input.results}
		prepared, ok := s.prepareWriteGroup(ctx, preparation)
		if !ok {
			continue
		}
		if err := input.outputs[owner].Reserve(len(prepared.operation.Document.Payload)); err != nil {
			failure := storage.WriteResult{Status: storage.WriteStatusFailed, Err: err}
			applyWriteGroupResult(part, input.results, failure)
			continue
		}
		if !include {
			candidate.operation = prepared.operation
			include = true
		} else {
			candidate.operation.Document = prepared.operation.Document
		}
		current.Status = storage.ReadStatusFound
		current.Document = prepared.operation.Document
	}
	return candidate, include
}
