package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type parsedWrite struct {
	index     int
	address   storage.Address
	original  *sink.WriteOperation
	put       *parsedPut
	merge     *parsedMerge
	routingID string
}

type parsedPut struct {
	document     storage.Document
	precondition storage.Precondition
}

type parsedMerge struct {
	incoming            storage.Document
	merger              merge.Merger
	program             merge.Program
	missingDocumentMode sink.MissingDocumentMode
	observedAt          time.Time
}

type mergeCandidate struct {
	work      parsedWrite
	operation storage.WriteOperation
}

func (s *Server) parseWrite(index int, operation *sink.WriteOperation, programs luaPrograms) (parsedWrite, error) {
	parsed := parsedWrite{}
	if operation == nil {
		return parsed, errors.New("write operation is required")
	}
	address, err := convertAddress(operation.GetAddress())
	if err != nil {
		return parsed, err
	}
	parsed.index = index
	parsed.address = address
	parsed.original = operation
	parsed.routingID = address.RoutingKey()

	switch action := operation.GetAction().(type) {
	case *sink.WriteOperation_Put:
		put, parseErr := parsePut(action.Put)
		if parseErr != nil {
			return parsed, parseErr
		}
		parsed.put = &put
	case *sink.WriteOperation_Merge:
		mergeOperation, parseErr := s.parseMerge(action.Merge, programs)
		if parseErr != nil {
			return parsed, parseErr
		}
		parsed.merge = &mergeOperation
	default:
		return parsed, errors.New("write action is required")
	}
	return parsed, nil
}

func parsePut(operation *sink.PutOperation) (parsedPut, error) {
	parsed := parsedPut{}
	if operation == nil {
		return parsed, errors.New("put operation is required")
	}
	document, err := convertDocument(operation.GetDocument())
	if err != nil {
		return parsed, err
	}
	parsed.document = document

	switch operation.GetMode() {
	case sink.WriteMode_WRITE_MODE_CREATE:
		parsed.precondition.Kind = storage.PreconditionRecordNotExists
	case sink.WriteMode_WRITE_MODE_REPLACE:
		parsed.precondition.Kind = storage.PreconditionRecordExists
	case sink.WriteMode_WRITE_MODE_UPSERT:
		parsed.precondition.Kind = storage.PreconditionNone
	default:
		return parsed, errors.New("put operation has an invalid write mode")
	}
	return parsed, nil
}

func (s *Server) parseMerge(operation *sink.MergeOperation, programs luaPrograms) (parsedMerge, error) {
	parsed := parsedMerge{}
	if operation == nil {
		return parsed, errors.New("merge operation is required")
	}
	incoming, err := convertDocument(operation.GetIncomingDocument())
	if err != nil {
		return parsed, err
	}
	program := operation.GetLuaProgram()
	if program == nil {
		return parsed, errors.New("lua merge program is required")
	}
	if operation.GetMissingDocumentMode() != sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL &&
		operation.GetMissingDocumentMode() != sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE {
		return parsed, errors.New("merge operation has an invalid missing document mode")
	}

	mergeProgram, err := resolveLuaProgram(program, programs)
	if err != nil {
		return parsed, err
	}
	merger, err := s.lua.Compile(mergeProgram)
	if err != nil {
		return parsed, err
	}
	parsed.incoming = incoming
	parsed.merger = merger
	parsed.program = mergeProgram
	parsed.missingDocumentMode = operation.GetMissingDocumentMode()
	parsed.observedAt = time.Now().UTC()
	return parsed, nil
}

func resolveLuaProgram(program *sink.LuaProgram, programs luaPrograms) (merge.Program, error) {
	var resolved merge.Program
	if len(program.GetSource()) > 0 {
		digest := sha256.Sum256(program.GetSource())
		if len(program.GetSha256()) != 0 &&
			(len(program.GetSha256()) != sha256.Size || !bytes.Equal(program.GetSha256(), digest[:])) {
			return resolved, errors.New("lua program SHA-256 digest does not match source")
		}
		resolved.Source = bytes.Clone(program.GetSource())
		resolved.SHA256 = bytes.Clone(digest[:])
		return resolved, nil
	}
	if len(program.GetSha256()) != sha256.Size {
		return resolved, errors.New("lua program source or SHA-256 reference is required")
	}
	digest := [sha256.Size]byte(program.GetSha256())
	declared, ok := programs[digest]
	if !ok {
		return resolved, errors.New("lua program SHA-256 reference was not declared in the write request")
	}
	resolved.Source = bytes.Clone(declared.Source)
	resolved.SHA256 = bytes.Clone(declared.SHA256)
	return resolved, nil
}

func buildWriteWaves(operations []parsedWrite) [][]parsedWrite {
	waves := make([][]parsedWrite, 0)
	occurrences := make(map[string]int)
	for _, operation := range operations {
		waveIndex := occurrences[operation.routingID]
		occurrences[operation.routingID]++
		for len(waves) <= waveIndex {
			waves = append(waves, nil)
		}
		waves[waveIndex] = append(waves[waveIndex], operation)
	}
	return waves
}

func (s *Server) executeWriteWave(
	ctx context.Context,
	wave []parsedWrite,
	results []*sink.WriteResult,
) error {
	puts := make([]parsedWrite, 0, len(wave))
	merges := make([]parsedWrite, 0, len(wave))
	for _, operation := range wave {
		if operation.put != nil {
			puts = append(puts, operation)
			continue
		}
		merges = append(merges, operation)
	}
	if err := s.executePuts(ctx, puts, results); err != nil {
		return err
	}
	return s.executeMerges(ctx, merges, results)
}

func (s *Server) executePuts(
	ctx context.Context,
	operations []parsedWrite,
	results []*sink.WriteResult,
) error {
	if len(operations) == 0 {
		return nil
	}
	storageOperations := make([]storage.WriteOperation, 0, len(operations))
	for _, operation := range operations {
		storageOperation := storage.WriteOperation{
			Address:      operation.address,
			Document:     operation.put.document,
			Precondition: operation.put.precondition,
		}
		storageOperations = append(storageOperations, storageOperation)
	}
	request := storage.WriteRequest{Operations: storageOperations}
	response, err := s.storage.Write(ctx, request)
	if err != nil {
		return status.Errorf(codes.Unavailable, "write records: %v", err)
	}
	if len(response.Results) != len(operations) {
		return status.Error(codes.Internal, "storage returned an invalid write result count")
	}
	for index, stored := range response.Results {
		applyWriteResult(results[operations[index].index], stored)
	}
	return nil
}

func (s *Server) executeMerges(
	ctx context.Context,
	operations []parsedWrite,
	results []*sink.WriteResult,
) error {
	pending := operations
	for range s.maxMergeAttempts {
		if len(pending) == 0 {
			return nil
		}
		next, err := s.executeMergeAttempt(ctx, pending, results)
		if err != nil {
			return err
		}
		pending = next
	}

	for _, operation := range pending {
		result := results[operation.index]
		result.Status = sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
		conflictErr := errors.New("record changed during merge")
		result.Failure = newFailure(sink.FailureCode_FAILURE_CODE_CONFLICT, conflictErr, true)
	}
	return nil
}

func (s *Server) executeMergeAttempt(
	ctx context.Context,
	operations []parsedWrite,
	results []*sink.WriteResult,
) ([]parsedWrite, error) {
	readOperations := make([]storage.ReadOperation, 0, len(operations))
	for _, operation := range operations {
		readOperation := storage.ReadOperation{Address: operation.address}
		readOperations = append(readOperations, readOperation)
	}
	readRequest := storage.ReadRequest{Operations: readOperations}
	readResponse, err := s.storage.Read(ctx, readRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read records for merge: %v", err)
	}
	if len(readResponse.Results) != len(operations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid merge read result count")
	}

	candidates := make([]mergeCandidate, 0, len(operations))
	for index, stored := range readResponse.Results {
		operation := operations[index]
		candidate, include := s.prepareMergeCandidate(ctx, operation, stored, results[operation.index])
		if include {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	writeOperations := make([]storage.WriteOperation, 0, len(candidates))
	for _, candidate := range candidates {
		writeOperations = append(writeOperations, candidate.operation)
	}
	writeRequest := storage.WriteRequest{Operations: writeOperations}
	writeResponse, err := s.storage.Write(ctx, writeRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "commit merged records: %v", err)
	}
	if len(writeResponse.Results) != len(candidates) {
		return nil, status.Error(codes.Internal, "storage returned an invalid merge write result count")
	}

	next := make([]parsedWrite, 0)
	for index, stored := range writeResponse.Results {
		work := candidates[index].work
		if stored.Status == storage.WriteStatusPreconditionFailed {
			next = append(next, work)
			continue
		}
		applyWriteResult(results[work.index], stored)
	}
	return next, nil
}

func (s *Server) prepareMergeCandidate(
	ctx context.Context,
	operation parsedWrite,
	stored storage.ReadResult,
	result *sink.WriteResult,
) (mergeCandidate, bool) {
	candidate := mergeCandidate{work: operation}
	mergeRequest := merge.Request{
		Incoming:   operation.merge.incoming,
		ObservedAt: operation.merge.observedAt,
	}
	condition := storage.Precondition{}

	switch stored.Status {
	case storage.ReadStatusFound:
		current := stored.Document
		mergeRequest.Current = &current
		if len(stored.Revision.Data) == 0 {
			condition.Kind = storage.PreconditionRevisionAbsent
		} else {
			condition.Kind = storage.PreconditionRevisionMatches
			condition.Revision = stored.Revision
		}
	case storage.ReadStatusNotFound:
		if operation.merge.missingDocumentMode == sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL {
			notFoundErr := errors.New("record does not exist")
			setWriteFailure(result, sink.FailureCode_FAILURE_CODE_NOT_FOUND, notFoundErr, false)
			return candidate, false
		}
		condition.Kind = storage.PreconditionRecordNotExists
	case storage.ReadStatusFailed:
		code, retryable := storageFailureDetails(stored.Err)
		setWriteFailure(result, code, stored.Err, retryable)
		return candidate, false
	default:
		err := fmt.Errorf("unsupported storage read status %d", stored.Status)
		setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
		return candidate, false
	}

	merged, err := operation.merge.merger.Merge(ctx, mergeRequest)
	if err != nil {
		code := mergeFailureCode(err)
		setWriteFailure(result, code, err, false)
		return candidate, false
	}
	if merged.Document.ContentType == "" || len(merged.Document.Data) == 0 {
		err := errors.New("lua merge program returned an invalid document")
		setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
		return candidate, false
	}
	candidate.operation = storage.WriteOperation{
		Address:      operation.address,
		Document:     merged.Document,
		Precondition: condition,
	}
	return candidate, true
}

func mergeFailureCode(err error) sink.FailureCode {
	switch {
	case errors.Is(err, merge.ErrExecutionDeadline):
		return sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED
	case errors.Is(err, merge.ErrExecutionExhausted):
		return sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
	case errors.Is(err, merge.ErrInvalidProgram), errors.Is(err, merge.ErrInvalidIncoming),
		errors.Is(err, merge.ErrInvalidResult), errors.Is(err, merge.ErrExecution):
		return sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT
	default:
		return sink.FailureCode_FAILURE_CODE_INTERNAL
	}
}
