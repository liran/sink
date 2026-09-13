package service

import (
	"context"
	"errors"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	outcome, err := s.read(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	return outcome.response, nil
}

type readOutcome struct {
	response *sink.ReadResponse
	deferred []bool
}

func (s *Server) read(ctx context.Context, req *sink.ReadRequest, budgets *requestBudgets) (readOutcome, error) {
	var outcome readOutcome
	if req == nil || len(req.GetOperations()) == 0 {
		return outcome, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return outcome, err
	}
	admission := admissionRequest{encodedBytes: req.SizeVT() + 2*s.maxReadBytes, stores: operationStores(req.GetOperations()), wait: budgets != nil}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return outcome, err
	}
	defer release()

	response := &sink.ReadResponse{
		Results: make([]*sink.ReadResult, len(req.GetOperations())),
	}
	outcome.response = response
	outcome.deferred = make([]bool, budgets.callerCount())
	storageOperations := make([]storage.ReadOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	storageIndexes := make([]int, 0, len(req.GetOperations()))
	owners := make([][]int, 0, len(req.GetOperations()))
	positions := make(map[recordIdentity]int)

	for index, operation := range req.GetOperations() {
		result := &sink.ReadResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		address, err := convertAddress(operation.GetAddress())
		if err != nil {
			setReadFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		key := s.identityOf(address)
		position, found := positions[key]
		if !found {
			position = len(storageOperations)
			positions[key] = position
			storageOperation := storage.ReadOperation{Address: address}
			storageOperations = append(storageOperations, storageOperation)
			owners = append(owners, nil)
		}
		owners[position] = append(owners[position], budgets.owner(index))
		operationIndexes = append(operationIndexes, index)
		storageIndexes = append(storageIndexes, position)
	}

	if len(storageOperations) == 0 {
		return outcome, nil
	}
	snapshotBudgets := budgets.fresh(s.maxReadBytes)
	working := storage.NewReadBudget(s.maxReadBytes)
	for index := range storageOperations {
		budget := sharedSnapshotBudget(owners[index], snapshotBudgets)
		if budgets.callerCount() > 1 {
			budget = storage.NewWorkingSetReadBudget(budget, working)
		}
		storageOperations[index].Budget = budget
	}
	storageRequest := storage.ReadRequest{Operations: storageOperations, Budget: storage.NewReadBudget(s.maxReadBytes)}
	storageResponse, err := s.storage.Read(ctx, storageRequest)
	if err != nil {
		return outcome, status.Errorf(codes.Unavailable, "read records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return outcome, status.Error(codes.Internal, "storage returned an invalid read result count")
	}

	defer clear(storageResponse.Results)
	for index, operationIndex := range operationIndexes {
		stored := storageResponse.Results[storageIndexes[index]]
		if errors.Is(stored.Err, storage.ErrReadWorkingSetFull) {
			outcome.deferred[budgets.owner(operationIndex)] = true
		}
	}

	// Charge copies in each original RPC's order, including repeated keys.
	// Check the aggregate output size before allocating any document copies.
	outputBudgets := budgets.fresh(s.maxReadBytes)
	outputBytes := make([]int, budgets.callerCount())
	for index, operationIndex := range operationIndexes {
		owner := budgets.owner(operationIndex)
		if outcome.deferred[owner] {
			continue
		}
		stored := storageResponse.Results[storageIndexes[index]]
		if stored.Status == storage.ReadStatusFound {
			if err := outputBudgets[owner].Reserve(len(stored.Document.Payload)); err != nil {
				code, retryable := storageFailureDetails(err)
				setReadFailure(response.Results[operationIndex], code, err, retryable)
				continue
			}
			outputBytes[owner] += len(stored.Document.Payload) + 128
		}
	}
	remaining := s.maxReadBytes
	for owner, size := range outputBytes {
		if size > remaining {
			outcome.deferred[owner] = true
		} else {
			remaining -= size
		}
	}
	for index, operationIndex := range operationIndexes {
		result := response.Results[operationIndex]
		if outcome.deferred[budgets.owner(operationIndex)] || result.Status != sink.ReadStatus_READ_STATUS_UNSPECIFIED {
			continue
		}
		applyReadResult(result, storageResponse.Results[storageIndexes[index]])
	}
	return outcome, nil
}
