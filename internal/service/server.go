// Package service implements the public Sink gRPC service.
package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxOperations    = 1000
	defaultMaxMergeAttempts = 3
)

type Options struct {
	Storage          storage.Storage
	Lua              *merge.LuaEngine
	Publisher        queue.Publisher
	MaxOperations    int
	MaxMergeAttempts int
}

type Server struct {
	sink.UnimplementedSinkServer

	storage          storage.Storage
	lua              *merge.LuaEngine
	publisher        queue.Publisher
	maxOperations    int
	maxMergeAttempts int
}

func New(opts Options) (*Server, error) {
	if opts.Storage == nil {
		return nil, errors.New("create Sink server: storage is required")
	}
	if opts.Lua == nil {
		return nil, errors.New("create Sink server: Lua merge engine is required")
	}
	if opts.MaxOperations < 0 {
		return nil, errors.New("create Sink server: max operations cannot be negative")
	}
	if opts.MaxMergeAttempts < 0 {
		return nil, errors.New("create Sink server: max merge attempts cannot be negative")
	}

	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	maxMergeAttempts := opts.MaxMergeAttempts
	if maxMergeAttempts == 0 {
		maxMergeAttempts = defaultMaxMergeAttempts
	}

	server := &Server{
		storage:          opts.Storage,
		lua:              opts.Lua,
		publisher:        opts.Publisher,
		maxOperations:    maxOperations,
		maxMergeAttempts: maxMergeAttempts,
	}
	return server, nil
}

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}

	response := &sink.ReadResponse{
		Results: make([]*sink.ReadResult, len(req.GetOperations())),
	}
	storageOperations := make([]storage.ReadOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))

	for index, operation := range req.GetOperations() {
		result := &sink.ReadResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		address, err := convertAddress(operation.GetAddress())
		if err != nil {
			setReadFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		storageOperation := storage.ReadOperation{Address: address}
		storageOperations = append(storageOperations, storageOperation)
		operationIndexes = append(operationIndexes, index)
	}

	if len(storageOperations) == 0 {
		return response, nil
	}
	storageRequest := storage.ReadRequest{Operations: storageOperations}
	storageResponse, err := s.storage.Read(ctx, storageRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid read result count")
	}

	for resultIndex, storageResult := range storageResponse.Results {
		result := response.Results[operationIndexes[resultIndex]]
		applyReadResult(result, storageResult)
	}
	return response, nil
}

func (s *Server) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "write request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "write request has an invalid completion mode")
	}
	luaPrograms, err := parseLuaPrograms(req.GetLuaPrograms())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "write request Lua programs: %v", err)
	}

	response := &sink.WriteResponse{
		Results: make([]*sink.WriteResult, len(req.GetOperations())),
	}
	operations := make([]parsedWrite, 0, len(req.GetOperations()))
	for index, operation := range req.GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		parsed, err := s.parseWrite(index, operation, luaPrograms)
		if err != nil {
			setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		operations = append(operations, parsed)
	}

	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		err := s.publishWrites(ctx, operations, response.Results)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	waves := buildWriteWaves(operations)
	executionOptions := writeExecutionOptions{
		WaitUntilVisible: req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
	}
	for _, wave := range waves {
		err := s.executeWriteWave(ctx, wave, response.Results, executionOptions)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

type luaPrograms map[[sha256.Size]byte]merge.Program

func parseLuaPrograms(programs []*sink.LuaProgram) (luaPrograms, error) {
	parsed := make(luaPrograms, len(programs))
	for index, program := range programs {
		if program == nil || len(program.GetSource()) == 0 {
			return nil, fmt.Errorf("program %d source is required", index)
		}
		digest := sha256.Sum256(program.GetSource())
		if len(program.GetSha256()) != 0 {
			if len(program.GetSha256()) != sha256.Size || !bytes.Equal(program.GetSha256(), digest[:]) {
				return nil, fmt.Errorf("program %d SHA-256 digest does not match source", index)
			}
		}
		if existing, ok := parsed[digest]; ok && !bytes.Equal(existing.Source, program.GetSource()) {
			return nil, fmt.Errorf("program %d has a duplicate SHA-256 digest", index)
		}
		parsed[digest] = merge.Program{Source: bytes.Clone(program.GetSource()), SHA256: bytes.Clone(digest[:])}
	}
	return parsed, nil
}

func (s *Server) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "delete request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "delete request has an invalid completion mode")
	}

	response := &sink.DeleteResponse{
		Results: make([]*sink.DeleteResult, len(req.GetOperations())),
	}
	storageOperations := make([]storage.DeleteOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	queueMutations := make([]queue.Mutation, 0, len(req.GetOperations()))

	for index, operation := range req.GetOperations() {
		result := &sink.DeleteResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		address, err := convertAddress(operation.GetAddress())
		if err != nil {
			setDeleteFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		operationIndexes = append(operationIndexes, index)
		if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
			clonedMessage := proto.Clone(operation)
			cloned, ok := clonedMessage.(*sink.DeleteOperation)
			if !ok {
				return nil, status.Error(codes.Internal, "clone asynchronous delete operation")
			}
			mutation := queue.Mutation{Delete: cloned}
			queueMutations = append(queueMutations, mutation)
			continue
		}
		storageOperation := storage.DeleteOperation{Address: address}
		storageOperations = append(storageOperations, storageOperation)
	}

	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		err := s.publishDeletes(ctx, queueMutations, operationIndexes, response.Results)
		if err != nil {
			return nil, err
		}
		return response, nil
	}
	if len(storageOperations) == 0 {
		return response, nil
	}

	storageRequest := storage.DeleteRequest{
		Operations:       storageOperations,
		WaitUntilVisible: req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
	}
	storageResponse, err := s.storage.Delete(ctx, storageRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "delete records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid delete result count")
	}
	for resultIndex, storageResult := range storageResponse.Results {
		result := response.Results[operationIndexes[resultIndex]]
		applyDeleteResult(result, storageResult)
	}
	return response, nil
}

func (s *Server) validateOperationCount(count int) error {
	if count <= s.maxOperations {
		return nil
	}
	message := fmt.Sprintf("request contains %d operations; maximum is %d", count, s.maxOperations)
	return status.Error(codes.ResourceExhausted, message)
}

func validCompletionMode(mode sink.CompletionMode) bool {
	return mode == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED ||
		mode == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED ||
		mode == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
}
