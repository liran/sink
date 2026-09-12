// Package service implements the public Sink gRPC service.
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
	sinkmetrics "github.com/liran/sink/internal/metrics"
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
	Storage              storage.Storage
	Lua                  *merge.LuaEngine
	Publisher            queue.Publisher
	MaxOperations        int
	MaxMergeAttempts     int
	Metrics              *sinkmetrics.Metrics
	RequestTimeout       time.Duration
	MaxInFlightRequests  int
	MaxInFlightBytes     int
	MaxPublishRequests   int
	MaxPublishBytes      int
	MaxStoreRequests     int
	MaxReadBytes         int
	MaxScanRequests      int
	MaxScanBytes         int
	MaxStoreScanRequests int
	StoreNames           []string
}

type Server struct {
	sink.UnimplementedSinkServer
	*admissionPool

	storage          storage.Storage
	lua              *merge.LuaEngine
	publisher        queue.Publisher
	maxOperations    int
	maxMergeAttempts int
	metrics          *sinkmetrics.Metrics
	maxReadBytes     int
	publishAdmission *admissionPool
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
	if opts.RequestTimeout < 0 || opts.MaxInFlightRequests < 0 || opts.MaxInFlightBytes < 0 || opts.MaxStoreRequests < 0 || opts.MaxReadBytes < 0 {
		return nil, errors.New("create Sink server: resource limits cannot be negative")
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = defaultRequestTimeout
	}
	if opts.MaxInFlightRequests == 0 {
		opts.MaxInFlightRequests = 128
	}
	if opts.MaxInFlightBytes == 0 {
		opts.MaxInFlightBytes = 256 << 20
	}
	if opts.MaxStoreRequests == 0 {
		opts.MaxStoreRequests = 32
	}
	if opts.MaxReadBytes == 0 {
		opts.MaxReadBytes = storage.DefaultMaxReadBytes
	}
	if opts.MaxPublishRequests < 0 || opts.MaxPublishBytes < 0 {
		return nil, errors.New("create Sink server: publish limits cannot be negative")
	}
	if opts.MaxPublishRequests == 0 {
		opts.MaxPublishRequests = 32
	}
	if opts.MaxPublishBytes == 0 {
		opts.MaxPublishBytes = 256 << 20
	}
	if opts.MaxScanRequests < 0 || opts.MaxScanBytes < 0 || opts.MaxStoreScanRequests < 0 {
		return nil, errors.New("create Sink server: scan limits cannot be negative")
	}
	if opts.MaxScanRequests == 0 {
		opts.MaxScanRequests = max(1, opts.MaxInFlightRequests/2)
	}
	if opts.MaxScanBytes == 0 {
		opts.MaxScanBytes = max(1, opts.MaxInFlightBytes/2)
	}
	if opts.MaxStoreScanRequests == 0 {
		opts.MaxStoreScanRequests = max(1, opts.MaxStoreRequests/2)
	}
	if opts.MaxScanRequests > opts.MaxInFlightRequests || opts.MaxScanBytes > opts.MaxInFlightBytes || opts.MaxStoreScanRequests > opts.MaxStoreRequests {
		return nil, errors.New("create Sink server: scan limits cannot exceed total limits")
	}
	storeRequests := make(map[string]int, len(opts.StoreNames))
	publishStoreRequests := make(map[string]int, len(opts.StoreNames))
	for _, name := range opts.StoreNames {
		storeRequests[name] = 0
		publishStoreRequests[name] = 0
	}

	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	maxMergeAttempts := opts.MaxMergeAttempts
	if maxMergeAttempts == 0 {
		maxMergeAttempts = defaultMaxMergeAttempts
	}

	executionAdmission := &admissionPool{
		name:                 "execution",
		metrics:              opts.Metrics,
		requestTimeout:       opts.RequestTimeout,
		maxInFlightRequests:  opts.MaxInFlightRequests,
		maxInFlightBytes:     opts.MaxInFlightBytes,
		maxStoreRequests:     opts.MaxStoreRequests,
		storeRequests:        storeRequests,
		admissionChanged:     make(chan struct{}),
		maxScanRequests:      opts.MaxScanRequests,
		maxScanBytes:         opts.MaxScanBytes,
		maxStoreScanRequests: opts.MaxStoreScanRequests,
		storeScanRequests:    make(map[string]int),
	}
	publishAdmission := &admissionPool{
		name:                "publish",
		metrics:             opts.Metrics,
		requestTimeout:      opts.RequestTimeout,
		maxInFlightRequests: opts.MaxPublishRequests,
		maxInFlightBytes:    opts.MaxPublishBytes,
		maxStoreRequests:    opts.MaxStoreRequests,
		storeRequests:       publishStoreRequests,
		admissionChanged:    make(chan struct{}),
	}
	server := &Server{
		storage:          opts.Storage,
		lua:              opts.Lua,
		publisher:        opts.Publisher,
		maxOperations:    maxOperations,
		maxMergeAttempts: maxMergeAttempts,
		metrics:          opts.Metrics,
		maxReadBytes:     opts.MaxReadBytes,
		admissionPool:    executionAdmission,
		publishAdmission: publishAdmission,
	}
	return server, nil
}

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	return s.read(ctx, req, nil)
}

func (s *Server) read(ctx context.Context, req *sink.ReadRequest, budgets *requestBudgets) (*sink.ReadResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	admission := admissionRequest{encodedBytes: req.SizeVT() + 2*s.maxReadBytes*budgets.callerCount(), stores: operationStores(req.GetOperations()), wait: budgets != nil}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()

	response := &sink.ReadResponse{
		Results: make([]*sink.ReadResult, len(req.GetOperations())),
	}
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
		return response, nil
	}
	snapshotBudgets := budgets.fresh(s.maxReadBytes)
	for index := range storageOperations {
		storageOperations[index].Budget = sharedSnapshotBudget(owners[index], snapshotBudgets)
	}
	storageRequest := storage.ReadRequest{Operations: storageOperations, Budget: storage.NewReadBudget(s.maxReadBytes)}
	storageResponse, err := s.storage.Read(ctx, storageRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid read result count")
	}

	// Fetch each address once, but charge every returned copy in caller order.
	// Deduplication must not let repeated keys bypass the response byte limit.
	outputBudgets := budgets.fresh(s.maxReadBytes)
	for index, operationIndex := range operationIndexes {
		storageResult := storageResponse.Results[storageIndexes[index]]
		result := response.Results[operationIndex]
		if storageResult.Status == storage.ReadStatusFound {
			if err := outputBudgets[budgets.owner(operationIndex)].Reserve(len(storageResult.Document.Payload)); err != nil {
				code, retryable := storageFailureDetails(err)
				setReadFailure(result, code, err, retryable)
				continue
			}
		}
		applyReadResult(result, storageResult)
	}
	return response, nil
}

func (s *Server) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	return s.write(ctx, req, nil, nil)
}

func (s *Server) write(ctx context.Context, req *sink.WriteRequest, budgets *requestBudgets, completion *writeCompletion) (*sink.WriteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "write request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "write request has an invalid completion mode")
	}
	if hasWriteReturns(req) && req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return nil, status.Error(codes.InvalidArgument, "returned write documents require synchronous completion")
	}
	observation := s.newWriteObservation(req)
	defer observation.finish()
	encodedBytes := s.writeExecutionBytesFor(req, budgets.callerCount(), returningCallerCount(req, budgets))
	admission := admissionRequest{encodedBytes: encodedBytes, stores: operationStores(req.GetOperations()), wait: budgets != nil}
	admission.publish = req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	started := time.Now()
	ctx, release, err := s.admitRequest(ctx, admission)
	observation.phase("admission", started)
	if err != nil {
		return nil, err
	}
	defer release()
	started = time.Now()
	luaPrograms, err := parseLuaPrograms(req.GetLuaPrograms())
	if err != nil {
		observation.phase("parse", started)
		return nil, status.Errorf(codes.InvalidArgument, "write request Lua programs: %v", err)
	}

	response := &sink.WriteResponse{
		Results: make([]*sink.WriteResult, len(req.GetOperations())),
	}
	operations := make([]parsedWrite, 0, len(req.GetOperations()))
	for index, operation := range req.GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		if err := contextError(ctx); err != nil {
			observation.phase("parse", started)
			return nil, err
		}
		parsed, err := s.parseWrite(index, operation, luaPrograms)
		if err != nil {
			setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			completion.operation(index, result)
			continue
		}
		if parsed.merge != nil {
			parsed.merge.observation = observation
		}
		operations = append(operations, parsed)
	}
	observation.phase("parse", started)

	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		err := s.publishWrites(ctx, operations, response.Results)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	groups := buildWriteGroups(operations)
	executionOptions := writeExecutionOptions{
		returns:          newWriteReturns(req, budgets, s.maxReadBytes),
		budgets:          budgets,
		completion:       completion,
		observation:      observation,
		WaitUntilVisible: req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
	}
	err = s.executeWriteGroups(ctx, groups, response.Results, executionOptions)
	if err != nil {
		return nil, err
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
	return s.delete(ctx, req, false)
}

func (s *Server) delete(ctx context.Context, req *sink.DeleteRequest, wait bool) (*sink.DeleteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "delete request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "delete request has an invalid completion mode")
	}
	admission := admissionRequest{encodedBytes: req.SizeVT(), stores: operationStores(req.GetOperations()), wait: wait}
	admission.publish = req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()

	response := &sink.DeleteResponse{
		Results: make([]*sink.DeleteResult, len(req.GetOperations())),
	}
	storageOperations := make([]storage.DeleteOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	storageIndexes := make([]int, 0, len(req.GetOperations()))
	positions := make(map[recordIdentity]int)
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
		key := s.identityOf(address)
		position, found := positions[key]
		if !found {
			position = len(storageOperations)
			positions[key] = position
			storageOperation := storage.DeleteOperation{Address: address}
			storageOperations = append(storageOperations, storageOperation)
		}
		storageIndexes = append(storageIndexes, position)
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
	for index, operationIndex := range operationIndexes {
		storageResult := storageResponse.Results[storageIndexes[index]]
		result := response.Results[operationIndex]
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
