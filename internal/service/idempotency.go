package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func requireOperationIDs(req *sink.WriteRequest) error {
	if len(req.GetOperations()) == 0 {
		return status.Error(codes.InvalidArgument, "idempotent write requires operations")
	}
	for _, operation := range req.GetOperations() {
		if operation.GetOperationId() == "" {
			return status.Error(codes.InvalidArgument, "idempotent write requires an operation_id for every operation")
		}
	}
	return nil
}

func (s *Server) WriteIdempotent(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if err := requireOperationIDs(req); err != nil {
		return nil, err
	}
	return s.Write(ctx, req)
}

func (s *BatchingServer) WriteIdempotent(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if err := requireOperationIDs(req); err != nil {
		return nil, err
	}
	return s.Write(ctx, req)
}

// Check capability before any mutation or publication, including worker replay.
func (s *Server) checkIdempotency(ctx context.Context, req *sink.WriteRequest) error {
	checked := make(map[string]bool)
	for _, operation := range req.GetOperations() {
		if operation.GetOperationId() == "" {
			continue
		}
		if _, err := storage.OperationCreated(operation.GetOperationId(), time.Now()); err != nil {
			return nativeStatus(err)
		}
		address, err := convertAddress(operation.GetAddress())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if checked[address.Store] {
			continue
		}
		backend, ok := s.storage.(storage.IdempotentStorage)
		if !ok {
			return status.Error(codes.Unimplemented, storage.ErrIdempotencyUnsupported.Error())
		}
		if err := backend.CheckIdempotency(ctx, address); err != nil {
			if errors.Is(err, storage.ErrIdempotencyUnsupported) {
				return status.Error(codes.Unimplemented, err.Error())
			}
			return nativeStatus(err)
		}
		checked[address.Store] = true
	}
	return nil
}

func (g writeGroup) hasOperationIDs() bool {
	for _, operation := range g.operations {
		if operation.original.GetOperationId() != "" {
			return true
		}
	}
	return false
}

type uncommittedWrite struct{ result *sink.WriteResult }

func (e *uncommittedWrite) Error() string { return "idempotent write did not commit" }

func (s *Server) executeIdempotent(ctx context.Context, group writeGroup, results []*sink.WriteResult, opts writeExecutionOptions) error {
	operation := group.operations[0]
	canonical := proto.Clone(operation.original).(*sink.WriteOperation)
	canonical.OperationId = ""
	if operation.merge != nil {
		program := &sink.LuaProgram{Sha256: operation.merge.program.SHA256}
		canonical.GetMerge().LuaProgram = program
	}
	encoded, err := canonical.MarshalVT()
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(encoded)
	// Transaction retries may overwrite private results, never publish early
	// batcher completion before the receipt's transaction commits.
	transactionOptions := opts
	transactionOptions.completion = nil
	transactionOptions.transaction = true
	inner := operation
	inner.original = canonical
	innerGroup := writeGroup{operations: []parsedWrite{inner}, merges: group.merges}
	apply := func(tx context.Context) ([]byte, error) {
		private := make([]*sink.WriteResult, len(results))
		private[operation.index] = &sink.WriteResult{OperationIndex: uint32(operation.index)}
		err := s.executeWriteWave(tx, []writeGroup{innerGroup}, private, transactionOptions)
		if err != nil {
			return nil, err
		}
		result := private[operation.index]
		if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			failure := &uncommittedWrite{result: result}
			return nil, failure
		}
		result.OperationIndex = 0
		return result.MarshalVT()
	}
	maximum := min(s.maxReadBytes, 15<<20)
	if !operation.original.GetReturnDocument() {
		maximum = min(maximum, 4096)
	}
	request := storage.IdempotentRequest{Address: operation.address, OperationID: operation.original.GetOperationId(), Fingerprint: fingerprint[:], MaxReceiptBytes: maximum, Apply: apply}
	backend := s.storage.(storage.IdempotentStorage)
	receipt, err := backend.ExecuteOnce(ctx, request)
	if err != nil {
		var uncommitted *uncommittedWrite
		if errors.As(err, &uncommitted) {
			results[operation.index] = uncommitted.result
		} else {
			code, retryable := storageFailureDetails(err)
			setWriteFailure(results[operation.index], code, err, retryable)
		}
		opts.completion.group(group, results)
		return nil
	}
	result := &sink.WriteResult{}
	if err := result.UnmarshalVT(receipt); err != nil {
		return status.Error(codes.Internal, "invalid committed receipt")
	}
	if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		return status.Error(codes.Internal, "receipt was not applied")
	}
	result.OperationIndex = uint32(operation.index)
	if result.Document != nil {
		document := storage.Document{Encoding: storage.DocumentEncoding(result.Document.Encoding), Payload: result.Document.Payload}
		if err := opts.returns.reserve(group, document); err != nil {
			code, retryable := storageFailureDetails(err)
			setWriteFailure(results[operation.index], code, err, retryable)
			opts.completion.group(group, results)
			return nil
		}
	}
	results[operation.index] = result
	opts.completion.group(group, results)
	return nil
}
