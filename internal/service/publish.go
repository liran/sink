package service

import (
	"context"
	"errors"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Server) publishWrites(
	ctx context.Context,
	operations []parsedWrite,
	results []*sink.WriteResult,
) error {
	if len(operations) == 0 {
		return nil
	}
	if s.publisher == nil {
		err := errors.New("asynchronous publisher is not configured")
		for _, operation := range operations {
			setWriteFailure(results[operation.index], sink.FailureCode_FAILURE_CODE_UNAVAILABLE, err, true)
		}
		return nil
	}

	mutations := make([]queue.Mutation, 0, len(operations))
	for _, operation := range operations {
		clonedMessage := proto.Clone(operation.original)
		cloned, ok := clonedMessage.(*sink.WriteOperation)
		if !ok {
			return status.Error(codes.Internal, "clone asynchronous write operation")
		}
		mutation := queue.Mutation{Write: cloned}
		mutations = append(mutations, mutation)
	}
	request := queue.PublishRequest{Mutations: mutations}
	response, err := s.publisher.Publish(ctx, request)
	if err != nil {
		return status.Errorf(codes.Unavailable, "publish write operations: %v", err)
	}
	if len(response.Results) != len(operations) {
		return status.Error(codes.Internal, "publisher returned an invalid write result count")
	}
	for index, published := range response.Results {
		result := results[operations[index].index]
		applyPublishWriteResult(result, published)
	}
	return nil
}

func (s *Server) publishDeletes(
	ctx context.Context,
	mutations []queue.Mutation,
	operationIndexes []int,
	results []*sink.DeleteResult,
) error {
	if len(mutations) == 0 {
		return nil
	}
	if s.publisher == nil {
		err := errors.New("asynchronous publisher is not configured")
		for _, index := range operationIndexes {
			setDeleteFailure(results[index], sink.FailureCode_FAILURE_CODE_UNAVAILABLE, err, true)
		}
		return nil
	}

	request := queue.PublishRequest{Mutations: mutations}
	response, err := s.publisher.Publish(ctx, request)
	if err != nil {
		return status.Errorf(codes.Unavailable, "publish delete operations: %v", err)
	}
	if len(response.Results) != len(mutations) {
		return status.Error(codes.Internal, "publisher returned an invalid delete result count")
	}
	for index, published := range response.Results {
		result := results[operationIndexes[index]]
		applyPublishDeleteResult(result, published)
	}
	return nil
}

func applyPublishWriteResult(result *sink.WriteResult, published queue.PublishResult) {
	switch published.Status {
	case queue.PublishStatusAccepted:
		result.Status = sink.WriteStatus_WRITE_STATUS_ACCEPTED
	case queue.PublishStatusFailed:
		setWriteFailure(result, sink.FailureCode_FAILURE_CODE_UNAVAILABLE, published.Err, true)
	default:
		err := errors.New("publisher returned an unsupported status")
		setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
	}
}

func applyPublishDeleteResult(result *sink.DeleteResult, published queue.PublishResult) {
	switch published.Status {
	case queue.PublishStatusAccepted:
		result.Status = sink.DeleteStatus_DELETE_STATUS_ACCEPTED
	case queue.PublishStatusFailed:
		setDeleteFailure(result, sink.FailureCode_FAILURE_CODE_UNAVAILABLE, published.Err, true)
	default:
		err := errors.New("publisher returned an unsupported status")
		setDeleteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
	}
}
