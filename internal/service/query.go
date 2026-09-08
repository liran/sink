package service

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	request, err := nativeRequest(req.GetCommand(), s.maxReadBytes)
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	if pageSize == 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		return nil, status.Error(codes.InvalidArgument, "query page size exceeds 1000")
	}
	page := max(req.GetPage(), 1)
	query := storage.QueryRequest{Request: request, Offset: int64(page-1) * int64(pageSize), PageSize: pageSize}
	for _, field := range req.GetSort() {
		item := storage.SortField{Field: field.GetField(), Descending: field.GetDescending()}
		query.Sort = append(query.Sort, item)
	}
	if projection := req.GetProjection(); projection != nil {
		query.Projection = &storage.Projection{Fields: projection.GetFields(), Exclude: projection.GetExclude()}
	}
	if err := query.Validate(); err != nil {
		return nil, nativeStatus(err)
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	encodedBytes := nativeExecutionBytes(req.GetCommand(), request) + req.SizeVT() - req.GetCommand().SizeVT()
	admission := admissionRequest{encodedBytes: encodedBytes, stores: []string{request.Store}}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	result, err := backend.Query(ctx, query)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if len(result.Documents) > pageSize || (result.HasMore && len(result.Documents) != pageSize) {
		return nil, status.Error(codes.Internal, "backend returned an invalid query page")
	}
	response := &sink.QueryResponse{HasMore: result.HasMore}
	for _, document := range result.Documents {
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		response.Documents = append(response.Documents, encoded)
	}
	if response.SizeVT() > s.maxReadBytes {
		return nil, status.Error(codes.ResourceExhausted, "query response exceeds byte limit")
	}
	return response, nil
}

func (s *Server) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	request, err := nativeRequest(req.GetCommand(), s.maxReadBytes)
	if err != nil {
		return nil, err
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	admission := admissionRequest{encodedBytes: nativeExecutionBytes(req.GetCommand(), request), stores: []string{request.Store}}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	countRequest := storage.CountRequest{Request: request, Estimate: req.GetEstimate()}
	count, err := backend.Count(ctx, countRequest)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if !req.GetEstimate() && count.Estimated {
		return nil, status.Error(codes.Internal, "backend returned an estimate for an exact count")
	}
	response := &sink.CountResponse{Count: count.Count, Estimated: count.Estimated}
	return response, nil
}

func (s *BatchingServer) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	return s.server.Query(ctx, req)
}

func (s *BatchingServer) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	return s.server.Count(ctx, req)
}
