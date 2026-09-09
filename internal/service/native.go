package service

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"sort"
	"strings"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nativeRequest(req *sink.Command, maximum int) (storage.NativeRequest, error) {
	request := storage.NativeRequest{MaxBytes: maximum}
	if req == nil || strings.TrimSpace(req.GetStore()) == "" {
		return request, status.Error(codes.InvalidArgument, "native request requires a store")
	}
	if len(req.GetPayload()) > 0 && req.GetContentType() == "" {
		return request, status.Error(codes.InvalidArgument, "native payload requires content_type")
	}
	if req.GetContentType() != "" {
		if _, _, err := mime.ParseMediaType(req.GetContentType()); err != nil {
			return request, status.Error(codes.InvalidArgument, "invalid native content_type")
		}
	}
	headers := make(http.Header)
	for _, header := range req.GetHeaders() {
		if header == nil || header.GetName() == "" || len(header.GetValues()) == 0 {
			return request, status.Error(codes.InvalidArgument, "native header requires a name and values")
		}
		headers[header.GetName()] = append(headers[header.GetName()], header.GetValues()...)
	}
	request = storage.NativeRequest{Store: req.GetStore(), Namespace: req.GetNamespace(),
		Method: req.GetMethod(), Path: req.GetPath(), Query: req.GetQuery(), Headers: headers,
		ContentType: req.GetContentType(), Payload: req.GetPayload(), MaxBytes: maximum}
	return request, nil
}

func nativeStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, storage.ErrNativeUnsupported) {
		return status.Error(codes.Unimplemented, err.Error())
	}
	code, _ := storage.ErrorDetails(err)
	switch code {
	case storage.ErrorCodeInvalidArgument:
		return status.Error(codes.InvalidArgument, err.Error())
	case storage.ErrorCodeResourceExhausted:
		return status.Error(codes.ResourceExhausted, err.Error())
	case storage.ErrorCodeUnavailable:
		return status.Error(codes.Unavailable, err.Error())
	case storage.ErrorCodeDeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *Server) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
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
	result, err := backend.Execute(ctx, request)
	if err != nil {
		return nil, nativeStatus(err)
	}
	response := &sink.ExecuteResponse{ContentType: result.ContentType, Payload: result.Payload,
		Success: result.Success, StatusCode: uint32(result.StatusCode)}
	names := make([]string, 0, len(result.Headers))
	for name := range result.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &sink.Header{Name: name, Values: result.Headers[name]}
		response.Headers = append(response.Headers, header)
	}
	if response.SizeVT() > s.maxReadBytes {
		return nil, status.Error(codes.ResourceExhausted, "native response exceeds byte limit")
	}
	return response, nil
}

func (s *Server) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	maximum := min(s.maxReadBytes, 4<<20)
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return nil, err
	}
	batchSize := int(req.GetBatchSize())
	if batchSize == 0 {
		batchSize = 100
	}
	scan := storage.ScanRequest{Request: request, BatchSize: batchSize, Cursor: req.GetCursor()}
	if batchSize > 1000 || len(scan.Cursor) > storage.MaxScanCursorBytes {
		return nil, status.Error(codes.InvalidArgument, "scan batch or cursor exceeds its limit")
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	encodedBytes := nativeExecutionBytes(req.GetCommand(), request) + req.SizeVT() - req.GetCommand().SizeVT()
	admission := admissionRequest{encodedBytes: encodedBytes, stores: []string{request.Store}, scan: true}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := scan.Resume(); err != nil {
		return nil, nativeStatus(err)
	}
	// Leave room for cursor metadata and protobuf framing inside the page limit.
	scan.Request.MaxBytes = maximum - min(maximum/4, storage.MaxScanCursorBytes) - 128
	if scan.Request.MaxBytes <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "scan page budget is too small")
	}
	result, err := backend.Scan(ctx, scan)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if len(result.Documents) > batchSize || (len(result.NextCursor) != 0 && len(result.Documents) == 0) {
		return nil, status.Error(codes.Internal, "backend returned an invalid scan page")
	}
	response := &sink.ScanResponse{NextCursor: result.NextCursor}
	for _, document := range result.Documents {
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		response.Documents = append(response.Documents, encoded)
	}
	if response.SizeVT() > maximum {
		return nil, status.Error(codes.ResourceExhausted, "scan response exceeds byte limit")
	}
	return response, nil
}

func nativeExecutionBytes(req *sink.Command, request storage.NativeRequest) int {
	bytes := req.SizeVT() + 16 + 2*request.MaxBytes
	mediaType, _, _ := mime.ParseMediaType(request.ContentType)
	if mediaType == "application/bson" {
		// The driver receives a complete wire message before Sink can enforce
		// its smaller document/page limit. Account for its 48 MiB wire ceiling.
		bytes += 48 << 20
	}
	return bytes
}

func (s *BatchingServer) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return s.server.Execute(ctx, req)
}

func (s *BatchingServer) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	return s.server.Scan(ctx, req)
}
