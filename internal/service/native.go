package service

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nativeRequest(req *sink.ExecuteRequest, maximum int) (storage.NativeRequest, error) {
	request := storage.NativeRequest{MaxBytes: maximum}
	if req == nil || strings.TrimSpace(req.GetStore()) == "" {
		return request, status.Error(codes.InvalidArgument, "native request requires a store")
	}
	request.Store = req.GetStore()
	switch command := req.GetCommand().(type) {
	case *sink.ExecuteRequest_Mongodb:
		if command.Mongodb == nil {
			return request, status.Error(codes.InvalidArgument, "MongoDB command is missing")
		}
		request.MongoDB = &storage.MongoCommand{Database: command.Mongodb.GetDatabase(), Command: command.Mongodb.GetCommand()}
	case *sink.ExecuteRequest_Search:
		if command.Search == nil {
			return request, status.Error(codes.InvalidArgument, "search command is missing")
		}
		headers := make(http.Header)
		for _, header := range command.Search.GetHeaders() {
			for _, value := range header.GetValues() {
				headers.Add(header.GetName(), value)
			}
		}
		request.Search = &storage.SearchCommand{Method: command.Search.GetMethod(), Path: command.Search.GetPath(),
			Query: command.Search.GetQuery(), Body: command.Search.GetBody(), Headers: headers}
	default:
		return request, status.Error(codes.InvalidArgument, "native command is missing")
	}
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
	request, err := nativeRequest(req, s.maxReadBytes)
	if err != nil {
		return nil, err
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	admission := admissionRequest{encodedBytes: nativeExecutionBytes(req, request), stores: []string{request.Store}}
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

func (s *Server) Scan(req *sink.ScanRequest, stream grpc.ServerStreamingServer[sink.ScanResponse]) error {
	maximum := min(s.maxReadBytes, 4<<20)
	request, err := nativeRequest(req.GetRequest(), maximum)
	if err != nil {
		return err
	}
	batchSize := int(req.GetBatchSize())
	if batchSize == 0 {
		batchSize = 100
	}
	if batchSize > 1000 {
		return status.Error(codes.InvalidArgument, "scan batch size exceeds 1000")
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nativeStatus(storage.ErrNativeUnsupported)
	}
	admission := admissionRequest{encodedBytes: nativeExecutionBytes(req.GetRequest(), request) + 16, stores: []string{request.Store}, timeout: s.scanTimeout}
	ctx, release, err := s.admitRequest(stream.Context(), admission)
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	idle := time.AfterFunc(s.requestTimeout, func() { cancel(context.DeadlineExceeded) })
	defer idle.Stop()
	scan := storage.ScanRequest{Request: request, BatchSize: batchSize}
	send := func(documents []storage.Document) error {
		response := &sink.ScanResponse{Documents: make([]*sink.Document, 0, len(documents))}
		for _, document := range documents {
			encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
			response.Documents = append(response.Documents, encoded)
		}
		if response.SizeVT() > maximum {
			return status.Error(codes.ResourceExhausted, "scan page exceeds byte limit")
		}
		// Returning the handler cancels the transport if Send is blocked by a
		// client that stopped receiving. At most one send is outstanding.
		completed := make(chan error, 1)
		go func() { completed <- stream.Send(response) }()
		select {
		case err := <-completed:
			if err == nil {
				idle.Reset(s.requestTimeout)
			}
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	err = backend.Scan(ctx, scan, send)
	if context.Cause(ctx) != nil {
		return nativeStatus(context.Cause(ctx))
	}
	return nativeStatus(err)
}

func nativeExecutionBytes(req *sink.ExecuteRequest, request storage.NativeRequest) int {
	bytes := req.SizeVT() + 2*request.MaxBytes
	if request.MongoDB != nil {
		// The driver receives a complete wire message before Sink can enforce
		// its smaller document/page limit. Account for its 48 MiB wire ceiling.
		bytes += 48 << 20
	}
	return bytes
}

func (s *BatchingServer) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return s.server.Execute(ctx, req)
}

func (s *BatchingServer) Scan(req *sink.ScanRequest, stream grpc.ServerStreamingServer[sink.ScanResponse]) error {
	return s.server.Scan(req, stream)
}
