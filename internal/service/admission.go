package service

import (
	"context"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultRequestTimeout = 30 * time.Second

func (s *Server) beginRequest(ctx context.Context, encodedBytes int, stores []string) (context.Context, context.CancelFunc, error) {
	if err := contextError(ctx); err != nil {
		return ctx, nil, err
	}
	s.admissionMu.Lock()
	full := s.inFlightRequests >= s.maxInFlightRequests || encodedBytes > s.maxInFlightBytes-s.inFlightBytes
	for _, name := range stores {
		if count, configured := s.storeRequests[name]; configured && count >= s.maxStoreRequests {
			full = true
		}
	}
	if full {
		s.admissionMu.Unlock()
		s.metrics.ObserveAdmissionRejected()
		return ctx, nil, status.Error(codes.ResourceExhausted, "Sink execution capacity is full")
	}
	s.inFlightRequests++
	s.inFlightBytes += encodedBytes
	for _, name := range stores {
		if _, configured := s.storeRequests[name]; configured {
			s.storeRequests[name]++
		}
	}
	s.admissionMu.Unlock()
	s.metrics.AdjustAdmission(1, encodedBytes)
	execution, cancel := context.WithTimeout(ctx, s.requestTimeout)
	release := func() {
		cancel()
		s.admissionMu.Lock()
		s.inFlightRequests--
		s.inFlightBytes -= encodedBytes
		for _, name := range stores {
			if _, configured := s.storeRequests[name]; configured {
				s.storeRequests[name]--
			}
		}
		s.admissionMu.Unlock()
		s.metrics.AdjustAdmission(-1, -encodedBytes)
	}
	return execution, release, nil
}

func operationStores[T interface{ GetAddress() *sink.RecordAddress }](operations []T) []string {
	seen := make(map[string]struct{})
	for _, operation := range operations {
		seen[operation.GetAddress().GetStore()] = struct{}{}
	}
	stores := make([]string, 0, len(seen))
	for name := range seen {
		stores = append(stores, name)
	}
	return stores
}

// Include retained output and expanded source copies, before parsing or cloning.
// This bounds admitted payload bytes; VM/driver overhead is sized separately.
func (s *Server) writeExecutionBytes(req *sink.WriteRequest) int {
	bytes := req.SizeVT()
	largestSource := 0
	for _, program := range req.GetLuaPrograms() {
		largestSource = max(largestSource, len(program.GetSource()))
	}
	hasMerge := false
	for _, operation := range req.GetOperations() {
		if operation.GetMerge() != nil {
			hasMerge = true
			bytes += max(largestSource, len(operation.GetMerge().GetLuaProgram().GetSource()))
		}
		if bytes > s.maxInFlightBytes {
			return bytes
		}
	}
	if hasMerge && req.GetCompletionMode() != sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		bytes += 2 * s.maxReadBytes
	}
	return bytes
}
