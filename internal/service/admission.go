package service

import (
	"context"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultRequestTimeout = 30 * time.Second

type admissionRequest struct {
	encodedBytes int
	stores       []string
	wait         bool
	timeout      time.Duration
	scan         bool
}

func (s *Server) admitRequest(ctx context.Context, request admissionRequest) (context.Context, context.CancelFunc, error) {
	for {
		if err := contextError(ctx); err != nil {
			return ctx, nil, err
		}
		s.admissionMu.Lock()
		full := s.inFlightRequests >= s.maxInFlightRequests || request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes
		if request.scan && (s.scanRequests >= s.maxScanRequests || request.encodedBytes > s.maxScanBytes-s.scanBytes) {
			full = true
		}
		for _, name := range request.stores {
			if request.scan && s.storeScanRequests[name] >= s.maxStoreScanRequests {
				full = true
			}
			if count, configured := s.storeRequests[name]; configured && count >= s.maxStoreRequests {
				full = true
			}
		}
		if full {
			changed := s.admissionChanged
			s.admissionMu.Unlock()
			if request.wait && request.encodedBytes <= s.maxInFlightBytes {
				select {
				case <-changed:
					continue
				case <-ctx.Done():
					return ctx, nil, contextError(ctx)
				}
			}
			s.metrics.ObserveAdmissionRejected()
			return ctx, nil, status.Error(codes.ResourceExhausted, "Sink execution capacity is full")
		}
		s.inFlightRequests++
		s.inFlightBytes += request.encodedBytes
		if request.scan {
			s.scanRequests++
			s.scanBytes += request.encodedBytes
		}
		for _, name := range request.stores {
			if request.scan {
				s.storeScanRequests[name]++
			}
			if _, configured := s.storeRequests[name]; configured {
				s.storeRequests[name]++
			}
		}
		s.admissionMu.Unlock()
		break
	}
	s.metrics.AdjustAdmission(1, request.encodedBytes)
	timeout := request.timeout
	if timeout == 0 {
		timeout = s.requestTimeout
	}
	execution, cancel := context.WithTimeout(ctx, timeout)
	release := func() {
		cancel()
		s.admissionMu.Lock()
		s.inFlightRequests--
		s.inFlightBytes -= request.encodedBytes
		if request.scan {
			s.scanRequests--
			s.scanBytes -= request.encodedBytes
		}
		for _, name := range request.stores {
			if request.scan {
				s.storeScanRequests[name]--
				if s.storeScanRequests[name] == 0 {
					delete(s.storeScanRequests, name)
				}
			}
			if _, configured := s.storeRequests[name]; configured {
				s.storeRequests[name]--
			}
		}
		close(s.admissionChanged)
		s.admissionChanged = make(chan struct{})
		s.admissionMu.Unlock()
		s.metrics.AdjustAdmission(-1, -request.encodedBytes)
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
	return s.writeExecutionBytesFor(req, 1)
}

func (s *Server) writeExecutionBytesFor(req *sink.WriteRequest, callers int) int {
	bytes := req.SizeVT()
	for _, operation := range req.GetOperations() {
		if operation.GetOperationId() != "" {
			// Mongo receipt lookups receive a complete driver wire message.
			// Protected operations execute sequentially inside this admission.
			bytes += 48 << 20
			break
		}
	}
	if hasWriteReturns(req) {
		bytes += s.maxReadBytes * callers
		for _, operation := range req.GetOperations() {
			if operation.GetOperationId() != "" {
				// Receipt encoding/decoding retains another bounded copy.
				bytes += 2 * s.maxReadBytes * callers
				break
			}
		}
	}
	largestSource := 0
	for _, program := range req.GetLuaPrograms() {
		largestSource = max(largestSource, len(program.GetSource()))
	}
	hasSnapshot := false
	hasConditionalPut := false
	if len(req.GetOperations()) > 1 {
		for _, operation := range req.GetOperations() {
			if operation.GetPut() != nil && operation.GetPut().GetMode() != sink.WriteMode_WRITE_MODE_UPSERT {
				hasConditionalPut = true
				break
			}
		}
	}
	type putRun struct {
		count       int
		conditional bool
	}
	puts := make(map[recordIdentity]putRun)
	for _, operation := range req.GetOperations() {
		if operation.GetMerge() != nil {
			hasSnapshot = true
			bytes += max(largestSource, len(operation.GetMerge().GetLuaProgram().GetSource()))
		}
		if hasConditionalPut && !hasSnapshot && operation.GetPut() != nil {
			address, err := convertAddress(operation.GetAddress())
			if err == nil {
				key := identityOf(address)
				run := puts[key]
				run.count++
				run.conditional = run.conditional || operation.GetPut().GetMode() != sink.WriteMode_WRITE_MODE_UPSERT
				puts[key] = run
				hasSnapshot = run.count > 1 && run.conditional
			}
		}
		if bytes > s.maxInFlightBytes {
			return bytes
		}
	}
	if hasSnapshot && req.GetCompletionMode() != sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		// Shared records retain one snapshot and final document, even when
		// several RPCs contribute to the chain. Preserve hot-key folding while
		// reserving separate space for independent callers/records.
		retained := callers
		if callers > 1 {
			records := make(map[recordIdentity]bool)
			for _, operation := range req.GetOperations() {
				address, err := convertAddress(operation.GetAddress())
				if err == nil {
					records[identityOf(address)] = true
				}
			}
			retained = min(callers, max(1, len(records)))
		}
		bytes += 2 * s.maxReadBytes * retained
	}
	return bytes
}
