package service

import (
	"context"
	"slices"
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
	var queued *admissionRequest
	defer func() {
		if queued != nil {
			s.admissionMu.Lock()
			s.removeAdmissionWaiter(queued)
			s.admissionMu.Unlock()
		}
	}()
	for {
		if err := contextError(ctx); err != nil {
			return ctx, nil, err
		}
		s.admissionMu.Lock()
		full := s.admissionSlotsFull(request) || request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes
		for _, earlier := range s.admissionWaiters {
			if earlier == queued {
				break
			}
			// A runnable older batch reserves the next available byte capacity.
			// Otherwise small arrivals can indefinitely starve returned-document
			// batches. A busy store or scan pool must still let other stores run.
			if !s.admissionSlotsFull(*earlier) {
				full = true
				break
			}
		}
		if full {
			canWait := request.wait && request.encodedBytes <= s.maxInFlightBytes
			if canWait && queued == nil {
				queued = &request
				s.admissionWaiters = append(s.admissionWaiters, queued)
			}
			changed := s.admissionChanged
			s.admissionMu.Unlock()
			if canWait {
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
		if queued != nil {
			s.removeAdmissionWaiter(queued)
			queued = nil
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

// The caller holds admissionMu. Global bytes are handled separately so a
// waiting large request can accumulate space without reserving a store slot.
func (s *Server) admissionSlotsFull(request admissionRequest) bool {
	if s.inFlightRequests >= s.maxInFlightRequests {
		return true
	}
	if request.scan && (s.scanRequests >= s.maxScanRequests || request.encodedBytes > s.maxScanBytes-s.scanBytes) {
		return true
	}
	for _, name := range request.stores {
		if request.scan && s.storeScanRequests[name] >= s.maxStoreScanRequests {
			return true
		}
		if count, configured := s.storeRequests[name]; configured && count >= s.maxStoreRequests {
			return true
		}
	}
	return false
}

func (s *Server) removeAdmissionWaiter(request *admissionRequest) {
	index := slices.Index(s.admissionWaiters, request)
	if index < 0 {
		return
	}
	s.admissionWaiters = slices.Delete(s.admissionWaiters, index, index+1)
	close(s.admissionChanged)
	s.admissionChanged = make(chan struct{})
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
	if hasWriteReturns(req) {
		bytes += s.maxReadBytes * callers
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
		// A micro-batch streams independent records through one bounded read
		// chunk and one output batch. One additional candidate can coexist with
		// the output batch while it is committed. Caller quotas remain separate.
		workingSets := min(2*retained, 3)
		bytes += s.maxReadBytes * workingSets
	}
	return bytes
}
