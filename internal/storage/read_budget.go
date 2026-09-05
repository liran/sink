package storage

import (
	"errors"
	"sync"
)

const DefaultMaxReadBytes = 32 << 20

// ReadBudget is shared across every store and repeated key in one read. Reserve
// before copying a result so small key batches cannot allocate unbounded output.
type ReadBudget struct {
	mu        sync.Mutex
	remaining int
	maximum   int
}

func NewReadBudget(maxBytes int) *ReadBudget {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxReadBytes
	}
	budget := &ReadBudget{remaining: maxBytes, maximum: maxBytes}
	return budget
}

func (b *ReadBudget) Reserve(size int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Include room for the per-operation protobuf envelope and revision.
	const overhead = 128
	if size < 0 || b.remaining < overhead || size > b.remaining-overhead {
		cause := errors.New("read response exceeds its byte budget; request fewer or smaller records")
		return NewOperationError(ErrorCodeResourceExhausted, size >= 0 && size <= b.maximum-overhead, cause)
	}
	b.remaining -= size + overhead
	return nil
}
