package storage

import (
	"errors"
	"sync"
)

const DefaultMaxReadBytes = 32 << 20

// ErrReadWorkingSetFull asks the synchronous executor to read this record in a
// later chunk. It is never a failure of the original RPC's document budget.
var ErrReadWorkingSetFull = errors.New("read working set is full")

// ReadBudget is shared across every store and repeated key in one read. Reserve
// before copying a result so small key batches cannot allocate unbounded output.
type ReadBudget struct {
	mu        sync.Mutex
	remaining int
	maximum   int
	shared    []*ReadBudget
	caller    *ReadBudget
	working   *ReadBudget
}

func NewReadBudget(maxBytes int) *ReadBudget {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxReadBytes
	}
	budget := &ReadBudget{remaining: maxBytes, maximum: maxBytes}
	return budget
}

// NewSharedReadBudget charges each interested caller once for a shared snapshot.
// A caller that exhausted its own quota cannot prevent another caller's read.
func NewSharedReadBudget(budgets []*ReadBudget) *ReadBudget {
	if len(budgets) == 1 {
		return budgets[0]
	}
	budget := &ReadBudget{shared: budgets}
	return budget
}

// NewWorkingSetReadBudget limits retained snapshots across independent RPCs.
// A deferred snapshot does not consume its caller's quota. The working budget
// must be an ordinary NewReadBudget, not another composite budget.
func NewWorkingSetReadBudget(caller, working *ReadBudget) *ReadBudget {
	budget := &ReadBudget{caller: caller, working: working}
	return budget
}

func (b *ReadBudget) Reserve(size int) error {
	if b.working != nil {
		if err := b.working.Reserve(size); err != nil {
			if size >= 0 && size <= b.working.maximum-128 {
				return ErrReadWorkingSetFull
			}
			return err
		}
		if err := b.caller.Reserve(size); err != nil {
			b.working.mu.Lock()
			b.working.remaining += size + 128
			b.working.mu.Unlock()
			return err
		}
		return nil
	}
	if len(b.shared) > 0 {
		accepted := false
		var failure error
		for _, budget := range b.shared {
			if err := budget.Reserve(size); err != nil {
				failure = err
			} else {
				accepted = true
			}
		}
		if accepted {
			return nil
		}
		return failure
	}

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
