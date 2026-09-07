package service

import "github.com/liran/sink/internal/storage"

// requestBudgets retains original RPC ownership after coalescing operations.
// Nil denotes one ordinary RPC with the core's existing byte limits.
type requestBudgets struct {
	owners []int
	count  int
}

func (b *requestBudgets) callerCount() int {
	if b == nil {
		return 1
	}
	return b.count
}

func (b *requestBudgets) owner(index int) int {
	if b == nil {
		return 0
	}
	return b.owners[index]
}

func (b *requestBudgets) fresh(maxBytes int) []*storage.ReadBudget {
	budgets := make([]*storage.ReadBudget, b.callerCount())
	for index := range budgets {
		budgets[index] = storage.NewReadBudget(maxBytes)
	}
	return budgets
}

func (b *requestBudgets) add(count int) {
	for range count {
		b.owners = append(b.owners, b.count)
	}
	b.count++
}

func sharedSnapshotBudget(owners []int, budgets []*storage.ReadBudget) *storage.ReadBudget {
	shared := make([]*storage.ReadBudget, 0, len(owners))
	seen := make(map[int]bool, len(owners))
	for _, owner := range owners {
		if !seen[owner] {
			shared = append(shared, budgets[owner])
			seen[owner] = true
		}
	}
	return storage.NewSharedReadBudget(shared)
}
