package service

import (
	"bytes"
	"errors"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
)

// Reserve the largest candidate for each operation across CAS attempts. This
// bounds retained responses before committing, including per-RPC batching.
type writeReturns struct {
	remaining []int
	sizes     map[int]int
	owners    *requestBudgets
}

func newWriteReturns(req *sink.WriteRequest, budgets *requestBudgets, maximum int) *writeReturns {
	if !hasWriteReturns(req) {
		return nil
	}
	remaining := make([]int, budgets.callerCount())
	for index := range remaining {
		remaining[index] = maximum
	}
	returns := &writeReturns{remaining: remaining, sizes: make(map[int]int), owners: budgets}
	return returns
}

func hasWriteReturns(req *sink.WriteRequest) bool {
	for _, operation := range req.GetOperations() {
		if operation.GetReturnDocument() {
			return true
		}
	}
	return false
}

func (g writeGroup) hasReturns() bool {
	for _, operation := range g.operations {
		if operation.original.GetReturnDocument() {
			return true
		}
	}
	return false
}

func (r *writeReturns) reserve(group writeGroup, document storage.Document) error {
	if r == nil {
		return nil
	}
	for _, operation := range group.operations {
		if !operation.original.GetReturnDocument() {
			continue
		}
		owner := r.owners.owner(operation.index)
		charge := len(document.Payload) + 128
		difference := max(0, charge-r.sizes[operation.index])
		if difference > r.remaining[owner] {
			return storage.ResourceExhaustedError(errors.New("returned write documents exceed response byte budget"))
		}
		r.remaining[owner] -= difference
		r.sizes[operation.index] = max(charge, r.sizes[operation.index])
	}
	return nil
}

func attachWriteDocument(group writeGroup, results []*sink.WriteResult, document storage.Document) {
	for _, operation := range group.operations {
		result := results[operation.index]
		if operation.original.GetReturnDocument() && result.GetStatus() == sink.WriteStatus_WRITE_STATUS_APPLIED {
			returned := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: bytes.Clone(document.Payload)}
			result.Document = returned
		}
	}
}
