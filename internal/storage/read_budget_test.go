package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWorkingSetBudgetDefersWithoutChargingCaller(t *testing.T) {
	caller := NewReadBudget(512)
	working := NewReadBudget(256)
	budget := NewWorkingSetReadBudget(caller, working)
	if err := budget.Reserve(100); err != nil {
		t.Fatal(err)
	}
	if err := budget.Reserve(100); !errors.Is(err, ErrReadWorkingSetFull) {
		t.Fatalf("full working set: %v", err)
	}
	next := NewWorkingSetReadBudget(caller, NewReadBudget(256))
	if err := next.Reserve(100); err != nil {
		t.Fatalf("deferred read consumed caller quota: %v", err)
	}
	if err := NewWorkingSetReadBudget(caller, NewReadBudget(256)).Reserve(100); err == nil || errors.Is(err, ErrReadWorkingSetFull) {
		t.Fatalf("caller quota was not enforced across chunks: %v", err)
	}
}

func TestWorkingSetBudgetRefundsRejectedCallerAndRejectsOversizedRecord(t *testing.T) {
	working := NewReadBudget(256)
	oversized := NewWorkingSetReadBudget(NewReadBudget(512), working)
	if err := oversized.Reserve(256); err == nil || errors.Is(err, ErrReadWorkingSetFull) {
		t.Fatalf("oversized record would be deferred forever: %v", err)
	}
	rejected := NewWorkingSetReadBudget(NewReadBudget(128), working)
	if err := rejected.Reserve(100); err == nil {
		t.Fatal("invalid caller was admitted")
	}
	healthy := NewWorkingSetReadBudget(NewReadBudget(256), working)
	if err := healthy.Reserve(100); err != nil {
		t.Fatalf("failed caller retained working capacity: %v", err)
	}
}

func TestWorkingSetBudgetIsSharedAcrossConcurrentReaders(t *testing.T) {
	working := NewReadBudget(8 * 256)
	var accepted atomic.Int64
	var readers sync.WaitGroup
	for range 64 {
		readers.Go(func() {
			budget := NewWorkingSetReadBudget(NewReadBudget(256), working)
			err := budget.Reserve(128)
			if err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrReadWorkingSetFull) {
				t.Error(err)
			}
		})
	}
	readers.Wait()
	if accepted.Load() != 8 {
		t.Fatalf("admitted %d records, want 8", accepted.Load())
	}
}
