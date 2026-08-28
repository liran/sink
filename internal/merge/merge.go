// Package merge executes client-provided document merge programs.
package merge

import (
	"context"
	"errors"
	"time"

	"github.com/liran/sink/internal/storage"
)

var (
	ErrInvalidProgram     = errors.New("invalid Lua merge program")
	ErrInvalidIncoming    = errors.New("invalid incoming merge document")
	ErrInvalidCurrent     = errors.New("invalid current merge document")
	ErrInvalidResult      = errors.New("invalid Lua merge result")
	ErrExecution          = errors.New("lua merge execution failed")
	ErrExecutionDeadline  = errors.New("lua merge execution deadline exceeded")
	ErrExecutionExhausted = errors.New("lua merge execution resource limit exceeded")
)

type Program struct {
	Source []byte
	SHA256 []byte
}

type Request struct {
	Current    *storage.Document
	Incoming   storage.Document
	ObservedAt time.Time
}

type Result struct {
	Document storage.Document
}

// Merger is safe for concurrent calls. Each call runs in a fresh Lua VM so
// mutable globals cannot cross request or tenant boundaries.
type Merger interface {
	Merge(ctx context.Context, req Request) (Result, error)
}
