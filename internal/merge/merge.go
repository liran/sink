// Package merge registers versioned document merge implementations.
package merge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liran/sink/internal/storage"
)

var (
	ErrProfileNotFound   = errors.New("merge profile not found")
	ErrProfileRegistered = errors.New("merge profile already registered")
)

type Profile struct {
	Name    string
	Version uint64
}

type Request struct {
	Current  *storage.Document
	Incoming storage.Document
}

type Result struct {
	Document storage.Document
}

// Merger must be deterministic and free of external side effects because Sink
// can call it more than once when a storage revision changes concurrently.
type Merger interface {
	Merge(ctx context.Context, req Request) (Result, error)
}

type Registry struct {
	mu      sync.RWMutex
	mergers map[Profile]Merger
}

func NewRegistry() *Registry {
	registry := &Registry{
		mergers: make(map[Profile]Merger),
	}
	return registry
}

func (r *Registry) Register(profile Profile, merger Merger) error {
	if profile.Name == "" || profile.Version == 0 || merger == nil {
		return fmt.Errorf("register merge profile: %w", ErrProfileNotFound)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.mergers[profile]; exists {
		return fmt.Errorf("register merge profile %s@%d: %w", profile.Name, profile.Version, ErrProfileRegistered)
	}
	r.mergers[profile] = merger
	return nil
}

func (r *Registry) Resolve(profile Profile) (Merger, error) {
	r.mu.RLock()
	merger, exists := r.mergers[profile]
	r.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("resolve merge profile %s@%d: %w", profile.Name, profile.Version, ErrProfileNotFound)
	}
	return merger, nil
}
