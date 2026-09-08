package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

var ErrNativeUnsupported = errors.New("native operation is not supported")

// NativeStorage is implemented by adapters that execute native queries. Data
// mutations stay on Storage so native commands cannot bypass revision checks.
type NativeStorage interface {
	Execute(context.Context, NativeRequest) (NativeResponse, error)
	Scan(context.Context, ScanRequest, func([]Document) error) error
}

type NativeRequest struct {
	Store    string
	MongoDB  *MongoCommand
	Search   *SearchCommand
	MaxBytes int
}

type MongoCommand struct {
	Database string
	Command  []byte
}

type SearchCommand struct {
	Method  string
	Path    string
	Query   string
	Headers http.Header
	Body    []byte
}

type NativeResponse struct {
	ContentType string
	Payload     []byte
	Success     bool
	StatusCode  int
	Headers     http.Header
}

type ScanRequest struct {
	Request   NativeRequest
	BatchSize int
}

func (r *Router) nativeBackend(name string) (NativeStorage, error) {
	backend, exists := r.backends[name]
	if !exists {
		cause := fmt.Errorf("storage %q is not configured", name)
		return nil, InvalidArgumentError(cause)
	}
	native, ok := backend.(NativeStorage)
	if !ok {
		return nil, fmt.Errorf("storage %q: %w", name, ErrNativeUnsupported)
	}
	return native, nil
}

func (r *Router) Execute(ctx context.Context, req NativeRequest) (NativeResponse, error) {
	backend, err := r.nativeBackend(req.Store)
	if err != nil {
		var empty NativeResponse
		return empty, err
	}
	return backend.Execute(ctx, req)
}

func (r *Router) Scan(ctx context.Context, req ScanRequest, send func([]Document) error) error {
	backend, err := r.nativeBackend(req.Request.Store)
	if err != nil {
		return err
	}
	return backend.Scan(ctx, req, send)
}
