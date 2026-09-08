package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var ErrNativeUnsupported = errors.New("native operation is not supported")

// NativeStorage executes backend-native commands and managed cursor queries.
// Native mutations use database semantics independently of the record API.
type NativeStorage interface {
	Execute(context.Context, NativeRequest) (NativeResponse, error)
	Query(context.Context, QueryRequest) (QueryResponse, error)
	Count(context.Context, CountRequest) (CountResponse, error)
	Scan(context.Context, ScanRequest, func([]Document) error) error
}

type NativeRequest struct {
	Store       string
	Namespace   string
	Method      string
	Path        string
	Query       string
	Headers     http.Header
	ContentType string
	Payload     []byte
	MaxBytes    int
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

type QueryRequest struct {
	Request    NativeRequest
	Offset     int64
	PageSize   int
	Sort       []SortField
	Projection *Projection
}

type CountRequest struct {
	Request NativeRequest
}

type CountResponse struct {
	Count     uint64
	Estimated bool
}

type SortField struct {
	Field      string
	Descending bool
}

type Projection struct {
	Fields  []string
	Exclude bool
}

func (r QueryRequest) Validate() error {
	if r.Offset < 0 || r.PageSize < 1 || r.PageSize > 1000 {
		return InvalidArgumentError(errors.New("invalid query offset or page size"))
	}
	seen := make(map[string]bool)
	for _, field := range r.Sort {
		if strings.TrimSpace(field.Field) == "" || seen[field.Field] {
			return InvalidArgumentError(errors.New("sort fields must be nonempty and unique"))
		}
		seen[field.Field] = true
	}
	if r.Projection != nil {
		seen = make(map[string]bool)
		for _, field := range r.Projection.Fields {
			if strings.TrimSpace(field) == "" || seen[field] {
				return InvalidArgumentError(errors.New("projection fields must be nonempty and unique"))
			}
			seen[field] = true
		}
	}
	return nil
}

type QueryResponse struct {
	Documents []Document
	HasMore   bool
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

func (r *Router) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	backend, err := r.nativeBackend(req.Request.Store)
	if err != nil {
		var empty QueryResponse
		return empty, err
	}
	return backend.Query(ctx, req)
}

func (r *Router) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	backend, err := r.nativeBackend(req.Request.Store)
	if err != nil {
		var empty CountResponse
		return empty, err
	}
	return backend.Count(ctx, req)
}
