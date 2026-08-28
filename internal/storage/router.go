package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Router dispatches operations to a configured storage by Address.Store.
type Router struct {
	backends map[string]Storage
}

func NewRouter(backends map[string]Storage) (*Router, error) {
	if len(backends) == 0 {
		return nil, errors.New("create storage router: at least one backend is required")
	}
	copied := make(map[string]Storage, len(backends))
	for rawName, backend := range backends {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, errors.New("create storage router: backend name is required")
		}
		if backend == nil {
			return nil, fmt.Errorf("create storage router: backend %q is required", name)
		}
		if _, exists := copied[name]; exists {
			return nil, fmt.Errorf("create storage router: duplicate backend %q", name)
		}
		copied[name] = backend
	}
	router := &Router{backends: copied}
	return router, nil
}

type routedRead struct {
	backend    Storage
	operations []ReadOperation
	indexes    []int
}

func (r *Router) Read(ctx context.Context, req ReadRequest) (ReadResponse, error) {
	response := ReadResponse{Results: make([]ReadResult, len(req.Operations))}
	groups := make(map[string]*routedRead)
	for index, operation := range req.Operations {
		name := operation.Address.Store
		backend, exists := r.backends[name]
		if !exists {
			response.Results[index] = missingReadResult(name)
			continue
		}
		group := groups[name]
		if group == nil {
			group = &routedRead{backend: backend}
			groups[name] = group
		}
		group.operations = append(group.operations, operation)
		group.indexes = append(group.indexes, index)
	}
	for name, group := range groups {
		request := ReadRequest{Operations: group.operations}
		routed, err := group.backend.Read(ctx, request)
		if err != nil {
			r.setReadGroupError(&response, group, fmt.Errorf("read storage %q: %w", name, err))
			continue
		}
		if len(routed.Results) != len(group.operations) {
			err = fmt.Errorf("storage %q returned %d read results for %d operations", name, len(routed.Results), len(group.operations))
			r.setReadGroupError(&response, group, err)
			continue
		}
		for resultIndex, result := range routed.Results {
			response.Results[group.indexes[resultIndex]] = result
		}
	}
	return response, nil
}

func (r *Router) setReadGroupError(response *ReadResponse, group *routedRead, err error) {
	for _, index := range group.indexes {
		response.Results[index] = ReadResult{Status: ReadStatusFailed, Err: err}
	}
}

func missingReadResult(name string) ReadResult {
	err := fmt.Errorf("storage %q is not configured", name)
	result := ReadResult{Status: ReadStatusFailed, Err: err}
	return result
}

type routedWrite struct {
	backend    Storage
	operations []WriteOperation
	indexes    []int
}

func (r *Router) Write(ctx context.Context, req WriteRequest) (WriteResponse, error) {
	response := WriteResponse{Results: make([]WriteResult, len(req.Operations))}
	groups := make(map[string]*routedWrite)
	for index, operation := range req.Operations {
		name := operation.Address.Store
		backend, exists := r.backends[name]
		if !exists {
			response.Results[index] = missingWriteResult(name)
			continue
		}
		group := groups[name]
		if group == nil {
			group = &routedWrite{backend: backend}
			groups[name] = group
		}
		group.operations = append(group.operations, operation)
		group.indexes = append(group.indexes, index)
	}
	for name, group := range groups {
		request := WriteRequest{Operations: group.operations}
		routed, err := group.backend.Write(ctx, request)
		if err != nil {
			r.setWriteGroupError(&response, group, fmt.Errorf("write storage %q: %w", name, err))
			continue
		}
		if len(routed.Results) != len(group.operations) {
			err = fmt.Errorf("storage %q returned %d write results for %d operations", name, len(routed.Results), len(group.operations))
			r.setWriteGroupError(&response, group, err)
			continue
		}
		for resultIndex, result := range routed.Results {
			response.Results[group.indexes[resultIndex]] = result
		}
	}
	return response, nil
}

func (r *Router) setWriteGroupError(response *WriteResponse, group *routedWrite, err error) {
	for _, index := range group.indexes {
		response.Results[index] = WriteResult{Status: WriteStatusFailed, Err: err}
	}
}

func missingWriteResult(name string) WriteResult {
	err := fmt.Errorf("storage %q is not configured", name)
	result := WriteResult{Status: WriteStatusFailed, Err: err}
	return result
}

type routedDelete struct {
	backend    Storage
	operations []DeleteOperation
	indexes    []int
}

func (r *Router) Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error) {
	response := DeleteResponse{Results: make([]DeleteResult, len(req.Operations))}
	groups := make(map[string]*routedDelete)
	for index, operation := range req.Operations {
		name := operation.Address.Store
		backend, exists := r.backends[name]
		if !exists {
			response.Results[index] = missingDeleteResult(name)
			continue
		}
		group := groups[name]
		if group == nil {
			group = &routedDelete{backend: backend}
			groups[name] = group
		}
		group.operations = append(group.operations, operation)
		group.indexes = append(group.indexes, index)
	}
	for name, group := range groups {
		request := DeleteRequest{Operations: group.operations}
		routed, err := group.backend.Delete(ctx, request)
		if err != nil {
			r.setDeleteGroupError(&response, group, fmt.Errorf("delete storage %q: %w", name, err))
			continue
		}
		if len(routed.Results) != len(group.operations) {
			err = fmt.Errorf("storage %q returned %d delete results for %d operations", name, len(routed.Results), len(group.operations))
			r.setDeleteGroupError(&response, group, err)
			continue
		}
		for resultIndex, result := range routed.Results {
			response.Results[group.indexes[resultIndex]] = result
		}
	}
	return response, nil
}

func (r *Router) setDeleteGroupError(response *DeleteResponse, group *routedDelete, err error) {
	for _, index := range group.indexes {
		response.Results[index] = DeleteResult{Status: DeleteStatusFailed, Err: err}
	}
}

func missingDeleteResult(name string) DeleteResult {
	err := fmt.Errorf("storage %q is not configured", name)
	result := DeleteResult{Status: DeleteStatusFailed, Err: err}
	return result
}
