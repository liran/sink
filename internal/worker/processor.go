// Package worker applies queued mutations through the synchronous Sink path.
package worker

import (
	"context"
	"errors"
	"fmt"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
)

type Applier interface {
	Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error)
	Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error)
}

type Processor struct {
	applier Applier
}

func NewProcessor(applier Applier) (*Processor, error) {
	if applier == nil {
		return nil, errors.New("create mutation processor: applier is required")
	}
	processor := &Processor{applier: applier}
	return processor, nil
}

func (p *Processor) Handle(ctx context.Context, mutation queue.Mutation) error {
	mutations := []queue.Mutation{mutation}
	results := p.HandleBatch(ctx, mutations)
	return results[0]
}

type mutationWork struct {
	index    int
	routing  string
	mutation queue.Mutation
}

func (p *Processor) HandleBatch(ctx context.Context, mutations []queue.Mutation) []error {
	results := make([]error, len(mutations))
	prepared := make([]mutationWork, 0, len(mutations))
	for index, mutation := range mutations {
		key, err := queue.MutationKey(mutation)
		if err != nil {
			results[index] = NewApplyError("apply queued mutation", false, err)
			continue
		}
		work := mutationWork{index: index, routing: string(key), mutation: mutation}
		prepared = append(prepared, work)
	}

	waves := buildMutationWaves(prepared)
	blocked := make(map[string]error)
	for _, wave := range waves {
		writes := make([]mutationWork, 0, len(wave))
		deletes := make([]mutationWork, 0, len(wave))
		for _, work := range wave {
			if blockedErr := blocked[work.routing]; blockedErr != nil {
				results[work.index] = blockedErr
				continue
			}
			if work.mutation.Write != nil {
				writes = append(writes, work)
				continue
			}
			deletes = append(deletes, work)
		}
		p.applyWrites(ctx, writes, results)
		p.applyDeletes(ctx, deletes, results)
		for _, work := range wave {
			if results[work.index] != nil {
				blocked[work.routing] = results[work.index]
			}
		}
	}
	return results
}

func buildMutationWaves(operations []mutationWork) [][]mutationWork {
	waves := make([][]mutationWork, 0)
	occurrences := make(map[string]int)
	for _, operation := range operations {
		waveIndex := occurrences[operation.routing]
		occurrences[operation.routing]++
		for len(waves) <= waveIndex {
			waves = append(waves, nil)
		}
		waves[waveIndex] = append(waves[waveIndex], operation)
	}
	return waves
}

func (p *Processor) applyWrites(ctx context.Context, operations []mutationWork, results []error) {
	if len(operations) == 0 {
		return
	}
	writeOperations := make([]*sink.WriteOperation, 0, len(operations))
	for _, operation := range operations {
		writeOperations = append(writeOperations, operation.mutation.Write)
	}
	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     writeOperations,
	}
	response, err := p.applier.Write(ctx, request)
	if err != nil {
		for _, operation := range operations {
			results[operation.index] = NewApplyError("apply queued writes", true, err)
		}
		return
	}
	if len(response.GetResults()) != len(operations) {
		err := errors.New("sink returned an invalid queued write result count")
		for _, operation := range operations {
			results[operation.index] = NewApplyError("apply queued writes", true, err)
		}
		return
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() == sink.WriteStatus_WRITE_STATUS_APPLIED {
			continue
		}
		operation := operations[index]
		results[operation.index] = resultApplyError("apply queued write", result.GetFailure())
	}
}

func (p *Processor) applyDeletes(ctx context.Context, operations []mutationWork, results []error) {
	if len(operations) == 0 {
		return
	}
	deleteOperations := make([]*sink.DeleteOperation, 0, len(operations))
	for _, operation := range operations {
		deleteOperations = append(deleteOperations, operation.mutation.Delete)
	}
	request := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     deleteOperations,
	}
	response, err := p.applier.Delete(ctx, request)
	if err != nil {
		for _, operation := range operations {
			results[operation.index] = NewApplyError("apply queued deletes", true, err)
		}
		return
	}
	if len(response.GetResults()) != len(operations) {
		err := errors.New("sink returned an invalid queued delete result count")
		for _, operation := range operations {
			results[operation.index] = NewApplyError("apply queued deletes", true, err)
		}
		return
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() == sink.DeleteStatus_DELETE_STATUS_APPLIED {
			continue
		}
		operation := operations[index]
		results[operation.index] = resultApplyError("apply queued delete", result.GetFailure())
	}
}

type ApplyError struct {
	operation string
	retryable bool
	cause     error
}

func NewApplyError(operation string, retryable bool, cause error) *ApplyError {
	applyError := &ApplyError{
		operation: operation,
		retryable: retryable,
		cause:     cause,
	}
	return applyError
}

func (e *ApplyError) Error() string {
	return fmt.Sprintf("%s: %v", e.operation, e.cause)
}

func (e *ApplyError) Unwrap() error {
	return e.cause
}

func (e *ApplyError) Retryable() bool {
	return e.retryable
}

func resultApplyError(operation string, failure *sink.Failure) error {
	if failure == nil {
		err := errors.New("sink returned a mutation failure without details")
		return NewApplyError(operation, false, err)
	}
	cause := errors.New(failure.GetMessage())
	return NewApplyError(operation, failure.GetRetryable(), cause)
}
