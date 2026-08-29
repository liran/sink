package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type RoutingPublisher struct {
	publishers map[string]Publisher
}

func NewRoutingPublisher(publishers map[string]Publisher) (*RoutingPublisher, error) {
	if len(publishers) == 0 {
		return nil, errors.New("create routing publisher: store publishers are required")
	}
	configured := make(map[string]Publisher, len(publishers))
	for store, publisher := range publishers {
		if store == "" || publisher == nil {
			return nil, errors.New("create routing publisher: store names and publishers are required")
		}
		configured[store] = publisher
	}
	router := &RoutingPublisher{publishers: configured}
	return router, nil
}

type indexedMutation struct {
	index    int
	mutation Mutation
}

func (p *RoutingPublisher) Publish(ctx context.Context, req PublishRequest) (PublishResponse, error) {
	response := PublishResponse{Results: make([]PublishResult, len(req.Mutations))}
	grouped := make(map[string][]indexedMutation)
	for index, mutation := range req.Mutations {
		store, err := MutationStore(mutation)
		if err != nil {
			response.Results[index] = PublishResult{Status: PublishStatusFailed, Err: err}
			continue
		}
		if _, exists := p.publishers[store]; !exists {
			err := fmt.Errorf("asynchronous publisher is not configured for store %q", store)
			response.Results[index] = PublishResult{Status: PublishStatusFailed, Err: err}
			continue
		}
		work := indexedMutation{index: index, mutation: mutation}
		grouped[store] = append(grouped[store], work)
	}

	var wait sync.WaitGroup
	for store, operations := range grouped {
		wait.Add(1)
		go func() {
			defer wait.Done()
			p.publishStore(ctx, store, operations, response.Results)
		}()
	}
	wait.Wait()
	return response, nil
}

func (p *RoutingPublisher) publishStore(ctx context.Context, store string, operations []indexedMutation, results []PublishResult) {
	mutations := make([]Mutation, len(operations))
	for index, operation := range operations {
		mutations[index] = operation.mutation
	}
	request := PublishRequest{Mutations: mutations}
	response, err := p.publishers[store].Publish(ctx, request)
	if err != nil {
		for _, operation := range operations {
			results[operation.index] = PublishResult{Status: PublishStatusFailed, Err: err}
		}
		return
	}
	if len(response.Results) != len(operations) {
		err := errors.New("store publisher returned an invalid result count")
		for _, operation := range operations {
			results[operation.index] = PublishResult{Status: PublishStatusFailed, Err: err}
		}
		return
	}
	for index, operation := range operations {
		results[operation.index] = response.Results[index]
	}
}
