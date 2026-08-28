// Package queue defines the durable asynchronous mutation publisher contract.
package queue

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
)

type Publisher interface {
	Publish(ctx context.Context, req PublishRequest) (PublishResponse, error)
}

type PublishRequest struct {
	Mutations []Mutation
}

type Mutation struct {
	Write  *sink.WriteOperation
	Delete *sink.DeleteOperation
}

type PublishResponse struct {
	Results []PublishResult
}

type PublishResult struct {
	Status PublishStatus
	Err    error
}

type PublishStatus uint8

const (
	PublishStatusUnspecified PublishStatus = iota
	PublishStatusAccepted
	PublishStatusFailed
)
