package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func appendQueryStages(command bson.D, stages bson.A) (bson.D, error) {
	result := append(bson.D(nil), command...)
	for index, field := range result {
		if field.Key != "pipeline" {
			continue
		}
		pipeline, ok := field.Value.(bson.A)
		if !ok {
			return nil, errors.New("aggregate requires a pipeline array")
		}
		pipeline = append(append(bson.A(nil), pipeline...), stages...)
		result[index].Value = pipeline
		return result, nil
	}
	return nil, errors.New("aggregate requires a pipeline array")
}

func (s *Store) Query(ctx context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	var empty storage.QueryResponse
	if err := req.Validate(); err != nil {
		return empty, err
	}
	command, err := validateNativeCommand(req.Request, true)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	limit := int64(req.PageSize + 1)
	sort := make(bson.D, 0, len(req.Sort))
	for _, field := range req.Sort {
		direction := int32(1)
		if field.Descending {
			direction = -1
		}
		item := bson.E{Key: field.Field, Value: direction}
		sort = append(sort, item)
	}
	projection := bson.D{}
	if req.Projection != nil {
		mode := int32(1)
		if req.Projection.Exclude {
			mode = 0
		}
		for _, field := range req.Projection.Fields {
			item := bson.E{Key: field, Value: mode}
			projection = append(projection, item)
		}
	}
	switch command[0].Key {
	case "find":
		paged := make(bson.D, 0, len(command)+2)
		for _, field := range command {
			if (field.Key == "sort" && len(sort) > 0) || (field.Key == "projection" && req.Projection != nil) {
				continue
			}
			if field.Key != "skip" && field.Key != "limit" && field.Key != "batchSize" {
				paged = append(paged, field)
			}
		}
		skipField := bson.E{Key: "skip", Value: req.Offset}
		limitField := bson.E{Key: "limit", Value: limit}
		command = append(paged, skipField, limitField)
		if len(sort) > 0 {
			field := bson.E{Key: "sort", Value: sort}
			command = append(command, field)
		}
		if req.Projection != nil {
			field := bson.E{Key: "projection", Value: projection}
			command = append(command, field)
		}
	case "aggregate":
		skipStage := bson.D{{Key: "$skip", Value: req.Offset}}
		limitStage := bson.D{{Key: "$limit", Value: limit}}
		stages := bson.A{}
		if len(sort) > 0 {
			stage := bson.D{{Key: "$sort", Value: sort}}
			stages = append(stages, stage)
		}
		if len(projection) > 0 {
			stage := bson.D{{Key: "$project", Value: projection}}
			stages = append(stages, stage)
		}
		stages = append(stages, skipStage, limitStage)
		command, err = appendQueryStages(command, stages)
		if err != nil {
			return empty, storage.InvalidArgumentError(err)
		}
	default:
		return empty, storage.InvalidArgumentError(errors.New("Query requires find or read-only aggregate"))
	}
	request := req.Request
	request.Payload, err = bson.Marshal(command)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	result := storage.QueryResponse{}
	budget := storage.NewReadBudget(request.MaxBytes)
	visit := func(documents []storage.Document) error {
		for _, document := range documents {
			if len(result.Documents) == req.PageSize {
				result.HasMore = true
				continue
			}
			if err := budget.Reserve(len(document.Payload)); err != nil {
				return err
			}
			result.Documents = append(result.Documents, document)
		}
		return nil
	}
	scan := storage.ScanRequest{Request: request, BatchSize: req.PageSize}
	if err := s.Scan(ctx, scan, visit); err != nil {
		return empty, err
	}
	return result, nil
}

func countPipeline(command bson.D) (bson.D, error) {
	if command[0].Key == "find" {
		pipeline := bson.A{}
		aggregate := bson.D{{Key: "aggregate", Value: command[0].Value}}
		for _, field := range command[1:] {
			switch field.Key {
			case "filter":
				match := bson.D{{Key: "$match", Value: field.Value}}
				pipeline = append(pipeline, match)
			case "skip", "limit", "batchSize", "sort", "projection":
				// Count observes matching documents before find pagination.
			case "hint", "collation", "readConcern", "let", "comment", "allowDiskUse":
				aggregate = append(aggregate, field)
			default:
				return nil, fmt.Errorf("Count does not support find option %q", field.Key)
			}
		}
		pipelineField := bson.E{Key: "pipeline", Value: pipeline}
		command = append(aggregate, pipelineField)
	} else if command[0].Key != "aggregate" {
		return nil, errors.New("Count requires find or read-only aggregate")
	}
	count := bson.D{{Key: "$count", Value: "count"}}
	stages := bson.A{count}
	return appendQueryStages(command, stages)
}

func (s *Store) Count(ctx context.Context, req storage.NativeRequest) (uint64, error) {
	command, err := validateNativeCommand(req, true)
	if err != nil {
		return 0, storage.InvalidArgumentError(err)
	}
	command, err = countPipeline(command)
	if err != nil {
		return 0, storage.InvalidArgumentError(err)
	}
	req.Payload, err = bson.Marshal(command)
	if err != nil {
		return 0, storage.InvalidArgumentError(err)
	}
	var count uint64
	seen := false
	visit := func(documents []storage.Document) error {
		for _, document := range documents {
			value := bson.Raw(document.Payload).Lookup("count")
			if seen || (value.Type != bson.TypeInt32 && value.Type != bson.TypeInt64) || value.AsInt64() < 0 {
				return errors.New("invalid native count result")
			}
			seen = true
			count = uint64(value.AsInt64())
		}
		return nil
	}
	scan := storage.ScanRequest{Request: req, BatchSize: 1}
	if err := s.Scan(ctx, scan, visit); err != nil {
		return 0, err
	}
	return count, nil
}
