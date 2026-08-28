package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type writeWork struct {
	index        int
	collection   resolvedCollection
	id           any
	replacement  bson.Raw
	revision     storage.Revision
	precondition storage.Precondition
}

type writeGroup struct {
	collection resolvedCollection
	operations []writeWork
}

func (s *Store) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{
		Results: make([]storage.WriteResult, len(req.Operations)),
	}
	prepared := make([]writeWork, 0, len(req.Operations))
	for index, operation := range req.Operations {
		work, err := s.prepareWrite(index, operation)
		if err != nil {
			setWriteError(&response.Results[index], err)
			continue
		}
		prepared = append(prepared, work)
	}

	waves := buildWriteWaves(prepared, req.Operations)
	for _, wave := range waves {
		s.writeWave(ctx, wave, response.Results)
	}
	return response, nil
}

func (s *Store) prepareWrite(index int, operation storage.WriteOperation) (writeWork, error) {
	var empty writeWork
	collection, err := s.resolve(operation.Address)
	if err != nil {
		return empty, err
	}
	id, err := mongoID(operation.Address.Key)
	if err != nil {
		return empty, err
	}
	if err := validatePrecondition(operation.Precondition); err != nil {
		return empty, err
	}
	revision, err := newRevision()
	if err != nil {
		return empty, err
	}
	replacement, err := s.replacement(operation.Document, id, revision)
	if err != nil {
		return empty, err
	}
	work := writeWork{
		index:        index,
		collection:   collection,
		id:           id,
		replacement:  replacement,
		revision:     revision,
		precondition: operation.Precondition,
	}
	return work, nil
}

func validatePrecondition(precondition storage.Precondition) error {
	switch precondition.Kind {
	case storage.PreconditionNone,
		storage.PreconditionRecordExists,
		storage.PreconditionRecordNotExists,
		storage.PreconditionRevisionAbsent:
		return nil
	case storage.PreconditionRevisionMatches:
		if len(precondition.Revision.Data) == 0 {
			return errors.New("revision match precondition requires a revision")
		}
		return nil
	default:
		return fmt.Errorf("unsupported precondition kind %d", precondition.Kind)
	}
}

func buildWriteWaves(prepared []writeWork, operations []storage.WriteOperation) [][]writeWork {
	waves := make([][]writeWork, 0)
	occurrences := make(map[string]int)
	for _, work := range prepared {
		routingKey := operations[work.index].Address.RoutingKey()
		waveIndex := occurrences[routingKey]
		occurrences[routingKey]++
		for len(waves) <= waveIndex {
			waves = append(waves, nil)
		}
		waves[waveIndex] = append(waves[waveIndex], work)
	}
	return waves
}

func (s *Store) writeWave(ctx context.Context, wave []writeWork, results []storage.WriteResult) {
	bulkGroups := make(map[string]*writeGroup)
	conditional := make([]writeWork, 0, len(wave))
	for _, operation := range wave {
		if operation.precondition.Kind != storage.PreconditionNone &&
			operation.precondition.Kind != storage.PreconditionRecordNotExists {
			conditional = append(conditional, operation)
			continue
		}
		key := operation.collection.key()
		group := bulkGroups[key]
		if group == nil {
			group = &writeGroup{collection: operation.collection}
			bulkGroups[key] = group
		}
		group.operations = append(group.operations, operation)
	}

	for _, group := range bulkGroups {
		s.bulkWrite(ctx, group, results)
	}
	s.writeConditional(ctx, conditional, results)
}

func (s *Store) bulkWrite(ctx context.Context, group *writeGroup, results []storage.WriteResult) {
	models := make([]mongo.WriteModel, 0, len(group.operations))
	for _, operation := range group.operations {
		if operation.precondition.Kind == storage.PreconditionRecordNotExists {
			model := mongo.NewInsertOneModel()
			model.SetDocument(operation.replacement)
			models = append(models, model)
			continue
		}
		filter := bson.D{{Key: "_id", Value: operation.id}}
		model := mongo.NewReplaceOneModel()
		model.SetFilter(filter)
		model.SetReplacement(operation.replacement)
		model.SetUpsert(true)
		models = append(models, model)
	}

	bulkOptions := options.BulkWrite().SetOrdered(false)
	result, err := group.collection.value.BulkWrite(ctx, models, bulkOptions)
	if err == nil && result.Acknowledged {
		for _, operation := range group.operations {
			setWriteApplied(&results[operation.index], operation.revision)
		}
		return
	}
	if err == nil {
		err = errors.New("MongoDB did not acknowledge bulk write")
	}

	var bulkError mongo.BulkWriteException
	if !errors.As(err, &bulkError) || bulkError.WriteConcernError != nil {
		for _, operation := range group.operations {
			setWriteError(&results[operation.index], err)
		}
		return
	}

	for _, operation := range group.operations {
		setWriteApplied(&results[operation.index], operation.revision)
	}
	for _, writeError := range bulkError.WriteErrors {
		if writeError.Index < 0 || writeError.Index >= len(group.operations) {
			continue
		}
		operation := group.operations[writeError.Index]
		if operation.precondition.Kind == storage.PreconditionRecordNotExists && isDuplicateCode(writeError.Code) {
			results[operation.index] = storage.WriteResult{Status: storage.WriteStatusPreconditionFailed}
			continue
		}
		if operation.precondition.Kind == storage.PreconditionNone && isDuplicateCode(writeError.Code) {
			s.writeUpsert(ctx, operation, &results[operation.index])
			continue
		}
		setWriteError(&results[operation.index], &writeError)
	}
}

func (s *Store) writeUpsert(ctx context.Context, operation writeWork, result *storage.WriteResult) {
	filter := bson.D{{Key: "_id", Value: operation.id}}
	replaceOptions := options.Replace().SetUpsert(true)
	for range 3 {
		replaced, err := operation.collection.value.ReplaceOne(ctx, filter, operation.replacement, replaceOptions)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue
			}
			setWriteError(result, err)
			return
		}
		if !replaced.Acknowledged {
			setWriteError(result, errors.New("MongoDB did not acknowledge upsert retry"))
			return
		}
		setWriteApplied(result, operation.revision)
		return
	}
	setWriteError(result, errors.New("MongoDB upsert repeatedly conflicted on _id"))
}

func (s *Store) writeConditional(
	ctx context.Context,
	operations []writeWork,
	results []storage.WriteResult,
) {
	if len(operations) == 0 {
		return
	}
	workerCount := min(s.maxConcurrentWrites, len(operations))
	workChannel := make(chan writeWork)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for operation := range workChannel {
				s.writeOne(ctx, operation, &results[operation.index])
			}
		}()
	}
	for _, operation := range operations {
		workChannel <- operation
	}
	close(workChannel)
	workers.Wait()
}

func (s *Store) writeOne(ctx context.Context, operation writeWork, result *storage.WriteResult) {
	filter, err := s.preconditionFilter(operation)
	if err != nil {
		setWriteError(result, err)
		return
	}
	replaced, err := operation.collection.value.ReplaceOne(ctx, filter, operation.replacement)
	if err != nil {
		setWriteError(result, err)
		return
	}
	if !replaced.Acknowledged {
		setWriteError(result, errors.New("MongoDB did not acknowledge conditional write"))
		return
	}
	if replaced.MatchedCount == 0 {
		result.Status = storage.WriteStatusPreconditionFailed
		return
	}
	setWriteApplied(result, operation.revision)
}

func (s *Store) preconditionFilter(operation writeWork) (bson.D, error) {
	filter := bson.D{{Key: "_id", Value: operation.id}}
	switch operation.precondition.Kind {
	case storage.PreconditionRecordExists:
		return filter, nil
	case storage.PreconditionRevisionMatches:
		revision := bson.Binary{
			Subtype: 0,
			Data:    operation.precondition.Revision.Data,
		}
		field := bson.E{Key: s.metadataField + ".revision", Value: revision}
		filter = append(filter, field)
		return filter, nil
	case storage.PreconditionRevisionAbsent:
		exists := bson.D{{Key: "$exists", Value: false}}
		field := bson.E{Key: s.metadataField, Value: exists}
		filter = append(filter, field)
		return filter, nil
	default:
		return nil, fmt.Errorf("precondition kind %d cannot use conditional write", operation.precondition.Kind)
	}
}

func setWriteApplied(result *storage.WriteResult, revision storage.Revision) {
	result.Status = storage.WriteStatusApplied
	result.Revision = cloneRevision(revision)
}

func setWriteError(result *storage.WriteResult, err error) {
	result.Status = storage.WriteStatusFailed
	result.Err = err
}

func isDuplicateCode(code int) bool {
	return code == 11000 || code == 11001 || code == 12582 || code == 16460
}
