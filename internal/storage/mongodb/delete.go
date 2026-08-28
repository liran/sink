package mongodb

import (
	"context"
	"errors"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type deleteWork struct {
	index      int
	collection resolvedCollection
	id         any
}

type deleteGroup struct {
	collection resolvedCollection
	operations []deleteWork
}

func (s *Store) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{
		Results: make([]storage.DeleteResult, len(req.Operations)),
	}
	groups := make(map[string]*deleteGroup)
	for index, operation := range req.Operations {
		collection, err := s.resolve(operation.Address)
		if err != nil {
			setDeleteError(&response.Results[index], err)
			continue
		}
		id, err := mongoID(operation.Address.Key)
		if err != nil {
			setDeleteError(&response.Results[index], err)
			continue
		}
		work := deleteWork{index: index, collection: collection, id: id}
		key := collection.key()
		group := groups[key]
		if group == nil {
			group = &deleteGroup{collection: collection}
			groups[key] = group
		}
		group.operations = append(group.operations, work)
	}

	for _, group := range groups {
		s.deleteGroup(ctx, group, response.Results)
	}
	return response, nil
}

func (s *Store) deleteGroup(ctx context.Context, group *deleteGroup, results []storage.DeleteResult) {
	models := make([]mongo.WriteModel, 0, len(group.operations))
	for _, operation := range group.operations {
		filter := bson.D{{Key: "_id", Value: operation.id}}
		model := mongo.NewDeleteOneModel()
		model.SetFilter(filter)
		models = append(models, model)
	}
	bulkOptions := options.BulkWrite().SetOrdered(false)
	result, err := group.collection.value.BulkWrite(ctx, models, bulkOptions)
	if err == nil && result.Acknowledged {
		for _, operation := range group.operations {
			results[operation.index].Status = storage.DeleteStatusApplied
		}
		return
	}
	if err == nil {
		err = errors.New("MongoDB did not acknowledge bulk delete")
	}

	var bulkError mongo.BulkWriteException
	if !errors.As(err, &bulkError) || bulkError.WriteConcernError != nil {
		for _, operation := range group.operations {
			setDeleteError(&results[operation.index], err)
		}
		return
	}
	for _, operation := range group.operations {
		results[operation.index].Status = storage.DeleteStatusApplied
	}
	for _, writeError := range bulkError.WriteErrors {
		if writeError.Index < 0 || writeError.Index >= len(group.operations) {
			continue
		}
		operation := group.operations[writeError.Index]
		setDeleteError(&results[operation.index], &writeError)
	}
}

func setDeleteError(result *storage.DeleteResult, err error) {
	result.Status = storage.DeleteStatusFailed
	result.Err = err
}
