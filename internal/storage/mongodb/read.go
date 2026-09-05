package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type readWork struct {
	index      int
	collection resolvedCollection
	id         any
	idKey      string
}

type readGroup struct {
	collection resolvedCollection
	operations []readWork
}

func (s *Store) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	if req.Budget == nil {
		req.Budget = storage.NewReadBudget(storage.DefaultMaxReadBytes)
	}
	response := storage.ReadResponse{
		Results: make([]storage.ReadResult, len(req.Operations)),
	}
	groups := make(map[string]*readGroup)
	for index, operation := range req.Operations {
		collection, err := s.resolve(operation.Address)
		if err != nil {
			setReadError(&response.Results[index], err)
			continue
		}
		id, err := mongoID(operation.Address.Key)
		if err != nil {
			setReadError(&response.Results[index], err)
			continue
		}
		encodedID, err := rawValue(id)
		if err != nil {
			setReadError(&response.Results[index], err)
			continue
		}
		work := readWork{
			index:      index,
			collection: collection,
			id:         id,
			idKey:      rawValueKey(encodedID),
		}
		group := groups[collection.key()]
		if group == nil {
			group = &readGroup{collection: collection}
			groups[collection.key()] = group
		}
		group.operations = append(group.operations, work)
	}

	var reads sync.WaitGroup
	reads.Add(len(groups))
	for _, group := range groups {
		select {
		case s.groups <- struct{}{}:
		case <-ctx.Done():
			s.setReadGroupError(group, response.Results, storage.BackendError(ctx.Err()))
			reads.Done()
			continue
		}
		go func() {
			defer reads.Done()
			defer func() {
				<-s.groups
			}()
			s.readGroup(ctx, group, response.Results, req.Budget)
		}()
	}
	reads.Wait()
	return response, nil
}

func (s *Store) readGroup(ctx context.Context, group *readGroup, results []storage.ReadResult, budget *storage.ReadBudget) {
	ids := make([]any, 0, len(group.operations))
	indexesByID := make(map[string][]int, len(group.operations))
	for _, operation := range group.operations {
		ids = append(ids, operation.id)
		indexesByID[operation.idKey] = append(indexesByID[operation.idKey], operation.index)
	}
	inFilter := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: inFilter}}
	cursor, err := group.collection.value.Find(ctx, filter)
	if err != nil {
		s.setReadGroupError(group, results, storage.BackendError(err))
		return
	}
	defer func() {
		_ = cursor.Close(ctx)
	}()

	found := make(map[string]bool, len(group.operations))
	for cursor.Next(ctx) {
		raw := cursor.Current
		id, lookupErr := raw.LookupErr("_id")
		if lookupErr != nil {
			s.setReadGroupError(group, results, fmt.Errorf("read MongoDB _id: %w", lookupErr))
			return
		}
		idKey := rawValueKey(id)
		indexes := indexesByID[idKey]
		if len(indexes) == 0 {
			continue
		}
		admitted := make([]int, 0, len(indexes))
		for _, index := range indexes {
			if err := budget.Reserve(len(raw)); err != nil {
				setReadError(&results[index], err)
			} else {
				admitted = append(admitted, index)
			}
		}
		if len(admitted) == 0 {
			continue
		}
		indexes = admitted
		document, revision, decodeErr := s.userDocument(raw)
		if decodeErr != nil {
			for _, index := range indexes {
				setReadError(&results[index], decodeErr)
			}
			found[idKey] = true
			continue
		}
		for _, index := range indexes {
			results[index] = storage.ReadResult{
				Status:   storage.ReadStatusFound,
				Document: cloneDocument(document),
				Revision: cloneRevision(revision),
			}
		}
		found[idKey] = true
	}
	if err := cursor.Err(); err != nil {
		s.setReadGroupError(group, results, storage.BackendError(err))
		return
	}

	for _, operation := range group.operations {
		if results[operation.index].Status != storage.ReadStatusUnspecified {
			continue
		}
		if found[operation.idKey] {
			continue
		}
		results[operation.index].Status = storage.ReadStatusNotFound
	}
}

func (s *Store) setReadGroupError(group *readGroup, results []storage.ReadResult, err error) {
	for _, operation := range group.operations {
		if results[operation.index].Status == storage.ReadStatusUnspecified {
			setReadError(&results[operation.index], err)
		}
	}
}

func setReadError(result *storage.ReadResult, err error) {
	result.Status = storage.ReadStatusFailed
	result.Err = err
}

func cloneDocument(document storage.Document) storage.Document {
	return storage.CloneDocument(document)
}

func cloneRevision(revision storage.Revision) storage.Revision {
	cloned := storage.Revision{Data: bytes.Clone(revision.Data)}
	return cloned
}
