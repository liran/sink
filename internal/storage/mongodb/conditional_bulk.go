package mongodb

import (
	"context"
	"errors"
	"strings"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	clientBulkUnavailable = 1
	clientBulkAvailable   = 2
)

// MongoDB 8 adds per-operation results to client bulk writes. Earlier servers
// keep the individual conditional-write path; aggregate collection bulk counts
// cannot identify which CAS operations matched.
func (s *Store) supportsClientBulk(ctx context.Context) bool {
	if capability := s.clientBulkCapability.Load(); capability != 0 {
		return capability == clientBulkAvailable
	}
	command := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		MaxWireVersion int `bson:"maxWireVersion"`
	}
	err := s.client.Database("admin").RunCommand(ctx, command).Decode(&hello)
	if err != nil {
		// A transient discovery failure must not permanently disable the path.
		return false
	}
	capability := uint32(clientBulkUnavailable)
	if hello.MaxWireVersion >= 25 {
		capability = clientBulkAvailable
	}
	s.clientBulkCapability.Store(capability)
	return capability == clientBulkAvailable
}

func (s *Store) writeConditionalBulk(ctx context.Context, operations []writeWork, results []storage.WriteResult) {
	writes := make([]mongo.ClientBulkWrite, 0, len(operations))
	prepared := make([]writeWork, 0, len(operations))
	for _, operation := range operations {
		filter, err := s.preconditionFilter(operation)
		if err != nil {
			setWriteError(&results[operation.index], err)
			continue
		}
		var model mongo.ClientWriteModel
		if useReplacementPipeline(operation.replacement) {
			literal := bson.D{{Key: "$literal", Value: operation.replacement}}
			stage := bson.D{{Key: "$replaceWith", Value: literal}}
			pipeline := mongo.Pipeline{stage}
			model = mongo.NewClientUpdateOneModel().SetFilter(filter).SetUpdate(pipeline)
		} else {
			model = mongo.NewClientReplaceOneModel().SetFilter(filter).SetReplacement(operation.replacement)
		}
		write := mongo.ClientBulkWrite{Database: operation.collection.database, Collection: operation.collection.collection, Model: model}
		writes = append(writes, write)
		prepared = append(prepared, operation)
	}
	if len(writes) == 0 {
		return
	}
	select {
	case s.groups <- struct{}{}:
		defer func() { <-s.groups }()
	case <-ctx.Done():
		for _, operation := range prepared {
			setWriteError(&results[operation.index], storage.BackendError(ctx.Err()))
		}
		return
	}
	select {
	case s.writes <- struct{}{}:
	case <-ctx.Done():
		for _, operation := range prepared {
			setWriteError(&results[operation.index], storage.BackendError(ctx.Err()))
		}
		return
	}
	bulkOptions := options.ClientBulkWrite().SetOrdered(false).SetVerboseResults(true)
	response, err := s.client.BulkWrite(ctx, writes, bulkOptions)
	<-s.writes
	if unsupportedClientBulk(response, err) {
		// Command availability can differ from cached wire-version discovery,
		// for example after a primary changes. Retry only a command rejection
		// with no evidence that any item ran; never retry ambiguous outcomes.
		s.clientBulkCapability.Store(clientBulkUnavailable)
		s.writeConditional(ctx, prepared, results)
		return
	}
	outcome := conditionalBulkOutcome{operations: prepared, results: results, response: response, err: err}
	applyConditionalBulkOutcome(outcome)
}

func useReplacementPipeline(document bson.Raw) bool {
	// A literal replacement pipeline preserves the complete document while
	// letting MongoDB log a small delta instead of copying large unchanged
	// fields into the oplog. Small documents keep the cheaper replacement path.
	if len(document) < 16<<10 {
		return false
	}
	elements, err := document.Elements()
	if err != nil {
		return false
	}
	seen := make(map[string]bool, len(elements))
	for _, element := range elements {
		key := element.Key()
		// Keep the original validation and representation of unusual documents.
		if strings.HasPrefix(key, "$") || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

func unsupportedClientBulk(response *mongo.ClientBulkWriteResult, err error) bool {
	var failure mongo.ClientBulkWriteException
	var commandError mongo.CommandError
	commandNotFound := errors.As(err, &commandError) && commandError.Code == 59
	if errors.As(err, &failure) {
		commandNotFound = failure.WriteError != nil && failure.WriteError.Code == 59 && len(failure.WriteConcernErrors) == 0 && len(failure.WriteErrors) == 0
	}
	if !commandNotFound {
		return false
	}
	return emptyClientBulkResult(response) && emptyClientBulkResult(failure.PartialResult)
}

func emptyClientBulkResult(response *mongo.ClientBulkWriteResult) bool {
	return response == nil || (response.MatchedCount == 0 && response.ModifiedCount == 0 && response.InsertedCount == 0 && response.DeletedCount == 0 && response.UpsertedCount == 0 && len(response.UpdateResults) == 0 && len(response.InsertResults) == 0 && len(response.DeleteResults) == 0)
}

type conditionalBulkOutcome struct {
	operations []writeWork
	results    []storage.WriteResult
	response   *mongo.ClientBulkWriteResult
	err        error
}

func applyConditionalBulkOutcome(outcome conditionalBulkOutcome) {
	response, err := outcome.response, outcome.err
	var failure mongo.ClientBulkWriteException
	itemErrorsOnly := errors.As(err, &failure) && failure.WriteError == nil && len(failure.WriteConcernErrors) == 0
	trusted := response != nil && response.Acknowledged && response.HasVerboseResults && (err == nil || itemErrorsOnly)
	if itemErrorsOnly {
		for index := range failure.WriteErrors {
			if index < 0 || index >= len(outcome.operations) {
				trusted = false
			}
		}
	}
	for index, operation := range outcome.operations {
		result := &outcome.results[operation.index]
		if !trusted {
			cause := err
			if cause == nil {
				cause = errors.New("MongoDB did not return acknowledged per-operation bulk results")
			}
			setWriteError(result, storage.BackendError(cause))
			continue
		}
		if writeError, failed := failure.WriteErrors[index]; failed {
			setWriteError(result, classifyWriteError(writeError))
			continue
		}
		updated, found := response.UpdateResults[index]
		if !found || updated.UpsertedID != nil || updated.MatchedCount < 0 || updated.MatchedCount > 1 {
			setWriteError(result, storage.BackendError(errors.New("MongoDB returned an incomplete conditional bulk result")))
			continue
		}
		if updated.MatchedCount == 0 {
			result.Status = storage.WriteStatusPreconditionFailed
			continue
		}
		setWriteApplied(result, operation.revision)
	}
}
