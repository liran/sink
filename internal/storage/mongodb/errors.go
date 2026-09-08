package mongodb

import (
	"errors"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func classifyOperationError(err error) error {
	var failure mongo.WriteException
	if errors.As(err, &failure) && failure.WriteConcernError == nil && len(failure.WriteErrors) == 1 {
		return classifyWriteError(failure.WriteErrors[0])
	}
	return storage.BackendError(err)
}

func classifyWriteError(failure mongo.WriteError) error {
	// These per-document replies positively identify a rejected record. Other
	// bulk item errors can be dependency failures, even without a retry label.
	switch failure.Code {
	case 66, 121, 10334: // ImmutableField, DocumentValidationFailure, BSONObjectTooLarge.
		return storage.InvalidArgumentError(failure)
	default:
		return storage.BackendError(failure)
	}
}

func validBulkFailures(failure mongo.BulkWriteException, operations int) bool {
	if len(failure.WriteErrors) == 0 || failure.WriteConcernError != nil {
		return false
	}
	seen := make(map[int]bool, len(failure.WriteErrors))
	for _, item := range failure.WriteErrors {
		if item.Index < 0 || item.Index >= operations || seen[item.Index] {
			return false
		}
		seen[item.Index] = true
	}
	return true
}
