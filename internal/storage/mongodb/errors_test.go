package mongodb

import (
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestBulkItemFailuresNeedPositiveRecordRejectionEvidence(t *testing.T) {
	for _, code := range []int{13, 91, 189, 10107, 11600, 11602, 99999} {
		failure := mongo.WriteError{Code: code, Message: "dependency failed without a retry label"}
		err := classifyWriteError(failure)
		kind, retryable := storage.ErrorDetails(err)
		if kind != storage.ErrorCodeUnavailable || !retryable {
			t.Errorf("environment error %d would be quarantined: %v", code, err)
		}
	}
	for _, code := range []int{66, 121, 10334} {
		failure := mongo.WriteError{Code: code, Message: "document rejected"}
		err := classifyWriteError(failure)
		kind, retryable := storage.ErrorDetails(err)
		if kind != storage.ErrorCodeInvalidArgument || retryable {
			t.Errorf("known bad record %d would block recovery: %v", code, err)
		}
		exception := mongo.WriteException{WriteErrors: []mongo.WriteError{failure}}
		_, retryable = storage.ErrorDetails(classifyOperationError(exception))
		if retryable {
			t.Errorf("single-record rejection %d lost its classification", code)
		}
		concern := &mongo.WriteConcernError{Code: 64, Message: "commit acknowledgement unknown"}
		exception.WriteConcernError = concern
		_, retryable = storage.ErrorDetails(classifyOperationError(exception))
		if !retryable {
			t.Errorf("write concern uncertainty %d was quarantined", code)
		}
	}
}

func TestMalformedBulkFailureCannotAcknowledgeUnidentifiedRecords(t *testing.T) {
	for _, indexes := range [][]int{nil, {-1}, {2}, {0, 0}} {
		failure := mongo.BulkWriteException{}
		for _, index := range indexes {
			item := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: index, Code: 91}}
			failure.WriteErrors = append(failure.WriteErrors, item)
		}
		if validBulkFailures(failure, 2) {
			t.Errorf("malformed failure indexes accepted: %v", indexes)
		}
	}
	first := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 1, Code: 121}}
	second := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 0, Code: 91}}
	failure := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{first, second}}
	if !validBulkFailures(failure, 2) {
		t.Fatal("valid unordered item failures rejected")
	}
}
