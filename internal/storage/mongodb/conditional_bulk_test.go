package mongodb

import (
	"errors"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestReplacementPipelineKeepsSmallAndUnusualDocumentsOnOriginalPath(t *testing.T) {
	padding := strings.Repeat("x", 32<<10)
	cases := []struct {
		name  string
		value bson.D
		want  bool
	}{
		{name: "small", value: bson.D{{Key: "value", Value: 1}}},
		{name: "large literal", value: bson.D{{Key: "padding", Value: padding}, {Key: "value", Value: "$field"}}, want: true},
		{name: "dollar field", value: bson.D{{Key: "padding", Value: padding}, {Key: "$field", Value: 1}}},
		{name: "duplicate field", value: bson.D{{Key: "padding", Value: padding}, {Key: "value", Value: 1}, {Key: "value", Value: 2}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := bson.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if useReplacementPipeline(raw) != tc.want {
				t.Fatal("selected a replacement path with different document semantics")
			}
		})
	}
}

func TestConditionalBulkResultsKeepOriginalIndexes(t *testing.T) {
	revision := storage.Revision{Data: []byte("committed")}
	operations := []writeWork{{index: 2, revision: revision}, {index: 0}, {index: 1}}
	results := make([]storage.WriteResult, 3)
	response := &mongo.ClientBulkWriteResult{Acknowledged: true, HasVerboseResults: true,
		UpdateResults: map[int]mongo.ClientBulkWriteUpdateResult{0: {MatchedCount: 1}, 1: {MatchedCount: 0}}}
	failure := mongo.ClientBulkWriteException{WriteErrors: map[int]mongo.WriteError{2: {Code: 121}}}
	outcome := conditionalBulkOutcome{operations: operations, results: results, response: response, err: failure}
	applyConditionalBulkOutcome(outcome)
	if results[2].Status != storage.WriteStatusApplied || string(results[2].Revision.Data) != "committed" || results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("lost original indexes: %+v", results)
	}
	kind, retryable := storage.ErrorDetails(results[1].Err)
	if results[1].Status != storage.WriteStatusFailed || kind != storage.ErrorCodeInvalidArgument || retryable {
		t.Fatalf("document rejection lost its classification: %+v", results[1])
	}
}

func TestConditionalBulkNeverAcknowledgesUncertainWrites(t *testing.T) {
	writeConcern := mongo.WriteConcernError{Code: 64, Message: "durability unknown"}
	commandError := &mongo.WriteError{Code: 91, Message: "primary stepped down"}
	cases := []struct {
		name         string
		err          error
		acknowledged bool
		verbose      bool
		missing      bool
	}{
		{name: "network failure", err: errors.New("connection lost"), acknowledged: true, verbose: true},
		{name: "write concern", err: mongo.ClientBulkWriteException{WriteConcernErrors: []mongo.WriteConcernError{writeConcern}}, acknowledged: true, verbose: true},
		{name: "top-level error", err: mongo.ClientBulkWriteException{WriteError: commandError}, acknowledged: true, verbose: true},
		{name: "invalid error index", err: mongo.ClientBulkWriteException{WriteErrors: map[int]mongo.WriteError{9: {Code: 121}}}, acknowledged: true, verbose: true},
		{name: "unacknowledged", verbose: true},
		{name: "aggregate counts only", acknowledged: true},
		{name: "missing item", acknowledged: true, verbose: true, missing: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			operations := []writeWork{{index: 0}}
			results := make([]storage.WriteResult, 1)
			response := &mongo.ClientBulkWriteResult{Acknowledged: tc.acknowledged, HasVerboseResults: tc.verbose,
				MatchedCount: 1, UpdateResults: map[int]mongo.ClientBulkWriteUpdateResult{0: {MatchedCount: 1}}}
			if tc.missing {
				response.UpdateResults = nil
			}
			outcome := conditionalBulkOutcome{operations: operations, results: results, response: response, err: tc.err}
			applyConditionalBulkOutcome(outcome)
			_, retryable := storage.ErrorDetails(results[0].Err)
			if results[0].Status != storage.WriteStatusFailed || !retryable {
				t.Fatalf("uncertain write was acknowledged or quarantined: %+v", results[0])
			}
		})
	}
}

func TestUnsupportedClientBulkRequiresProofThatNoItemRan(t *testing.T) {
	commandError := mongo.CommandError{Code: 59}
	if !unsupportedClientBulk(nil, commandError) {
		t.Fatal("command rejection should allow legacy fallback")
	}
	item := &mongo.WriteError{Code: 59}
	failure := mongo.ClientBulkWriteException{WriteError: item}
	if !unsupportedClientBulk(nil, failure) {
		t.Fatal("wrapped command rejection should allow legacy fallback")
	}
	response := &mongo.ClientBulkWriteResult{Acknowledged: true, HasVerboseResults: true,
		UpdateResults: map[int]mongo.ClientBulkWriteUpdateResult{0: {MatchedCount: 0}}}
	if unsupportedClientBulk(response, failure) {
		t.Fatal("a completed unmatched CAS cannot be replayed")
	}
	failure.PartialResult = response
	if unsupportedClientBulk(nil, failure) {
		t.Fatal("partial execution cannot be replayed")
	}
	uncertain := errors.New("connection reset")
	if unsupportedClientBulk(nil, uncertain) {
		t.Fatal("unknown execution cannot be replayed")
	}
	concern := mongo.WriteConcernError{Code: 64}
	failure.PartialResult = nil
	failure.WriteConcernErrors = []mongo.WriteConcernError{concern}
	if unsupportedClientBulk(nil, failure) {
		t.Fatal("write concern uncertainty cannot be replayed")
	}
}
