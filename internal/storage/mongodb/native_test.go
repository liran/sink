package mongodb

import (
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeCommandAllowsWritesAndRejectsCursorSessionState(t *testing.T) {
	tests := []struct {
		name    string
		command string
		scan    bool
		allowed bool
	}{
		{name: "count", command: `{"count":"products","query":{"active":true}}`, allowed: true},
		{name: "index", command: `{"createIndexes":"products","indexes":[{"key":{"signature":1},"name":"signature","unique":true}]}`, allowed: true},
		{name: "find", command: `{"find":"products","filter":{}}`, scan: true, allowed: true},
		{name: "aggregate", command: `{"aggregate":"products","pipeline":[{"$match":{"active":true}}],"cursor":{}}`, scan: true, allowed: true},
		{name: "update", command: `{"update":"products","updates":[]}`, allowed: true},
		{name: "cursor requires scan", command: `{"find":"products"}`},
		{name: "transaction", command: `{"count":"products","autocommit":false}`},
		{name: "session", command: `{"count":"products","lsid":{}}`},
		{name: "start session", command: `{"startSession":1}`},
		{name: "commit transaction", command: `{"commitTransaction":1}`},
		{name: "database override", command: `{"count":"products","$db":"another"}`},
		{name: "duplicate command", command: `{"count":"products","count":"another"}`},
		{name: "insert", command: `{"insert":"products","documents":[]}`, allowed: true},
		{name: "delete", command: `{"delete":"products","deletes":[]}`, allowed: true},
		{name: "findAndModify", command: `{"findAndModify":"products","query":{},"update":{"$inc":{"count":1}},"new":true}`, allowed: true},
		{name: "drop", command: `{"drop":"products"}`, allowed: true},
		{name: "unknown command forwarded", command: `{"futureCommand":1}`, allowed: true},
		{name: "getMore", command: `{"getMore":{"$numberLong":"123"},"collection":"products"}`},
		{name: "killCursors", command: `{"killCursors":"products","cursors":[]}`},
		{name: "list indexes requires scan", command: `{"listIndexes":"products"}`},
		{name: "list collections requires scan", command: `{"listCollections":1}`},
		{name: "aggregate requires scan", command: `{"aggregate":"products","pipeline":[],"cursor":{}}`},
		{name: "bulkWrite cursor", command: `{"bulkWrite":1,"ops":[],"nsInfo":[]}`},
		{name: "parallel cursor", command: `{"parallelCollectionScan":"products","numCursors":1}`},
		{name: "out", command: `{"aggregate":"products","pipeline":[{"$out":"another"}]}`, scan: true},
		{name: "nested merge", command: `{"aggregate":"products","pipeline":[{"$facet":{"x":[{"$merge":"another"}]}}]}`, scan: true},
		{name: "explain update", command: `{"explain":{"update":"products","updates":[]}}`, allowed: true},
		{name: "explain find", command: `{"explain":{"find":"products","filter":{}}}`, allowed: true},
		{name: "tail", command: `{"find":"products","tailable":true}`, scan: true},
		{name: "single batch", command: `{"find":"products","singleBatch":true}`, scan: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var command bson.D
			if err := bson.UnmarshalExtJSON([]byte(test.command), false, &command); err != nil {
				t.Fatal(err)
			}
			payload, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			mongoCommand := &storage.MongoCommand{Database: "catalog", Command: payload}
			req := storage.NativeRequest{Store: "primary", MongoDB: mongoCommand}
			_, err = validateNativeCommand(req, test.scan)
			if (err == nil) != test.allowed {
				t.Fatalf("allowed=%v, error=%v", test.allowed, err)
			}
		})
	}
}

func TestScanCommandBoundsBatchAndPreservesFilter(t *testing.T) {
	command := bson.D{{Key: "find", Value: "products"}, {Key: "batchSize", Value: int32(90000)},
		{Key: "filter", Value: bson.D{{Key: "enabled", Value: true}}}}
	bounded := scanCommand(command, 25)
	payload, err := bson.Marshal(bounded)
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(payload)
	if raw.Lookup("batchSize").Int32() != 25 || !raw.Lookup("filter", "enabled").Boolean() {
		t.Fatalf("unexpected command %s", raw)
	}
}
