package mongodb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nativeWriteTestCommand(t *testing.T, input string) bson.D {
	t.Helper()
	var command bson.D
	if err := bson.UnmarshalExtJSON([]byte(input), false, &command); err != nil {
		t.Fatal(err)
	}
	return command
}

func nativeWriteTestRaw(t *testing.T, value any) bson.Raw {
	t.Helper()
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(payload)
}

func TestNativeWritesInjectFreshRevision(t *testing.T) {
	tests := []struct {
		name    string
		command string
		path    []string
	}{
		{"insert", `{"insert":"items","documents":[{"_id":"a","value":{"$numberLong":"9007199254740993"}}]}`, []string{"documents", "0"}},
		{"operator", `{"update":"items","updates":[{"q":{"_id":"a"},"u":{"$inc":{"value":1}}}]}`, []string{"updates", "0", "u", "$set"}},
		{"existing set", `{"update":"items","updates":[{"q":{},"u":{"$set":{"value":1},"$inc":{"n":1}}}]}`, []string{"updates", "0", "u", "$set"}},
		{"replacement", `{"update":"items","updates":[{"q":{"_id":"a"},"u":{"value":3},"upsert":true}]}`, []string{"updates", "0", "u"}},
		{"empty replacement", `{"update":"items","updates":[{"q":{"_id":"a"},"u":{}}]}`, []string{"updates", "0", "u"}},
		{"pipeline", `{"update":"items","updates":[{"q":{},"u":[{"$set":{"n":{"$add":["$n",1]}}}],"multi":true}]}`, []string{"updates", "0", "u", "1", "$set"}},
		{"empty pipeline", `{"update":"items","updates":[{"q":{},"u":[]}]}`, []string{"updates", "0", "u", "0", "$set"}},
		{"root pipeline", `{"update":"items","updates":[{"q":{},"u":[{"$replaceWith":{"value":2}}]}]}`, []string{"updates", "0", "u", "1", "$set"}},
		{"modify", `{"findAndModify":"items","query":{},"update":{"$inc":{"n":1}},"new":true}`, []string{"update", "$set"}},
		{"modify replacement", `{"findandmodify":"items","query":{},"update":{"n":2}}`, []string{"update"}},
		{"modify pipeline", `{"findAndModify":"items","query":{},"update":[{"$project":{"n":1}}]}`, []string{"update", "1", "$set"}},
	}
	for _, metadataField := range []string{"__sink", "__revision"} {
		for _, test := range tests {
			t.Run(metadataField+"/"+test.name, func(t *testing.T) {
				store := &Store{metadataField: metadataField}
				var previous []byte
				for range 2 {
					command := nativeWriteTestCommand(t, test.command)
					prepared, err := store.prepareNativeWrite(command)
					if err != nil {
						t.Fatal(err)
					}
					raw := nativeWriteTestRaw(t, prepared)
					path := append([]string(nil), test.path...)
					path = append(path, metadataField)
					if strings.Contains(test.name, "pipeline") {
						path = append(path, "$literal")
					}
					path = append(path, "revision")
					subtype, revision, ok := raw.Lookup(path...).BinaryOK()
					if !ok || subtype != 0 || len(revision) != 16 || bytes.Equal(previous, revision) {
						t.Fatalf("invalid or reused revision in %s", raw)
					}
					previous = bytes.Clone(revision)
					if test.name == "insert" && raw.Lookup("documents", "0", "value").Int64() != 9007199254740993 {
						t.Fatal("native numeric width or value changed")
					}
					if test.name == "existing set" && raw.Lookup("updates", "0", "u", "$set", "value").AsInt64() != 1 {
						t.Fatal("user update was overwritten")
					}
				}
			})
		}
	}
}

func TestNativeWriteRejectsUnsafeMembersBeforeBackendAccess(t *testing.T) {
	tests := []string{
		`{"insert":"system.buckets.items","documents":[{"_id":"a"}]}`,
		`{"delete":"system.views","deletes":[{"q":{},"limit":0}]}`,
		`{"update":1,"updates":[]}`,
		`{"insert":"","documents":[]}`,
		`{"insert":"items","documents":[{"_id":"first"},{"_id":"second","__sink":{}}]}`,
		`{"insert":"items","documents":[{"__sink.revision":1}]}`,
		`{"insert":"items","documents":[1]}`,
		`{"insert":"items","documents":{}}`,
		`{"update":"items","updates":[{"q":{},"u":{"$inc":{"n":1}}},{"q":{},"u":{"$set":{"__sink.revision":1}}}],"ordered":false}`,
		`{"update":"items","updates":[{"q":{},"u":{"$unset":{"__sink":1}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$rename":{"n":"__sink.revision"}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$rename":{"__sink":"n"}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$setOnInsert":{"__sink":{}}},"upsert":true}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"__sink":{},"n":2}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$futureOperator":{"n":1}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$set":{"n":1},"plain":1}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"plain":1,"$set":{"n":2}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$set":1}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$rename":{"n":1}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":1}]}`,
		`{"update":"items","updates":[{"q":{}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{},"u":{"__sink":{}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":{"$set":{"n":1},"$set":{"__sink":{}}}}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$set":{"__sink":{}}}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$unset":["n","__sink"]}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$project":{"__sink.revision":0}}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$merge":"other"}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$out":"other"}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$set":1}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$unset":[1]}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[{"$set":{},"$unset":"n"}]}]}`,
		`{"update":"items","updates":[{"q":{},"u":[1]}]}`,
		`{"update":"items"}`,
		`{"findAndModify":"items","update":{"$set":{"__sink":1}}}`,
		`{"findAndModify":"items","remove":true,"update":{"$set":{"n":1}}}`,
		`{"findAndModify":"items"}`,
	}
	store := &Store{store: "primary", metadataField: "__sink"}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			command := nativeWriteTestCommand(t, input)
			raw := nativeWriteTestRaw(t, command)
			before := bytes.Clone(raw)
			request := storage.NativeRequest{Store: "primary", Namespace: "catalog", ContentType: "application/bson", Payload: raw}
			// A nil client panics if an unsafe command gets as far as MongoDB.
			_, err := store.Execute(t.Context(), request)
			code, _ := storage.ErrorDetails(err)
			if code != storage.ErrorCodeInvalidArgument {
				t.Fatalf("unsafe write was not rejected: %v", err)
			}
			if !bytes.Equal(raw, before) {
				t.Fatal("caller payload mutated")
			}
		})
	}
}

func TestNativeWritePreservesDeletesAndReadCommands(t *testing.T) {
	store := &Store{metadataField: "__sink"}
	for _, input := range []string{
		`{"delete":"items","deletes":[{"q":{"_id":"a"},"limit":1}]}`,
		`{"findAndModify":"items","query":{"_id":"a"},"remove":true}`,
		`{"count":"items","query":{"active":true}}`,
		`{"createIndexes":"items","indexes":[{"key":{"n":1},"name":"n"}]}`,
	} {
		command := nativeWriteTestCommand(t, input)
		before := nativeWriteTestRaw(t, command)
		prepared, err := store.prepareNativeWrite(command)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, nativeWriteTestRaw(t, prepared)) {
			t.Fatalf("command changed: %s", input)
		}
	}
}

func TestNativeWriteRevisionIsUniqueWithinCommand(t *testing.T) {
	store := &Store{metadataField: "__sink"}
	command := nativeWriteTestCommand(t, `{"insert":"items","documents":[{"_id":"a"},{"_id":"b"}]}`)
	prepared, err := store.prepareNativeWrite(command)
	if err != nil {
		t.Fatal(err)
	}
	raw := nativeWriteTestRaw(t, prepared)
	_, first := raw.Lookup("documents", "0", "__sink", "revision").Binary()
	_, second := raw.Lookup("documents", "1", "__sink", "revision").Binary()
	if bytes.Equal(first, second) {
		t.Fatal("inserted records shared a revision")
	}
}
