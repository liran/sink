package protocol_test

import (
	"crypto/sha256"
	"slices"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSinkServiceContract(t *testing.T) {
	service := sink.File_sink_sink_proto.Services().ByName(protoreflect.Name("Sink"))
	if service == nil {
		t.Fatal("Sink service descriptor is missing")
	}

	methods := service.Methods()
	methodNames := make([]string, 0, methods.Len())
	for index := range methods.Len() {
		methodNames = append(methodNames, string(methods.Get(index).Name()))
	}

	want := []string{"Read", "Write", "Delete"}
	if !slices.Equal(methodNames, want) {
		t.Fatalf("Sink methods = %v, want %v", methodNames, want)
	}
}

func TestWriteRequestVTRoundTripPreservesActions(t *testing.T) {
	key := &sink.RecordKey{
		Kind: &sink.RecordKey_StringValue{StringValue: "record-1"},
	}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   "products",
		Key:       key,
	}
	document := &sink.Document{
		Json:          []byte(`{"created_at":"2026-08-29T04:34:56Z","value":1}`),
		DateTimePaths: []string{"/created_at"},
	}
	put := &sink.PutOperation{
		Document: document,
		Mode:     sink.WriteMode_WRITE_MODE_UPSERT,
	}
	putOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Put{Put: put},
	}

	source := []byte("return function(current, incoming, context) return incoming end")
	digest := sha256.Sum256(source)
	programReference := &sink.LuaProgram{
		Sha256: digest[:],
	}
	fullProgram := &sink.LuaProgram{Source: source, Sha256: digest[:]}
	merge := &sink.MergeOperation{
		IncomingDocument:    document,
		LuaProgram:          programReference,
		MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL,
	}
	mergeOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Merge{Merge: merge},
	}

	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.WriteOperation{putOperation, mergeOperation},
		LuaPrograms:    []*sink.LuaProgram{fullProgram},
	}

	encoded, err := request.MarshalVT()
	if err != nil {
		t.Fatalf("MarshalVT() error = %v", err)
	}

	decoded := &sink.WriteRequest{}
	err = decoded.UnmarshalVT(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVT() error = %v", err)
	}

	if !proto.Equal(decoded, request) {
		t.Fatalf("round trip request = %v, want %v", decoded, request)
	}
	if decoded.GetOperations()[0].GetPut() == nil {
		t.Fatal("first operation did not preserve put action")
	}
	if decoded.GetOperations()[1].GetMerge() == nil {
		t.Fatal("second operation did not preserve merge action")
	}
}
