package queue_test

import (
	"bytes"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"google.golang.org/protobuf/proto"
)

func TestMutationCodecRoundTrip(t *testing.T) {
	address := testQueueAddress("record-1")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(`{"created_at":"2026-08-29T04:34:56Z","value":1}`),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	mutation := queue.Mutation{Write: operation}

	encoded, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatalf("MarshalMutation() error = %v", err)
	}
	decoded, err := queue.UnmarshalMutation(encoded)
	if err != nil {
		t.Fatalf("UnmarshalMutation() error = %v", err)
	}
	if decoded.Delete != nil || !proto.Equal(decoded.Write, operation) {
		t.Fatalf("UnmarshalMutation() = %#v", decoded)
	}
}

func TestMutationKeyIsStablePerAddress(t *testing.T) {
	address := testQueueAddress("record-1")
	write := &sink.WriteOperation{Address: address}
	deleteOperation := &sink.DeleteOperation{Address: address}
	writeMutation := queue.Mutation{Write: write}
	deleteMutation := queue.Mutation{Delete: deleteOperation}
	writeKey, err := queue.MutationKey(writeMutation)
	if err != nil {
		t.Fatalf("MutationKey(write) error = %v", err)
	}
	deleteKey, err := queue.MutationKey(deleteMutation)
	if err != nil {
		t.Fatalf("MutationKey(delete) error = %v", err)
	}
	if !bytes.Equal(writeKey, deleteKey) {
		t.Fatalf("write key %x differs from delete key %x", writeKey, deleteKey)
	}
	marshalOptions := proto.MarshalOptions{Deterministic: true}
	standardKey, err := marshalOptions.Marshal(address)
	if err != nil {
		t.Fatalf("proto.Marshal(address) error = %v", err)
	}
	if !bytes.Equal(writeKey, standardKey) {
		t.Fatalf("VT key %x differs from deterministic protobuf key %x", writeKey, standardKey)
	}
}

func TestMutationKeyIgnoresUnknownAddressFields(t *testing.T) {
	plain := testQueueAddress("record-1")
	withUnknown := proto.Clone(plain).(*sink.RecordAddress)
	withUnknown.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	withUnknown.GetKey().ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x02})
	plainKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: plain}})
	if err != nil {
		t.Fatal(err)
	}
	unknownKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: withUnknown}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plainKey, unknownKey) {
		t.Fatalf("semantic address key changed: %x != %x", plainKey, unknownKey)
	}
}

func TestMutationKeyWithoutNamespaceMatchesSearchIdentity(t *testing.T) {
	first := testQueueAddress("record-1")
	second := proto.Clone(first).(*sink.RecordAddress)
	second.Namespace = "another-logical-namespace"
	firstKey, err := queue.MutationKeyWithoutNamespace(queue.Mutation{Write: &sink.WriteOperation{Address: first}})
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := queue.MutationKeyWithoutNamespace(queue.Mutation{Write: &sink.WriteOperation{Address: second}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstKey, secondKey) {
		t.Fatalf("search identity keys differ: %x != %x", firstKey, secondKey)
	}
}

func BenchmarkMutationPayloadMarshal(b *testing.B) {
	address := testQueueAddress("benchmark")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  bytes.Repeat([]byte("x"), 4096),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}

	b.Run("vtproto", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoded, err := operation.MarshalVT()
			if err != nil {
				b.Fatal(err)
			}
			_ = encoded
		}
	})
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoded, err := proto.Marshal(operation)
			if err != nil {
				b.Fatal(err)
			}
			_ = encoded
		}
	})
}

func BenchmarkMutationPayloadUnmarshal(b *testing.B) {
	address := testQueueAddress("benchmark")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  bytes.Repeat([]byte("x"), 4096),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	encoded, err := operation.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("vtproto", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := &sink.WriteOperation{}
			if err := decoded.UnmarshalVT(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := &sink.WriteOperation{}
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func testQueueAddress(key string) *sink.RecordAddress {
	recordKey := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: key}}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       recordKey,
	}
	return address
}
