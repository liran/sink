package queue

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
)

func TestIdempotentEnvelopeNeverDowngrades(t *testing.T) {
	operation := &sink.WriteOperation{OperationId: "stable-id"}
	mutation := Mutation{Write: operation}
	encoded, err := MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[4] != 2 {
		t.Fatal("protected write uses legacy envelope")
	}
	replayed, err := UnmarshalMutation(encoded)
	if err != nil || replayed.Write.GetOperationId() != operation.OperationId {
		t.Fatalf("ID lost: %v %v", replayed, err)
	}
	encoded[4] = 1
	if _, err := UnmarshalMutation(encoded); err == nil {
		t.Fatal("downgraded envelope accepted")
	}
	operation.OperationId = ""
	legacy, err := MarshalMutation(mutation)
	if err != nil || legacy[4] != 1 {
		t.Fatalf("legacy writes broken: %v", err)
	}
	legacy[4] = 2
	if _, err := UnmarshalMutation(legacy); err == nil {
		t.Fatal("protected envelope without ID accepted")
	}
}
