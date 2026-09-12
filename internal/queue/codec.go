package queue

import (
	"bytes"
	"errors"
	"fmt"

	sink "github.com/liran/sink/gen/sink"
)

var envelopeMagic = [4]byte{'S', 'N', 'K', 'Q'}

const (
	envelopeVersion byte = 1
	mutationWrite   byte = 1
	mutationDelete  byte = 2
)

type marshalVTMessage interface {
	MarshalVT() ([]byte, error)
	SizeVT() int
}

type MutationKeyFunc func(Mutation) ([]byte, error)

// MutationSize includes the queue envelope without allocating its payload.
func MutationSize(mutation Mutation) int {
	_, message, err := mutationMessage(mutation)
	if err != nil {
		return 0
	}
	return len(envelopeMagic) + 2 + message.SizeVT()
}

func MarshalMutation(mutation Mutation) ([]byte, error) {
	kind, message, err := mutationMessage(mutation)
	if err != nil {
		return nil, err
	}
	payload, err := message.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal queue mutation: %w", err)
	}
	envelope := make([]byte, 0, len(envelopeMagic)+2+len(payload))
	envelope = append(envelope, envelopeMagic[:]...)
	envelope = append(envelope, envelopeVersion, kind)
	envelope = append(envelope, payload...)
	return envelope, nil
}

func UnmarshalMutation(envelope []byte) (Mutation, error) {
	var empty Mutation
	if len(envelope) < len(envelopeMagic)+2 {
		return empty, errors.New("queue mutation envelope is truncated")
	}
	if !bytes.Equal(envelope[:len(envelopeMagic)], envelopeMagic[:]) {
		return empty, errors.New("queue mutation envelope has invalid magic")
	}
	if envelope[len(envelopeMagic)] != envelopeVersion {
		return empty, fmt.Errorf("unsupported queue mutation envelope version %d", envelope[len(envelopeMagic)])
	}
	kind := envelope[len(envelopeMagic)+1]
	payload := envelope[len(envelopeMagic)+2:]
	switch kind {
	case mutationWrite:
		operation := &sink.WriteOperation{}
		if err := operation.UnmarshalVT(payload); err != nil {
			return empty, fmt.Errorf("unmarshal queued write: %w", err)
		}
		mutation := Mutation{Write: operation}
		return mutation, nil
	case mutationDelete:
		operation := &sink.DeleteOperation{}
		if err := operation.UnmarshalVT(payload); err != nil {
			return empty, fmt.Errorf("unmarshal queued delete: %w", err)
		}
		mutation := Mutation{Delete: operation}
		return mutation, nil
	default:
		return empty, fmt.Errorf("unsupported queue mutation kind %d", kind)
	}
}

func MutationKey(mutation Mutation) ([]byte, error) {
	return mutationKey(mutation, true)
}

// MutationKeyWithoutNamespace is used by search backends, where namespace is
// logical metadata and dataset already contains the complete index or alias.
func MutationKeyWithoutNamespace(mutation Mutation) ([]byte, error) {
	return mutationKey(mutation, false)
}

func mutationKey(mutation Mutation, includeNamespace bool) ([]byte, error) {
	address, err := mutationAddress(mutation)
	if err != nil {
		return nil, err
	}
	canonical := &sink.RecordAddress{
		Store:   address.GetStore(),
		Dataset: address.GetDataset(),
		Key:     canonicalRecordKey(address.GetKey()),
	}
	if includeNamespace {
		canonical.Namespace = address.GetNamespace()
	}
	encoded, err := canonical.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal queue mutation address: %w", err)
	}
	return encoded, nil
}

func canonicalRecordKey(key *sink.RecordKey) *sink.RecordKey {
	canonical := &sink.RecordKey{}
	switch kind := key.GetKind().(type) {
	case *sink.RecordKey_StringValue:
		canonical.Kind = &sink.RecordKey_StringValue{StringValue: kind.StringValue}
	case *sink.RecordKey_Int64Value:
		canonical.Kind = &sink.RecordKey_Int64Value{Int64Value: kind.Int64Value}
	case *sink.RecordKey_BytesValue:
		canonical.Kind = &sink.RecordKey_BytesValue{BytesValue: bytes.Clone(kind.BytesValue)}
	case *sink.RecordKey_OpaqueValue:
		opaque := &sink.OpaqueValue{}
		if kind.OpaqueValue != nil {
			opaque.Type = kind.OpaqueValue.GetType()
			opaque.Data = bytes.Clone(kind.OpaqueValue.GetData())
		}
		canonical.Kind = &sink.RecordKey_OpaqueValue{OpaqueValue: opaque}
	}
	return canonical
}

func MutationStore(mutation Mutation) (string, error) {
	address, err := mutationAddress(mutation)
	if err != nil {
		return "", err
	}
	return address.GetStore(), nil
}

func mutationAddress(mutation Mutation) (*sink.RecordAddress, error) {
	_, message, err := mutationMessage(mutation)
	if err != nil {
		return nil, err
	}
	var address *sink.RecordAddress
	switch operation := message.(type) {
	case *sink.WriteOperation:
		address = operation.GetAddress()
	case *sink.DeleteOperation:
		address = operation.GetAddress()
	}
	if address == nil || address.GetStore() == "" || address.GetNamespace() == "" || address.GetDataset() == "" ||
		address.GetKey() == nil || address.GetKey().GetKind() == nil {
		return nil, errors.New("queue mutation record address is required")
	}
	return address, nil
}

func mutationMessage(mutation Mutation) (byte, marshalVTMessage, error) {
	if mutation.Write != nil && mutation.Delete != nil {
		return 0, nil, errors.New("queue mutation contains both write and delete operations")
	}
	if mutation.Write != nil {
		return mutationWrite, mutation.Write, nil
	}
	if mutation.Delete != nil {
		return mutationDelete, mutation.Delete, nil
	}
	return 0, nil, errors.New("queue mutation has no operation")
}
