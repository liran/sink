// Package storage defines the database-independent record storage contract.
package storage

import "context"

// Storage applies batch-native operations. Atomicity is guaranteed per record,
// not for an entire request.
type Storage interface {
	Read(ctx context.Context, req ReadRequest) (ReadResponse, error)
	Write(ctx context.Context, req WriteRequest) (WriteResponse, error)
	Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error)
}

type Key struct {
	Type string
	Data []byte
}

type Address struct {
	Store     string
	Namespace string
	Dataset   string
	Key       Key
}

// RoutingKey returns a deterministic binary-safe identity for an address.
func (a Address) RoutingKey() string {
	encoded := make([]byte, 0, 64+len(a.Key.Data))
	encoded = appendPart(encoded, []byte(a.Store))
	encoded = appendPart(encoded, []byte(a.Namespace))
	encoded = appendPart(encoded, []byte(a.Dataset))
	encoded = appendPart(encoded, []byte(a.Key.Type))
	encoded = appendPart(encoded, a.Key.Data)
	return string(encoded)
}

func appendPart(destination []byte, part []byte) []byte {
	partLength := uint64(len(part))
	destination = append(destination,
		byte(partLength>>56),
		byte(partLength>>48),
		byte(partLength>>40),
		byte(partLength>>32),
		byte(partLength>>24),
		byte(partLength>>16),
		byte(partLength>>8),
		byte(partLength),
	)
	destination = append(destination, part...)
	return destination
}

type Document struct {
	ContentType string
	Data        []byte
}

type Revision struct {
	Data []byte
}

type ReadRequest struct {
	Operations []ReadOperation
}

type ReadOperation struct {
	Address Address
}

type ReadResponse struct {
	Results []ReadResult
}

type ReadResult struct {
	Status   ReadStatus
	Document Document
	Revision Revision
	Err      error
}

type ReadStatus uint8

const (
	ReadStatusUnspecified ReadStatus = iota
	ReadStatusFound
	ReadStatusNotFound
	ReadStatusFailed
)

type WriteRequest struct {
	Operations []WriteOperation
}

type WriteOperation struct {
	Address      Address
	Document     Document
	Precondition Precondition
}

type Precondition struct {
	Kind     PreconditionKind
	Revision Revision
}

type PreconditionKind uint8

const (
	PreconditionNone PreconditionKind = iota
	PreconditionRecordExists
	PreconditionRecordNotExists
	PreconditionRevisionMatches
	PreconditionRevisionAbsent
)

type WriteResponse struct {
	Results []WriteResult
}

type WriteResult struct {
	Status   WriteStatus
	Revision Revision
	Err      error
}

type WriteStatus uint8

const (
	WriteStatusUnspecified WriteStatus = iota
	WriteStatusApplied
	WriteStatusPreconditionFailed
	WriteStatusFailed
)

type DeleteRequest struct {
	Operations []DeleteOperation
}

type DeleteOperation struct {
	Address Address
}

type DeleteResponse struct {
	Results []DeleteResult
}

type DeleteResult struct {
	Status DeleteStatus
	Err    error
}

type DeleteStatus uint8

const (
	DeleteStatusUnspecified DeleteStatus = iota
	DeleteStatusApplied
	DeleteStatusFailed
)
