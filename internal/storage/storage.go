// Package storage defines the database-independent record storage contract.
package storage

import (
	"context"
	"errors"
	"fmt"
)

// Storage applies batch-native operations. Atomicity is guaranteed per record,
// not for an entire request.
type Storage interface {
	Ping(ctx context.Context) error
	Read(ctx context.Context, req ReadRequest) (ReadResponse, error)
	Write(ctx context.Context, req WriteRequest) (WriteResponse, error)
	Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error)
}

type ErrorCode uint8

const (
	ErrorCodeInternal ErrorCode = iota
	ErrorCodeInvalidArgument
	ErrorCodeResourceExhausted
	ErrorCodeUnavailable
	ErrorCodeDeadlineExceeded
)

// OperationError carries storage-independent failure classification through the
// router and service layers.
type OperationError struct {
	code      ErrorCode
	retryable bool
	cause     error
}

func NewOperationError(code ErrorCode, retryable bool, cause error) *OperationError {
	operationError := &OperationError{code: code, retryable: retryable, cause: cause}
	return operationError
}

func InvalidArgumentError(cause error) *OperationError {
	return NewOperationError(ErrorCodeInvalidArgument, false, cause)
}

func ResourceExhaustedError(cause error) *OperationError {
	return NewOperationError(ErrorCodeResourceExhausted, true, cause)
}

func BackendError(cause error) *OperationError {
	var operationError *OperationError
	if errors.As(cause, &operationError) {
		return operationError
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return NewOperationError(ErrorCodeDeadlineExceeded, true, cause)
	}
	if errors.Is(cause, context.Canceled) {
		return NewOperationError(ErrorCodeInternal, false, cause)
	}
	return NewOperationError(ErrorCodeUnavailable, true, cause)
}

func ErrorDetails(err error) (ErrorCode, bool) {
	var operationError *OperationError
	if !errors.As(err, &operationError) {
		return ErrorCodeInternal, false
	}
	return operationError.code, operationError.retryable
}

func (e *OperationError) Error() string {
	if e == nil || e.cause == nil {
		return "storage operation failed"
	}
	return e.cause.Error()
}

func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e ErrorCode) String() string {
	switch e {
	case ErrorCodeInvalidArgument:
		return "invalid_argument"
	case ErrorCodeResourceExhausted:
		return "resource_exhausted"
	case ErrorCodeUnavailable:
		return "unavailable"
	case ErrorCodeDeadlineExceeded:
		return "deadline_exceeded"
	default:
		return fmt.Sprintf("internal(%d)", e)
	}
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

type DocumentEncoding uint8

const (
	DocumentEncodingUnspecified DocumentEncoding = iota
	DocumentEncodingJSON
	DocumentEncodingBSON
)

type Document struct {
	Encoding DocumentEncoding
	Payload  []byte
}

type Revision struct {
	Data []byte
}

type ReadRequest struct {
	Operations []ReadOperation
	Budget     *ReadBudget
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
	Operations       []WriteOperation
	WaitUntilVisible bool
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
	Operations       []DeleteOperation
	WaitUntilVisible bool
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
