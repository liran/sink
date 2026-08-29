package service

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
)

func convertAddress(address *sink.RecordAddress) (storage.Address, error) {
	var emptyAddress storage.Address
	if address == nil {
		return emptyAddress, errors.New("record address is required")
	}
	if address.GetStore() == "" {
		return emptyAddress, errors.New("record address store is required")
	}
	if address.GetNamespace() == "" {
		return emptyAddress, errors.New("record address namespace is required")
	}
	if address.GetDataset() == "" {
		return emptyAddress, errors.New("record address dataset is required")
	}

	key, err := convertKey(address.GetKey())
	if err != nil {
		return emptyAddress, err
	}
	converted := storage.Address{
		Store:     address.GetStore(),
		Namespace: address.GetNamespace(),
		Dataset:   address.GetDataset(),
		Key:       key,
	}
	return converted, nil
}

func convertKey(key *sink.RecordKey) (storage.Key, error) {
	var emptyKey storage.Key
	if key == nil {
		return emptyKey, errors.New("record key is required")
	}

	converted := storage.Key{}
	switch kind := key.GetKind().(type) {
	case *sink.RecordKey_StringValue:
		converted.Type = "string"
		converted.Data = []byte(kind.StringValue)
	case *sink.RecordKey_Int64Value:
		converted.Type = "int64"
		converted.Data = make([]byte, 8)
		binary.BigEndian.PutUint64(converted.Data, uint64(kind.Int64Value))
	case *sink.RecordKey_BytesValue:
		converted.Type = "bytes"
		converted.Data = bytes.Clone(kind.BytesValue)
	case *sink.RecordKey_OpaqueValue:
		if kind.OpaqueValue == nil || kind.OpaqueValue.GetType() == "" {
			return emptyKey, errors.New("opaque record key type is required")
		}
		converted.Type = "opaque:" + kind.OpaqueValue.GetType()
		converted.Data = bytes.Clone(kind.OpaqueValue.GetData())
	default:
		return emptyKey, errors.New("record key kind is required")
	}
	return converted, nil
}

func convertDocument(document *sink.Document) (storage.Document, error) {
	var emptyDocument storage.Document
	if document == nil {
		return emptyDocument, errors.New("document is required")
	}
	encoded := bytes.TrimSpace(document.GetJson())
	if len(encoded) < 2 || encoded[0] != '{' || encoded[len(encoded)-1] != '}' || !json.Valid(encoded) {
		return emptyDocument, errors.New("document must contain a valid JSON object")
	}
	converted := storage.Document{
		JSON:          bytes.Clone(encoded),
		DateTimePaths: append([]string(nil), document.GetDateTimePaths()...),
	}
	if _, err := storage.DecodeDateTimeValues(converted); err != nil {
		return emptyDocument, fmt.Errorf("document date-time metadata: %w", err)
	}
	return converted, nil
}

func applyReadResult(result *sink.ReadResult, stored storage.ReadResult) {
	switch stored.Status {
	case storage.ReadStatusFound:
		result.Status = sink.ReadStatus_READ_STATUS_FOUND
		document := &sink.Document{
			Json:          bytes.Clone(stored.Document.JSON),
			DateTimePaths: append([]string(nil), stored.Document.DateTimePaths...),
		}
		result.Document = document
		result.Revision = &sink.RevisionToken{Data: bytes.Clone(stored.Revision.Data)}
	case storage.ReadStatusNotFound:
		result.Status = sink.ReadStatus_READ_STATUS_NOT_FOUND
	case storage.ReadStatusFailed:
		code, retryable := storageFailureDetails(stored.Err)
		setReadFailure(result, code, stored.Err, retryable)
	default:
		err := fmt.Errorf("unsupported storage read status %d", stored.Status)
		setReadFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
	}
}

func applyWriteResult(result *sink.WriteResult, stored storage.WriteResult) {
	switch stored.Status {
	case storage.WriteStatusApplied:
		result.Status = sink.WriteStatus_WRITE_STATUS_APPLIED
		result.Revision = &sink.RevisionToken{Data: bytes.Clone(stored.Revision.Data)}
	case storage.WriteStatusPreconditionFailed:
		result.Status = sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
		failure := &sink.Failure{
			Code:      sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED,
			Message:   "write precondition did not match",
			Retryable: false,
		}
		result.Failure = failure
	case storage.WriteStatusFailed:
		code, retryable := storageFailureDetails(stored.Err)
		setWriteFailure(result, code, stored.Err, retryable)
	default:
		err := fmt.Errorf("unsupported storage write status %d", stored.Status)
		setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
	}
}

func applyDeleteResult(result *sink.DeleteResult, stored storage.DeleteResult) {
	switch stored.Status {
	case storage.DeleteStatusApplied:
		result.Status = sink.DeleteStatus_DELETE_STATUS_APPLIED
	case storage.DeleteStatusFailed:
		code, retryable := storageFailureDetails(stored.Err)
		setDeleteFailure(result, code, stored.Err, retryable)
	default:
		err := fmt.Errorf("unsupported storage delete status %d", stored.Status)
		setDeleteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
	}
}

func storageFailureDetails(err error) (sink.FailureCode, bool) {
	code, retryable := storage.ErrorDetails(err)
	switch code {
	case storage.ErrorCodeInvalidArgument:
		return sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, retryable
	case storage.ErrorCodeResourceExhausted:
		return sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED, retryable
	case storage.ErrorCodeUnavailable:
		return sink.FailureCode_FAILURE_CODE_UNAVAILABLE, retryable
	case storage.ErrorCodeDeadlineExceeded:
		return sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED, retryable
	default:
		return sink.FailureCode_FAILURE_CODE_INTERNAL, retryable
	}
}

func setReadFailure(result *sink.ReadResult, code sink.FailureCode, err error, retryable bool) {
	result.Status = sink.ReadStatus_READ_STATUS_FAILED
	result.Failure = newFailure(code, err, retryable)
}

func setWriteFailure(result *sink.WriteResult, code sink.FailureCode, err error, retryable bool) {
	result.Status = sink.WriteStatus_WRITE_STATUS_FAILED
	result.Failure = newFailure(code, err, retryable)
}

func setDeleteFailure(result *sink.DeleteResult, code sink.FailureCode, err error, retryable bool) {
	result.Status = sink.DeleteStatus_DELETE_STATUS_FAILED
	result.Failure = newFailure(code, err, retryable)
}

func newFailure(code sink.FailureCode, err error, retryable bool) *sink.Failure {
	message := "operation failed"
	if err != nil {
		message = err.Error()
	}
	failure := &sink.Failure{
		Code:      code,
		Message:   message,
		Retryable: retryable,
	}
	return failure
}
