package kafka

import (
	"context"
	"errors"

	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func publishError(err error) error {
	if errors.Is(err, kgo.ErrMaxBuffered) {
		return storage.ResourceExhaustedError(err)
	}
	var kafkaError *kerr.Error
	if errors.As(err, &kafkaError) && !kafkaError.Retriable {
		switch kafkaError.Code {
		case kerr.MessageTooLarge.Code, kerr.RecordListTooLarge.Code, kerr.InvalidRecord.Code,
			kerr.InvalidTimestamp.Code, kerr.CorruptMessage.Code:
			return storage.InvalidArgumentError(err)
		default:
			// Authorization, fencing, and protocol failures need operator action;
			// they are not evidence that the business document was malformed.
			return storage.NewOperationError(storage.ErrorCodeUnavailable, false, err)
		}
	}
	return storage.BackendError(err)
}

func retryableProcessingError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isRetryable(err)
}
