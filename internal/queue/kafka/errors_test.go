package kafka

import (
	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestPublisherClassifiesPayloadCapacityAndOperatorFailures(t *testing.T) {
	cases := []struct {
		err   error
		code  storage.ErrorCode
		retry bool
	}{
		{err: kerr.MessageTooLarge, code: storage.ErrorCodeInvalidArgument},
		{err: kerr.TopicAuthorizationFailed, code: storage.ErrorCodeUnavailable},
		{err: kerr.InvalidProducerEpoch, code: storage.ErrorCodeUnavailable},
		{err: kerr.NotEnoughReplicas, code: storage.ErrorCodeUnavailable, retry: true},
		{err: kgo.ErrMaxBuffered, code: storage.ErrorCodeResourceExhausted, retry: true},
	}
	for _, test := range cases {
		code, retry := storage.ErrorDetails(publishError(test.err))
		if code != test.code || retry != test.retry {
			t.Fatalf("%v => %v, retry=%v", test.err, code, retry)
		}
	}
}
