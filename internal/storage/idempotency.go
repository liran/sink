package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

const OperationRetention = 31 * 24 * time.Hour

var ErrIdempotencyUnsupported = errors.New("idempotent writes require a supported transactional backend")

// IdempotentStorage commits a receipt and the callback's database effects in
// one transaction. The callback must use the supplied context and must not
// publish completion or perform external effects. It may be called again.
type IdempotentStorage interface {
	CheckIdempotency(context.Context, Address) error
	ExecuteOnce(context.Context, IdempotentRequest) ([]byte, error)
}

type IdempotentRequest struct {
	Address         Address
	OperationID     string
	Fingerprint     []byte
	MaxReceiptBytes int
	Apply           func(context.Context) ([]byte, error)
}

// OperationCreated enforces a bounded replay horizon without depending on TTL
// deletion timing. Replaying an expired ID never creates a fresh operation.
func OperationCreated(id string, now time.Time) (time.Time, error) {
	var empty time.Time
	parts := strings.Split(id, ":")
	if len(parts) != 3 || parts[0] != "v1" || len(id) > 160 {
		return empty, InvalidArgumentError(errors.New("invalid operation_id format"))
	}
	millis, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || strconv.FormatInt(millis, 10) != parts[1] {
		return empty, InvalidArgumentError(errors.New("invalid operation_id timestamp"))
	}
	identity, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(identity) < 16 || len(identity) > 64 || base64.RawURLEncoding.EncodeToString(identity) != parts[2] {
		return empty, InvalidArgumentError(errors.New("invalid operation_id identity"))
	}
	created := time.UnixMilli(millis)
	if created.After(now.Add(5*time.Minute)) || !now.Before(created.Add(OperationRetention)) {
		return empty, InvalidArgumentError(errors.New("operation_id is expired or has a future timestamp; do not regenerate it to retry"))
	}
	return created, nil
}

func (r *Router) CheckIdempotency(ctx context.Context, address Address) error {
	backend, ok := r.backends[address.Store].(IdempotentStorage)
	if !ok {
		return ErrIdempotencyUnsupported
	}
	return backend.CheckIdempotency(ctx, address)
}

func (r *Router) ExecuteOnce(ctx context.Context, req IdempotentRequest) ([]byte, error) {
	backend, ok := r.backends[req.Address.Store].(IdempotentStorage)
	if !ok {
		return nil, ErrIdempotencyUnsupported
	}
	return backend.ExecuteOnce(ctx, req)
}
