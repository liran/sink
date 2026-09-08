package storage_test

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/liran/sink/internal/storage"
)

func TestOperationIDsRejectExpiredMalformedAndFutureReplays(t *testing.T) {
	now := time.Now()
	identity := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	valid := fmt.Sprintf("v1:%d:%s", now.UnixMilli(), identity)
	if _, err := storage.OperationCreated(valid, now); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "abc", "v1:0:invalid", fmt.Sprintf("v1:%d:%s", now.Add(-storage.OperationRetention).UnixMilli(), identity), fmt.Sprintf("v1:%d:%s", now.Add(time.Hour).UnixMilli(), identity), fmt.Sprintf("v1:+%d:%s", now.UnixMilli(), identity)} {
		if _, err := storage.OperationCreated(id, now); err == nil {
			t.Errorf("accepted %q", id)
		}
	}
}

func FuzzOperationCreated(f *testing.F) {
	f.Add("v1:0:AAAAAAAAAAAAAAAAAAAAAA")
	f.Add("v1:9223372036854775807:AAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, id string) { _, _ = storage.OperationCreated(id, time.Unix(1_800_000_000, 0)) })
}
