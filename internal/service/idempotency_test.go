package service

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIdempotencyRejectsUnsupportedBackendBeforeAnyWrite(t *testing.T) {
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	backend := memory.New()
	opts := Options{Storage: backend, Lua: lua}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := completionPut("ordinary", 1)
	protected := completionPut("protected", 1)
	protected.OperationId = fmt.Sprintf("v1:%d:%s", time.Now().UnixMilli(), base64.RawURLEncoding.EncodeToString(make([]byte, 16)))
	req := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{ordinary, protected}}
	if _, err := s.Write(t.Context(), req); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsafe fallback: %v", err)
	}
	read := &sink.ReadOperation{Address: ordinary.Address}
	readRequest := &sink.ReadRequest{Operations: []*sink.ReadOperation{read}}
	result, err := s.Read(t.Context(), readRequest)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatal("capability failure partially wrote batch")
	}
	if _, err := s.WriteIdempotent(t.Context(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing ID accepted: %v", err)
	}
}
