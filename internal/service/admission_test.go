package service

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockedReadStorage struct {
	storage.Storage
	started chan struct{}
}

func (b *blockedReadStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	b.started <- struct{}{}
	<-ctx.Done()
	var response storage.ReadResponse
	return response, ctx.Err()
}

func TestCoreAdmissionCoversCrossStoreAndBypassRequests(t *testing.T) {
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	backend := &blockedReadStorage{Storage: memory.New(), started: make(chan struct{}, 4)}
	opts := Options{Storage: backend, Lua: lua, StoreNames: []string{"a", "b"},
		MaxStoreRequests: 1, MaxInFlightRequests: 2, MaxReadBytes: 1024, RequestTimeout: 100 * time.Millisecond}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	a := admissionRead("a")
	b := admissionRead("b")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { _, _ = s.Read(ctx, a); close(done) }()
	<-backend.started
	cross := &sink.ReadRequest{Operations: []*sink.ReadOperation{a.Operations[0], b.Operations[0]}}
	if _, err := s.Read(t.Context(), cross); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("cross-store call bypassed occupied store limit: %v", err)
	}
	cancel()
	<-done
	// A caller without a deadline is still released by the server's deadline.
	_, err = s.Read(t.Context(), b)
	if err == nil {
		t.Fatal("blocked read unexpectedly succeeded")
	}
	if s.inFlightRequests != 0 || s.inFlightBytes != 0 || s.storeRequests["a"] != 0 || s.storeRequests["b"] != 0 {
		t.Fatal("cancelled/timed-out calls leaked admission capacity")
	}
}

func admissionRead(store string) *sink.ReadRequest {
	kind := &sink.RecordKey_StringValue{StringValue: "key"}
	key := &sink.RecordKey{Kind: kind}
	address := &sink.RecordAddress{Store: store, Namespace: "n", Dataset: "d", Key: key}
	op := &sink.ReadOperation{Address: address}
	req := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
	return req
}

func TestExecutionContextSurvivesOneCallerButCancelsForAll(t *testing.T) {
	first, cancelFirst := context.WithCancel(t.Context())
	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelFirst()
	defer cancelSecond()
	b := &requestBatcher[int, int]{ctx: t.Context(), executionTimeout: time.Second}
	calls := []*batchCall[int, int]{{ctx: first}, {ctx: second}}
	ctx, cleanup := b.executionContext(calls)
	defer cleanup()
	cancelFirst()
	select {
	case <-ctx.Done():
		t.Fatal("one caller cancelled the other caller's work")
	case <-time.After(20 * time.Millisecond):
	}
	cancelSecond()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("all cancelled callers retained execution")
	}
}
