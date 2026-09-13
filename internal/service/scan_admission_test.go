package service

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/search"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestScansLeaveOrdinaryRequestCapacity(t *testing.T) {
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Storage: memory.New(), Lua: lua, StoreNames: []string{"mongo"}}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	scan := admissionRequest{encodedBytes: (56 << 20) + 128, stores: []string{"mongo"}, scan: true}
	for range 2 {
		_, release, admitErr := s.admitRequest(t.Context(), scan)
		if admitErr != nil {
			t.Fatal(admitErr)
		}
		t.Cleanup(release)
	}
	_, _, err = s.admitRequest(t.Context(), scan)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third Mongo scan must hit scan quota: %v", err)
	}
	ordinary := admissionRequest{encodedBytes: (64 << 20) + 128, stores: []string{"mongo"}}
	_, release, err := s.admitRequest(t.Context(), ordinary)
	if err != nil {
		t.Fatalf("scans starved ordinary Read/Merge: %v", err)
	}
	release()
	if s.inFlightBytes != 2*scan.encodedBytes || s.scanBytes != s.inFlightBytes {
		t.Fatal("ordinary release corrupted scan accounting")
	}
}

func TestScanRequestAndStoreQuotasRelease(t *testing.T) {
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Storage: memory.New(), Lua: lua, MaxInFlightRequests: 4, MaxStoreRequests: 2,
		MaxInFlightBytes: 4096, MaxScanRequests: 2, MaxScanBytes: 2048, MaxStoreScanRequests: 1, StoreNames: []string{"a", "b", "c"}}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	first := admissionRequest{encodedBytes: 100, scan: true, stores: []string{"a"}}
	_, release, err := s.admitRequest(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.admitRequest(t.Context(), first)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("store quota: %v", err)
	}
	second := admissionRequest{encodedBytes: 100, scan: true, stores: []string{"b"}}
	_, releaseSecond, err := s.admitRequest(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	third := admissionRequest{encodedBytes: 100, scan: true, stores: []string{"c"}}
	_, _, err = s.admitRequest(t.Context(), third)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("request quota: %v", err)
	}
	release()
	releaseSecond()
	if s.scanRequests != 0 || s.scanBytes != 0 || len(s.storeScanRequests) != 0 {
		t.Fatal("scan quota leaked")
	}
	_, release, err = s.admitRequest(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestSearchScanReservesLookaheadAndDecodeBuffers(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[]}}`))
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := search.Options{Driver: search.DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
	store, err := search.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	core := completionServer(t, store).server
	core.maxScanBytes = 12 << 20
	command := &sink.Command{Store: "primary", Method: "POST", Path: "/products/_search",
		ContentType: "application/json; charset=utf-8", Payload: []byte(`{"sort":["uid"]}`)}
	request := &sink.ScanRequest{Command: command}
	_, err = core.Scan(t.Context(), request)
	if status.Code(err) != codes.ResourceExhausted || calls.Load() != 0 {
		t.Fatalf("Scan exceeded its working-buffer quota: calls=%d, err=%v", calls.Load(), err)
	}
	core.maxScanBytes = 20 << 20
	if _, err := core.Scan(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || core.scanBytes != 0 || core.inFlightBytes != 0 {
		t.Fatal("Scan did not release its working-buffer reservation")
	}
}
