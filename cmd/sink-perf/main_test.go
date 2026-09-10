package main

import (
	"maps"
	"net"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
)

func TestLoadGeneratorReconcilesAcknowledgedWrites(t *testing.T) {
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	// Reconciliation starts above this limit and must retry smaller read
	// batches without changing acknowledgement accounting.
	serverOptions := service.Options{Storage: memory.New(), Lua: engine, StoreNames: []string{"mongo"}, MaxReadBytes: 2048}
	core, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := service.BatchingOptions{StoreNames: []string{"mongo"}, MaxWait: time.Millisecond}
	batched, err := service.NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batched.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	codec := protocol.NewVTProtoCodec()
	server := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	sink.RegisterSinkServer(server, batched)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	for _, workload := range []string{"merge", "upsert", "mixed", "heavy-merge"} {
		t.Run(workload, func(t *testing.T) {
			opts := settings{Address: listener.Addr().String(), Dataset: "perf-" + workload, Store: "mongo", Workload: workload,
				Concurrency: 4, Keys: 16, Batch: 2, Padding: 128, Duration: 100 * time.Millisecond, Timeout: time.Second,
				Connections: 1, Shards: 1, ReturnDocument: true, RandomPadding: true, FullIncoming: true, Fields: 8}
			result, err := execute(t.Context(), opts)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Verified || result.Operations == 0 || len(result.Errors) != 0 {
				t.Fatalf("load did not complete correctly: %+v", result)
			}
		})
	}
	t.Run("hot-merge", func(t *testing.T) {
		opts := settings{Address: listener.Addr().String(), Dataset: "perf-hot-merge", Store: "mongo", Workload: "merge",
			Concurrency: 4, HotKeys: 2, Keys: 2, Batch: 1, Padding: 128, Fields: 8, Duration: 100 * time.Millisecond,
			Timeout: time.Second, Connections: 1, Shards: 1, ReturnDocument: true}
		result, err := execute(t.Context(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Healthy {
			t.Fatalf("hot-key reconciliation failed: %+v", result)
		}
	})
}

func TestValidateSettingsProtectsDisposableDataset(t *testing.T) {
	for _, name := range []string{"production", "perf-", "perf-a/b", "perf-a\"b", "perf-a\nb", "perf-a?x=1"} {
		opts := settings{Dataset: name, Store: "mongo", Workload: "merge", Concurrency: 4, Keys: 64, Batch: 1, Duration: time.Second, Timeout: time.Second, Connections: 1, Shards: 1}
		if err := validateSettings(&opts); err == nil {
			t.Fatalf("accepted unsafe dataset %q", name)
		}
	}
}

func TestValidateSettingsPartitionsEveryKey(t *testing.T) {
	opts := settings{Dataset: "perf-test", Store: "mongo", Workload: "merge", Concurrency: 7, Keys: 9, Batch: 3, Duration: time.Second, Timeout: time.Second, Connections: 1, Shards: 1}
	if err := validateSettings(&opts); err != nil {
		t.Fatal(err)
	}
	if opts.Keys != 21 {
		t.Fatalf("keys=%d, want one full batch per worker", opts.Keys)
	}
	opts.HotKeys, opts.Workload = 4, "upsert"
	if err := validateSettings(&opts); err == nil {
		t.Fatal("accepted an unverifiable shared-key overwrite workload")
	}
}

func TestDocumentCountersRoundTripBothEncodings(t *testing.T) {
	for _, store := range []string{"mongo", "search"} {
		opts := settings{Store: store}
		value := document{Value: 1024, Padding: "payload", Seen: map[string]int64{"w0": 1024}, Fields: extraFields(8)}
		encoded, err := encodeDocument(opts, value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeDocument(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Value != value.Value || decoded.Seen["w0"] != value.Seen["w0"] || decoded.Padding != value.Padding || !maps.Equal(decoded.Fields, value.Fields) {
			t.Fatalf("%s changed the document: %+v", store, decoded)
		}
	}
}

func TestRejectedWritesCannotBeReconciledAsUnknownCommits(t *testing.T) {
	for _, code := range []sink.FailureCode{sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT,
		sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED, sink.FailureCode_FAILURE_CODE_NOT_FOUND,
		sink.FailureCode_FAILURE_CODE_CONFLICT} {
		failure := &sink.Failure{Code: code}
		result := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_FAILED, Failure: failure}
		if mayHaveCommitted(result) {
			t.Fatalf("definite rejection %v was treated as an unknown commit", code)
		}
	}
	failure := &sink.Failure{Code: sink.FailureCode_FAILURE_CODE_UNAVAILABLE}
	result := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_FAILED, Failure: failure}
	if !mayHaveCommitted(result) {
		t.Fatal("lost response must allow reconciliation of an unknown commit")
	}
}
