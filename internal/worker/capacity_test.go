package worker_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type capacityApplier struct {
	worker.Applier
	writeSizes  []int
	deleteSizes []int
	failure     error
}

func (a *capacityApplier) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	a.writeSizes = append(a.writeSizes, len(req.Operations))
	if a.failure != nil {
		return nil, a.failure
	}
	return a.Applier.Write(ctx, req)
}

func (a *capacityApplier) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	a.deleteSizes = append(a.deleteSizes, len(req.Operations))
	if a.failure != nil {
		return nil, a.failure
	}
	return a.Applier.Delete(ctx, req)
}

func TestProcessorSplitsRejectedWritesAndDeletesWithoutReplayingSuccess(t *testing.T) {
	store := memory.New()
	luaOpts := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: store, Lua: engine, MaxInFlightBytes: 1024}
	core, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	observed := &capacityApplier{Applier: core}
	processor, err := worker.NewProcessor(observed)
	if err != nil {
		t.Fatal(err)
	}
	var writes, deletes []queue.Mutation
	for _, id := range []string{"a", "b", "c", "d"} {
		address := processorAddressFor(id + strings.Repeat("x", 550))
		put := processorPut(address, "value")
		put.GetPut().Mode = sink.WriteMode_WRITE_MODE_CREATE
		mutation := queue.Mutation{Write: put}
		writes = append(writes, mutation)
		operation := &sink.DeleteOperation{Address: address}
		mutation = queue.Mutation{Delete: operation}
		deletes = append(deletes, mutation)
	}
	for _, batch := range [][]queue.Mutation{writes, deletes} {
		results := processor.HandleBatch(t.Context(), batch)
		for _, result := range results {
			if result != nil {
				t.Fatal(result)
			}
		}
	}
	for _, sizes := range [][]int{observed.writeSizes, observed.deleteSizes} {
		if len(sizes) != 7 || sizes[0] != 4 {
			t.Fatalf("unexpected split sizes %v", sizes)
		}
		singles := 0
		for _, count := range sizes {
			if count == 1 {
				singles++
			}
		}
		if singles != 4 {
			t.Fatalf("successful records replayed: %v", sizes)
		}
	}
}

func TestProcessorDoesNotSplitAmbiguousOrCanceledRequests(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if code == codes.ResourceExhausted {
				cancel()
			}
			applier := &capacityApplier{failure: status.Error(code, "injected failure")}
			processor, err := worker.NewProcessor(applier)
			if err != nil {
				t.Fatal(err)
			}
			first := processorPut(processorAddressFor("a"), "value")
			second := processorPut(processorAddressFor("b"), "value")
			writeA, writeB := queue.Mutation{Write: first}, queue.Mutation{Write: second}
			deleteA := &sink.DeleteOperation{Address: first.Address}
			deleteB := &sink.DeleteOperation{Address: second.Address}
			mutationA, mutationB := queue.Mutation{Delete: deleteA}, queue.Mutation{Delete: deleteB}
			for _, batch := range [][]queue.Mutation{{writeA, writeB}, {mutationA, mutationB}} {
				results := processor.HandleBatch(ctx, batch)
				for _, result := range results {
					var failure *worker.ApplyError
					if !errors.As(result, &failure) || !failure.Retryable() {
						t.Fatal(result)
					}
				}
			}
			if len(applier.writeSizes) != 1 || len(applier.deleteSizes) != 1 {
				t.Fatal("ambiguous or canceled request was split")
			}
		})
	}
}
