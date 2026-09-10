package service

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

// Count actual adapter rounds for the same collected RPCs, including admission
// splitting. The optional delay models I/O cost, not production latency.
type syncCapacityStorage struct {
	storage.Storage
	delay  time.Duration
	reads  atomic.Int64
	writes atomic.Int64
}

func (s *syncCapacityStorage) Read(ctx context.Context, request storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Read(ctx, request)
}

func (s *syncCapacityStorage) Write(ctx context.Context, request storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Write(ctx, request)
}

func BenchmarkSynchronousMergeMicrobatch(b *testing.B) {
	for _, callers := range []int{1, 32, 128, 512} {
		for _, delay := range []time.Duration{0, time.Millisecond} {
			b.Run(fmt.Sprintf("callers=%d/io=%s", callers, delay), func(b *testing.B) {
				backend := &syncCapacityStorage{Storage: memory.New(), delay: delay}
				luaOptions := merge.LuaOptions{}
				engine, err := merge.NewLuaEngine(luaOptions)
				if err != nil {
					b.Fatal(err)
				}
				options := Options{Storage: backend, Lua: engine, StoreNames: []string{"primary"}}
				core, err := New(options)
				if err != nil {
					b.Fatal(err)
				}
				server := &BatchingServer{server: core}
				operations := make([]*sink.WriteOperation, callers)
				for index := range operations {
					operations[index] = completionMerge(fmt.Sprintf("record-%d", index), 1)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					calls := make([]*batchCall[*sink.WriteRequest, *sink.WriteResponse], callers)
					for index, operation := range operations {
						calls[index] = completionWriteCall(b.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operation)
					}
					server.executeWrites(b.Context(), calls)
					for _, call := range calls {
						result := <-call.result
						if result.err != nil || result.response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
							b.Fatalf("write failed: %v, %v", result.response, result.err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(backend.reads.Load())/float64(b.N), "reads/batch")
				b.ReportMetric(float64(backend.writes.Load())/float64(b.N), "writes/batch")
				b.ReportMetric(float64(callers*b.N)/b.Elapsed().Seconds(), "ops/s")
			})
		}
	}
}
