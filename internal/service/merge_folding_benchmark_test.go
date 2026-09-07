package service_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

// Controlled I/O cost makes the number of backend rounds visible independently
// of a developer's network. These timings are not production latency estimates.
type foldingBenchmarkStorage struct {
	storage.Storage
	delay           time.Duration
	reads, writes   atomic.Int64
	deletes         atomic.Int64
	visibilityWaits atomic.Int64
	conflicts       atomic.Int64
}

func (s *foldingBenchmarkStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	s.deletes.Add(int64(len(req.Operations)))
	if req.WaitUntilVisible {
		s.visibilityWaits.Add(1)
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Delete(ctx, req)
}

func (s *foldingBenchmarkStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads.Add(int64(len(req.Operations)))
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Read(ctx, req)
}

func (s *foldingBenchmarkStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes.Add(int64(len(req.Operations)))
	if req.WaitUntilVisible {
		s.visibilityWaits.Add(1)
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	response, err := s.Storage.Write(ctx, req)
	for _, result := range response.Results {
		if result.Status == storage.WriteStatusPreconditionFailed {
			s.conflicts.Add(1)
		}
	}
	return response, err
}

func BenchmarkMergeFolding(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Millisecond} {
		for _, workload := range []string{"hot", "mixed", "unique"} {
			b.Run(fmt.Sprintf("%s/io=%s", workload, delay), func(b *testing.B) {
				backend := &foldingBenchmarkStorage{Storage: memory.New(), delay: delay}
				server := newTestServer(b, backend, nil)
				var operations []*sink.WriteOperation
				for index := range 64 {
					key := fmt.Sprintf("document-%d", index)
					if workload == "hot" || (workload == "mixed" && index%2 == 0) {
						key = "hot"
					}
					operation := foldingMerge(key, incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
					operations = append(operations, operation)
				}
				request := foldingRequest(operations...)
				samples := make([]int64, 0, min(b.N, 100000))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					started := time.Now()
					response, err := server.Write(b.Context(), request)
					elapsed := time.Since(started).Nanoseconds()
					if len(samples) < cap(samples) {
						samples = append(samples, elapsed)
					}
					if err != nil {
						b.Fatal(err)
					}
					for _, result := range response.Results {
						if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
							b.Fatal(result)
						}
					}
				}
				b.StopTimer()
				slices.Sort(samples)
				b.ReportMetric(float64(samples[len(samples)/2]), "p50-ns/batch")
				b.ReportMetric(float64(samples[min(len(samples)-1, len(samples)*99/100)]), "p99-ns/batch")
				b.ReportMetric(float64(backend.reads.Load())/float64(b.N), "reads/batch")
				b.ReportMetric(float64(backend.writes.Load())/float64(b.N), "writes/batch")
				b.ReportMetric(float64(backend.visibilityWaits.Load())/float64(b.N), "visibility-waits/batch")
			})
		}
	}
}

func BenchmarkMergeFoldingContention(b *testing.B) {
	backend := &foldingBenchmarkStorage{Storage: memory.New(), delay: time.Millisecond}
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		b.Fatal(err)
	}
	options := service.Options{Storage: backend, Lua: engine, MaxReadBytes: 1 << 20, MaxMergeAttempts: 1000}
	server, err := service.New(options)
	if err != nil {
		b.Fatal(err)
	}
	var operations []*sink.WriteOperation
	for range 64 {
		operation := foldingMerge("hot", incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE)
		operations = append(operations, operation)
	}
	request := foldingRequest(operations...)
	var mu sync.Mutex
	samples := make([]int64, 0, min(b.N, 100000))
	var failures atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			started := time.Now()
			response, err := server.Write(b.Context(), request)
			elapsed := time.Since(started).Nanoseconds()
			mu.Lock()
			if len(samples) < cap(samples) {
				samples = append(samples, elapsed)
			}
			mu.Unlock()
			if err != nil {
				failures.Add(1)
				continue
			}
			for _, result := range response.Results {
				if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					failures.Add(1)
				}
			}
		}
	})
	b.StopTimer()
	if failures.Load() != 0 {
		b.Fatalf("failed operations/RPCs = %d", failures.Load())
	}
	if got := foldingValue(b, backend.Storage, "hot"); got != b.N*64 {
		b.Fatalf("lost/repeated updates: got %d want %d", got, b.N*64)
	}
	slices.Sort(samples)
	b.ReportMetric(float64(samples[len(samples)/2]), "p50-ns/batch")
	b.ReportMetric(float64(samples[min(len(samples)-1, len(samples)*99/100)]), "p99-ns/batch")
	b.ReportMetric(float64(backend.reads.Load())/float64(b.N), "reads/batch")
	b.ReportMetric(float64(backend.writes.Load())/float64(b.N), "writes/batch")
	b.ReportMetric(float64(backend.conflicts.Load())/float64(b.N), "conflicts/batch")
	b.ReportMetric(float64(backend.conflicts.Load())/float64(backend.writes.Load()), "conflicts/write-attempt")
}
