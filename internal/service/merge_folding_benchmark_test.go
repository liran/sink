package service_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

// Controlled I/O cost makes the number of backend rounds visible independently
// of a developer's network. These timings are not production latency estimates.
type foldingBenchmarkStorage struct {
	storage.Storage
	delay           time.Duration
	reads, writes   int
	visibilityWaits int
}

func (s *foldingBenchmarkStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads += len(req.Operations)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Read(ctx, req)
}

func (s *foldingBenchmarkStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes += len(req.Operations)
	if req.WaitUntilVisible {
		s.visibilityWaits++
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Write(ctx, req)
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
				b.ReportMetric(float64(backend.reads)/float64(b.N), "reads/batch")
				b.ReportMetric(float64(backend.writes)/float64(b.N), "writes/batch")
				b.ReportMetric(float64(backend.visibilityWaits)/float64(b.N), "visibility-waits/batch")
			})
		}
	}
}
