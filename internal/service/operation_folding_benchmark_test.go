package service_test

import (
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/memory"
)

func BenchmarkRecordFolding(b *testing.B) {
	for _, method := range []string{"upsert", "replace", "put_merge", "read", "delete"} {
		for _, workload := range []string{"hot", "mixed", "unique"} {
			for _, delay := range []time.Duration{0, time.Millisecond} {
				b.Run(fmt.Sprintf("%s/%s/io=%s", method, workload, delay), func(b *testing.B) {
					memoryStore := memory.New()
					backend := &foldingBenchmarkStorage{Storage: memoryStore, delay: delay}
					server := newTestServer(b, backend, nil)
					write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
					read := &sink.ReadRequest{}
					remove := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE}
					for index := range 64 {
						key := fmt.Sprintf("record-%d", index)
						if workload == "hot" || (workload == "mixed" && index%2 == 0) {
							key = "hot"
						}
						seed := memory.SeedRequest{Address: storageAddress(key), Document: storageJSONDocument(`{"value":0}`)}
						memoryStore.Seed(seed)
						mode := sink.WriteMode_WRITE_MODE_UPSERT
						if method == "replace" {
							mode = sink.WriteMode_WRITE_MODE_REPLACE
						}
						operation := foldingPut(key, mode, index)
						if method == "put_merge" && index%2 == 1 {
							operation = foldingMerge(key, incrementLua, `{"value":1}`, sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL)
						}
						write.Operations = append(write.Operations, operation)
						readOperation := &sink.ReadOperation{Address: protoAddress(key)}
						deleteOperation := &sink.DeleteOperation{Address: protoAddress(key)}
						read.Operations = append(read.Operations, readOperation)
						remove.Operations = append(remove.Operations, deleteOperation)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						switch method {
						case "read":
							response, err := server.Read(b.Context(), read)
							if err != nil {
								b.Fatal(err)
							}
							for _, result := range response.Results {
								if result.Status != sink.ReadStatus_READ_STATUS_FOUND {
									b.Fatal(result)
								}
							}
						case "delete":
							response, err := server.Delete(b.Context(), remove)
							if err != nil {
								b.Fatal(err)
							}
							for _, result := range response.Results {
								if result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
									b.Fatal(result)
								}
							}
						default:
							response, err := server.Write(b.Context(), write)
							if err != nil {
								b.Fatal(err)
							}
							for _, result := range response.Results {
								if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
									b.Fatal(result)
								}
							}
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(backend.reads.Load())/float64(b.N), "reads/batch")
					b.ReportMetric(float64(backend.writes.Load())/float64(b.N), "writes/batch")
					b.ReportMetric(float64(backend.deletes.Load())/float64(b.N), "deletes/batch")
					b.ReportMetric(float64(backend.visibilityWaits.Load())/float64(b.N), "visibility-waits/batch")
				})
			}
		}
	}
}
