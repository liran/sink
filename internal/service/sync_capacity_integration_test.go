//go:build integration

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"github.com/liran/sink/internal/storage/search"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type syncCapacityFixture struct {
	backend   storage.Storage
	namespace string
	dataset   string
	encoding  sink.DocumentEncoding
}

// Run only against disposable loopback databases. Every sample uses its own
// collection/index and verifies the final counter for every independent writer.
func BenchmarkSynchronousStorage(b *testing.B) {
	for _, driver := range []string{"mongodb", "opensearch"} {
		b.Run(driver, func(b *testing.B) {
			for _, workload := range []string{"upsert", "merge", "merge-visible"} {
				if driver == "mongodb" && workload == "merge-visible" {
					continue
				}
				for _, concurrency := range []int{32, 128, 512} {
					b.Run(fmt.Sprintf("%s/concurrency=%d", workload, concurrency), func(b *testing.B) {
						test := syncCapacityCase{driver: driver, workload: workload, concurrency: concurrency}
						benchmarkSynchronousStorage(b, test)
					})
				}
			}
		})
	}
}

type syncCapacityCase struct {
	driver      string
	workload    string
	concurrency int
}

func benchmarkSynchronousStorage(b *testing.B, test syncCapacityCase) {
	fixture := newSyncCapacityFixture(b, test.driver)
	client := newSyncCapacityClient(b, fixture)
	requests := make([]*sink.WriteRequest, test.concurrency)
	seed := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	read := &sink.ReadRequest{}
	for index := range test.concurrency {
		address := completionAddress(fmt.Sprintf("writer-%d", index))
		address.Namespace, address.Dataset = fixture.namespace, fixture.dataset
		value := map[string]any{"value": 0, "padding": strings.Repeat("x", 1024)}
		document := syncCapacityDocument(b, fixture.encoding, value)
		put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Address: address, Action: action}
		seed.Operations = append(seed.Operations, operation)
		readOperation := &sink.ReadOperation{Address: address}
		read.Operations = append(read.Operations, readOperation)
		mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
		if test.workload != "upsert" {
			operation = completionMerge(fmt.Sprintf("writer-%d", index), 1)
			operation.Address = address
			incoming := map[string]any{"value": 1}
			operation.GetMerge().IncomingDocument = syncCapacityDocument(b, fixture.encoding, incoming)
			operation.GetMerge().LuaProgram.Source = []byte(`return function(current, incoming) current.value=current.value+incoming.value; return current end`)
		}
		if test.workload == "merge-visible" {
			mode = sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
		}
		request := &sink.WriteRequest{CompletionMode: mode, Operations: []*sink.WriteOperation{operation}}
		requests[index] = request
	}
	seeded, err := client.Write(b.Context(), seed)
	if err != nil {
		b.Fatal(err)
	}
	if len(seeded.Results) != test.concurrency {
		b.Fatalf("seed returned %d results, want %d", len(seeded.Results), test.concurrency)
	}
	for _, result := range seeded.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			b.Fatal(result)
		}
	}
	var position atomic.Int64
	var workers sync.WaitGroup
	counts := make([]int, test.concurrency)
	durations := make([]time.Duration, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for writer := range test.concurrency {
		workers.Go(func() {
			request := requests[writer]
			for {
				index := int(position.Add(1) - 1)
				if index >= b.N {
					return
				}
				started := time.Now()
				response, writeErr := client.Write(b.Context(), request)
				elapsed := time.Since(started)
				durations[index] = elapsed
				if writeErr != nil || len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
					b.Errorf("write failed: %v, %v", response, writeErr)
					return
				}
				counts[writer]++
			}
		})
	}
	workers.Wait()
	b.StopTimer()
	if b.Failed() {
		return
	}
	slices.Sort(durations)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
	b.ReportMetric(float64(durations[(len(durations)-1)*95/100])/float64(time.Millisecond), "p95-ms")
	b.ReportMetric(float64(durations[(len(durations)-1)*99/100])/float64(time.Millisecond), "p99-ms")
	stored, err := client.Read(b.Context(), read)
	if err != nil {
		b.Fatal(err)
	}
	if len(stored.Results) != test.concurrency {
		b.Fatalf("read returned %d results, want %d", len(stored.Results), test.concurrency)
	}
	for index, result := range stored.Results {
		if result.Status != sink.ReadStatus_READ_STATUS_FOUND {
			b.Fatal(result)
		}
		var value map[string]any
		if fixture.encoding == sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
			err = bson.Unmarshal(result.Document.Payload, &value)
		} else {
			err = json.Unmarshal(result.Document.Payload, &value)
		}
		want := counts[index]
		if test.workload == "upsert" {
			want = 0
		}
		if err != nil || fmt.Sprint(value["value"]) != fmt.Sprint(want) {
			b.Fatalf("writer %d persisted %v, want %d: %v", index, value["value"], want, err)
		}
	}
}

func newSyncCapacityClient(b *testing.B, fixture syncCapacityFixture) sink.SinkClient {
	b.Helper()
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		b.Fatal(err)
	}
	serverOptions := Options{Storage: fixture.backend, Lua: engine, StoreNames: []string{"primary"}}
	if value := os.Getenv("SINK_SYNC_BENCH_EXECUTION_MIB"); value != "" {
		mib, parseErr := strconv.Atoi(value)
		if parseErr != nil || mib <= 0 || mib > 65536 {
			b.Fatal("SINK_SYNC_BENCH_EXECUTION_MIB must be between 1 and 65536")
		}
		serverOptions.MaxInFlightBytes = mib << 20
	}
	core, err := New(serverOptions)
	if err != nil {
		b.Fatal(err)
	}
	maxWait := 10 * time.Millisecond
	if value := os.Getenv("SINK_SYNC_BENCH_WAIT"); value != "" {
		maxWait, err = time.ParseDuration(value)
		if err != nil || maxWait <= 0 {
			b.Fatal("SINK_SYNC_BENCH_WAIT must be a positive duration")
		}
	}
	batchOptions := BatchingOptions{StoreNames: []string{"primary"}, MaxWait: maxWait}
	batched, err := NewBatchingServer(core, batchOptions)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(batched.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	codec := protocol.NewVTProtoCodec()
	server := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	sink.RegisterSinkServer(server, batched)
	go func() { _ = server.Serve(listener) }()
	b.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec)))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = connection.Close() })
	return sink.NewSinkClient(connection)
}

func syncCapacityDocument(b testing.TB, encoding sink.DocumentEncoding, value map[string]any) *sink.Document {
	b.Helper()
	var payload []byte
	var err error
	if encoding == sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
		payload, err = bson.Marshal(value)
	} else {
		payload, err = json.Marshal(value)
	}
	if err != nil {
		b.Fatal(err)
	}
	document := &sink.Document{Encoding: encoding, Payload: payload}
	return document
}

func newSyncCapacityFixture(b testing.TB, driver string) syncCapacityFixture {
	b.Helper()
	name := fmt.Sprintf("sink_capacity_%d", time.Now().UnixNano())
	fixture := syncCapacityFixture{namespace: name, dataset: name, encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON}
	variable := "SINK_SEARCH_TEST_ENDPOINT"
	if driver == "mongodb" {
		variable = "SINK_MONGODB_TEST_URI"
	}
	endpoint := os.Getenv(variable)
	if endpoint == "" {
		b.Skip(variable + " is not set")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		b.Fatal("capacity benchmarks require disposable loopback databases")
	}
	if driver == "mongodb" {
		clientOptions := options.Client().ApplyURI(endpoint)
		client, connectErr := mongo.Connect(clientOptions)
		if connectErr != nil {
			b.Fatal(connectErr)
		}
		b.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if dropErr := client.Database(name).Drop(ctx); dropErr != nil {
				b.Error(dropErr)
			}
			_ = client.Disconnect(ctx)
		})
		storeOptions := mongodb.Options{Store: "primary"}
		fixture.backend, err = mongodb.New(client, storeOptions)
		fixture.encoding = sink.DocumentEncoding_DOCUMENT_ENCODING_BSON
	} else {
		storeOptions := search.Options{Driver: search.DriverOpenSearch, Endpoints: []string{endpoint}, Store: "primary"}
		fixture.backend, err = search.New(storeOptions)
		settings := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"1s"}}`)
		syncCapacityIndexRequest(b, http.MethodPut, endpoint+"/"+name, settings)
		b.Cleanup(func() { syncCapacityIndexRequest(b, http.MethodDelete, endpoint+"/"+name, nil) })
	}
	if err != nil {
		b.Fatal(err)
	}
	return fixture
}

func syncCapacityIndexRequest(b testing.TB, method, endpoint string, payload []byte) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		b.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		b.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		b.Fatalf("index %s returned HTTP %d", method, response.StatusCode)
	}
}

func TestSynchronousStorageStreamsLargeRecords(t *testing.T) {
	for _, driver := range []string{"mongodb", "opensearch"} {
		for _, scenario := range []string{"snapshots", "outputs"} {
			t.Run(driver+"/"+scenario, func(t *testing.T) {
				fixture := newSyncCapacityFixture(t, driver)
				backend := &syncCapacityStorage{Storage: fixture.backend}
				server := completionServer(t, backend)
				server.server.maxReadBytes = 1024
				var seed storage.WriteRequest
				var read storage.ReadRequest
				var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
				for index := range 8 {
					key := fmt.Sprintf("record-%d", index)
					operation := completionMerge(key, 1)
					operation.Address.Namespace, operation.Address.Dataset = fixture.namespace, fixture.dataset
					operation.ReturnDocument = true
					incoming := map[string]any{"value": 1}
					operation.GetMerge().IncomingDocument = syncCapacityDocument(t, fixture.encoding, incoming)
					operation.GetMerge().LuaProgram.Source = []byte(fmt.Sprintf(`return function(current, incoming) return {value=current.value+incoming.value,padding=%q} end`, strings.Repeat("x", 700)))
					padding := ""
					if scenario == "snapshots" {
						padding = strings.Repeat("x", 700)
					}
					value := map[string]any{"value": 0, "padding": padding}
					document := syncCapacityDocument(t, fixture.encoding, value)
					address, err := convertAddress(operation.Address)
					if err != nil {
						t.Fatal(err)
					}
					storedDocument, err := convertDocument(document)
					if err != nil {
						t.Fatal(err)
					}
					writeOperation := storage.WriteOperation{Address: address, Document: storedDocument}
					seed.Operations = append(seed.Operations, writeOperation)
					readOperation := storage.ReadOperation{Address: address}
					read.Operations = append(read.Operations, readOperation)
					call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operation)
					calls = append(calls, call)
				}
				seeded, err := fixture.backend.Write(t.Context(), seed)
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range seeded.Results {
					if result.Status != storage.WriteStatusApplied {
						t.Fatal(result)
					}
				}
				server.executeWrites(t.Context(), calls)
				for _, call := range calls {
					result := awaitCompletion(t, call.result)
					if result.err != nil || len(result.response.GetResults()) != 1 || result.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
						t.Fatalf("chunk failed: %v, %v", result.response, result.err)
					}
					if len(result.response.Results[0].GetDocument().GetPayload()) < 700 {
						t.Fatal("chunk lost returned document")
					}
				}
				if backend.writes.Load() < 2 || (scenario == "snapshots" && backend.reads.Load() < 2) {
					t.Fatalf("records did not stream: reads=%d writes=%d", backend.reads.Load(), backend.writes.Load())
				}
				stored, err := fixture.backend.Read(t.Context(), read)
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range stored.Results {
					var value map[string]any
					if fixture.encoding == sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
						err = bson.Unmarshal(result.Document.Payload, &value)
					} else {
						err = json.Unmarshal(result.Document.Payload, &value)
					}
					if err != nil || fmt.Sprint(value["value"]) != "1" || value["padding"] != strings.Repeat("x", 700) {
						t.Fatalf("chunk did not persist exactly once: %v, %v", result, err)
					}
				}
			})
		}
	}
}
