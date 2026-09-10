// Command sink-perf exercises synchronous Sink RPCs against disposable datasets.
// It is opt-in: ordinary go test runs never connect to a server.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver/dns"
	"google.golang.org/grpc/status"
)

type settings struct {
	Address         string        `json:"-"`
	SearchEndpoint  string        `json:"-"`
	Dataset         string        `json:"dataset"`
	Store           string        `json:"store"`
	Workload        string        `json:"workload"`
	Concurrency     int           `json:"concurrency"`
	Keys            int           `json:"keys"`
	HotKeys         int           `json:"hot_keys"`
	Padding         int           `json:"padding_bytes"`
	RandomPadding   bool          `json:"random_padding"`
	FullIncoming    bool          `json:"full_incoming_document"`
	Fields          int           `json:"extra_string_fields"`
	Batch           int           `json:"operations_per_rpc"`
	Duration        time.Duration `json:"duration_ns"`
	Timeout         time.Duration `json:"rpc_timeout_ns"`
	Rate            float64       `json:"offered_rpcs_per_second"`
	Visible         bool          `json:"wait_until_visible"`
	ReturnDocument  bool          `json:"return_document"`
	Replicas        int           `json:"search_replicas"`
	Shards          int           `json:"search_shards"`
	ActiveShards    string        `json:"search_active_shards"`
	Connections     int           `json:"connections"`
	WarmConnections bool          `json:"warm_connections"`
	ColdMapping     bool          `json:"cold_mapping"`
	ErrorBackoff    time.Duration `json:"error_backoff_ns"`
	DNSMinInterval  time.Duration `json:"dns_min_resolution_interval_ns"`
}

type document struct {
	Value   int64             `json:"value" bson:"value"`
	Padding string            `json:"padding" bson:"padding"`
	Seen    map[string]int64  `json:"seen" bson:"seen"`
	Fields  map[string]string `json:"fields,omitempty" bson:"fields,omitempty"`
}

type worker struct {
	client            sink.SinkClient
	padding           string
	fields            map[string]string
	index             int
	keys              []int
	counts            []int64
	pending           []bool
	latencies         []time.Duration
	wire              []time.Duration
	errors            map[string]int64
	operationFailures map[string]string
	rpcs              int64
	succeeded         int64
	ops               int64
	reads             int64
	returned          int64
	failures          []float64
	seconds           map[int]*secondReport
	paddingCache      map[int]string
	paddingCacheBytes int
}

type secondReport struct {
	Second    int   `json:"second"`
	Succeeded int64 `json:"successful_rpcs"`
	Failed    int64 `json:"failed_rpcs"`
}

type workerFailure struct {
	Worker              int               `json:"worker"`
	Channel             int               `json:"channel"`
	Errors              map[string]int64  `json:"errors"`
	FirstFailureSeconds []float64         `json:"first_failure_seconds"`
	OperationFailures   map[string]string `json:"operation_failure_samples,omitempty"`
}

type report struct {
	Settings       settings         `json:"settings"`
	GoVersion      string           `json:"go_version"`
	GoMaxProcs     int              `json:"client_gomaxprocs"`
	Elapsed        float64          `json:"elapsed_seconds"`
	StartedUnixNS  int64            `json:"started_unix_ns"`
	FinishedUnixNS int64            `json:"finished_unix_ns"`
	RPCs           int64            `json:"rpcs"`
	SuccessfulRPCs int64            `json:"successful_rpcs"`
	Operations     int64            `json:"successful_operations"`
	ReadOperations int64            `json:"read_operations"`
	ReturnedBytes  int64            `json:"returned_bytes"`
	RPCsPerSecond  float64          `json:"rpcs_per_second"`
	OpsPerSecond   float64          `json:"operations_per_second"`
	P50            float64          `json:"p50_ms"`
	P95            float64          `json:"p95_ms"`
	P99            float64          `json:"p99_ms"`
	Max            float64          `json:"max_ms"`
	WireP99        float64          `json:"execution_p99_ms"`
	NotIssued      int64            `json:"scheduled_not_issued"`
	Errors         map[string]int64 `json:"errors"`
	Verified       bool             `json:"verified"`
	Healthy        bool             `json:"healthy"`
	Reconciled     int64            `json:"reconciled_unacknowledged"`
	WarmupSeconds  float64          `json:"connection_warmup_seconds"`
	WorkerFailures []workerFailure  `json:"worker_failures,omitempty"`
	Seconds        []secondReport   `json:"timeline"`
}

const mergeSource = `return function(current, incoming)
  current.seen = current.seen or {}
  local previous = current.seen[incoming.writer] or 0
  if incoming.seq <= previous then return current end
  current.value = current.value + 1
  current.seen[incoming.writer] = incoming.seq
  return current
end`

func main() {
	opts := settings{}
	flag.StringVar(&opts.Address, "address", "dns:///sink:8080", "Sink address; use headless DNS for replica balancing")
	flag.StringVar(&opts.SearchEndpoint, "search-endpoint", "http://opensearch:9200", "Disposable OpenSearch endpoint")
	flag.StringVar(&opts.Dataset, "dataset", "", "Required unique disposable dataset, beginning perf-")
	flag.StringVar(&opts.Store, "store", "mongo", "Logical storage: mongo or search")
	flag.StringVar(&opts.Workload, "workload", "merge", "merge, heavy-merge, upsert, read, or mixed (80% merge / 20% read)")
	flag.IntVar(&opts.Concurrency, "concurrency", 64, "Concurrent RPC workers")
	flag.IntVar(&opts.Keys, "keys", 4096, "Distinct records, rounded up to equal worker partitions")
	flag.IntVar(&opts.HotKeys, "hot-keys", 0, "Shared keys for contention tests; zero uses disjoint worker partitions")
	flag.IntVar(&opts.Padding, "padding", 1024, "Padding bytes per stored document")
	flag.BoolVar(&opts.RandomPadding, "random-padding", false, "Use deterministic per-key random-looking text instead of repeated x padding")
	flag.BoolVar(&opts.FullIncoming, "full-incoming", false, "Send and merge the padding and extra fields on every mutation, instead of a small counter delta")
	flag.IntVar(&opts.Fields, "fields", 0, "Additional 16-byte string fields to exercise document conversion")
	flag.IntVar(&opts.Batch, "batch", 1, "Operations per RPC")
	flag.DurationVar(&opts.Duration, "duration", 30*time.Second, "Measured duration; setup and reconciliation are excluded")
	flag.DurationVar(&opts.Timeout, "timeout", 5*time.Second, "Individual RPC deadline")
	flag.Float64Var(&opts.Rate, "rate", 0, "Fixed offered RPC rate; zero is closed-loop saturation")
	flag.BoolVar(&opts.Visible, "visible", false, "Wait for search visibility")
	flag.BoolVar(&opts.ReturnDocument, "return-document", false, "Return committed write documents")
	flag.IntVar(&opts.Replicas, "search-replicas", 0, "Search index replicas; all copies must be active before load")
	flag.IntVar(&opts.Shards, "search-shards", 1, "Search primary shards")
	flag.StringVar(&opts.ActiveShards, "active-shards", "1", "Search write availability requirement: 1 or all")
	flag.IntVar(&opts.Connections, "connections", 4, "Independent gRPC channels, all using round_robin")
	flag.BoolVar(&opts.WarmConnections, "warm-connections", true, "Connect every gRPC channel before measuring; disable to test cold starts")
	flag.BoolVar(&opts.ColdMapping, "cold-mapping", false, "Introduce writer counter fields during load instead of setup to test dynamic mapping bursts")
	flag.DurationVar(&opts.ErrorBackoff, "error-backoff", 10*time.Millisecond, "Delay after a failed RPC; prevents retry storms during fault tests")
	flag.DurationVar(&opts.DNSMinInterval, "dns-min-interval", 30*time.Second, "Minimum interval between gRPC DNS re-resolutions; compare 5s during Pod replacement tests")
	flag.Parse()
	if err := validateSettings(&opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// The gRPC setting is process-wide and must be set before creating clients.
	dns.SetMinResolutionInterval(opts.DNSMinInterval)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result, err := execute(ctx, opts)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !result.Verified {
		os.Exit(1)
	}
}

func validateSettings(opts *settings) error {
	validName, _ := regexp.MatchString(`^perf-[a-z0-9-]{1,55}$`, opts.Dataset)
	if !validName {
		return errors.New("use a unique disposable dataset beginning perf-, at most 60 lowercase letters, digits and hyphens")
	}
	if opts.Store != "mongo" && opts.Store != "search" {
		return errors.New("store must be mongo or search")
	}
	if !slices.Contains([]string{"merge", "heavy-merge", "upsert", "read", "mixed"}, opts.Workload) {
		return errors.New("unknown workload")
	}
	if opts.Concurrency < 1 || opts.Concurrency > 4096 || opts.Batch < 1 || opts.Batch > 1000 || opts.Keys < 1 || opts.Keys > 1_000_000 || opts.HotKeys < 0 || opts.Padding < 0 || opts.Padding > 1<<20 || opts.Duration <= 0 || opts.Timeout <= 0 || opts.Rate < 0 || opts.Connections < 1 || opts.Connections > 128 {
		return errors.New("invalid load limits")
	}
	if opts.HotKeys > 0 && opts.Workload == "upsert" {
		return errors.New("hot-key upserts cannot be verified as counters; use merge")
	}
	if opts.HotKeys > 1_000_000 || opts.Shards < 1 || opts.Replicas < 0 || opts.Replicas > 2 {
		return errors.New("invalid key or replica limits")
	}
	if opts.ActiveShards != "" && opts.ActiveShards != "1" && opts.ActiveShards != "all" {
		return errors.New("active shards must be 1 or all")
	}
	if opts.Fields < 0 || opts.Fields > 1000 {
		return errors.New("fields must be between zero and 1000")
	}
	if opts.ErrorBackoff < 0 || opts.DNSMinInterval < 0 {
		return errors.New("backoff and DNS intervals must not be negative")
	}
	if opts.HotKeys > 0 {
		opts.Keys = opts.HotKeys
	} else {
		perWorker := max(opts.Batch, (opts.Keys+opts.Concurrency-1)/opts.Concurrency)
		opts.Keys = perWorker * opts.Concurrency
	}
	if opts.Batch > opts.Keys {
		return errors.New("batch exceeds the number of keys")
	}
	return nil
}

func address(opts settings, key int) *sink.RecordAddress {
	kind := &sink.RecordKey_StringValue{StringValue: fmt.Sprintf("record-%d", key)}
	recordKey := &sink.RecordKey{Kind: kind}
	result := &sink.RecordAddress{Store: opts.Store, Namespace: opts.Dataset, Dataset: opts.Dataset, Key: recordKey}
	return result
}

func encodeDocument(opts settings, value any) (*sink.Document, error) {
	encoding := sink.DocumentEncoding_DOCUMENT_ENCODING_JSON
	var payload []byte
	var err error
	if opts.Store == "mongo" {
		encoding = sink.DocumentEncoding_DOCUMENT_ENCODING_BSON
		payload, err = bson.Marshal(value)
	} else {
		payload, err = json.Marshal(value)
	}
	result := &sink.Document{Encoding: encoding, Payload: payload}
	return result, err
}

func decodeDocument(value *sink.Document) (document, error) {
	var result document
	if value.GetEncoding() == sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
		err := bson.Unmarshal(value.GetPayload(), &result)
		return result, err
	}
	err := json.Unmarshal(value.GetPayload(), &result)
	return result, err
}

func execute(ctx context.Context, opts settings) (report, error) {
	result := report{Settings: opts, GoVersion: runtime.Version(), GoMaxProcs: runtime.GOMAXPROCS(0), Errors: make(map[string]int64)}
	codec := protocol.NewVTProtoCodec()
	clients := make([]sink.SinkClient, opts.Connections)
	for index := range clients {
		connection, err := grpc.NewClient(opts.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`), grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec), grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
		if err != nil {
			return result, err
		}
		defer connection.Close()
		clients[index] = sink.NewSinkClient(connection)
	}
	if err := seed(ctx, opts, clients[0]); err != nil {
		return result, err
	}
	if opts.WarmConnections {
		started := time.Now()
		for _, client := range clients {
			operation := &sink.ReadOperation{Address: address(opts, 0)}
			request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
			call, cancel := context.WithTimeout(ctx, 30*time.Second)
			response, err := client.Read(call, request)
			cancel()
			if err != nil {
				return result, fmt.Errorf("warm gRPC channel: %w", err)
			}
			if len(response.Results) != 1 || response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
				return result, errors.New("warm gRPC channel did not read the seeded record")
			}
		}
		result.WarmupSeconds = time.Since(started).Seconds()
	}
	fmt.Fprintln(os.Stderr, "measured workload begins")
	workers := make([]*worker, opts.Concurrency)
	padding := strings.Repeat("x", opts.Padding)
	fields := extraFields(opts.Fields)
	for index := range workers {
		entry := &worker{client: clients[index%len(clients)], index: index, errors: make(map[string]int64), padding: padding, fields: fields, seconds: make(map[int]*secondReport)}
		count := opts.Keys / opts.Concurrency
		if opts.HotKeys > 0 {
			count = opts.Keys
		}
		entry.counts, entry.pending = make([]int64, count), make([]bool, count)
		for key := range count {
			global := index*count + key
			if opts.HotKeys > 0 {
				global = key
			}
			entry.keys = append(entry.keys, global)
		}
		workers[index] = entry
	}
	var tickets atomic.Int64
	var group sync.WaitGroup
	started := time.Now().Add(100 * time.Millisecond)
	for _, entry := range workers {
		group.Go(func() { entry.run(ctx, opts, started, &tickets) })
	}
	group.Wait()
	finished := time.Now()
	result.Elapsed = finished.Sub(started).Seconds()
	result.StartedUnixNS, result.FinishedUnixNS = started.UnixNano(), finished.UnixNano()
	fmt.Fprintln(os.Stderr, "measured workload finished; reconciliation begins")
	var latencies, wire []time.Duration
	seconds := make(map[int]secondReport)
	for _, entry := range workers {
		result.RPCs += entry.rpcs
		result.SuccessfulRPCs += entry.succeeded
		result.Operations += entry.ops
		result.ReadOperations += entry.reads
		result.ReturnedBytes += entry.returned
		latencies = append(latencies, entry.latencies...)
		wire = append(wire, entry.wire...)
		for second, sample := range entry.seconds {
			combined := seconds[second]
			combined.Second = second
			combined.Succeeded += sample.Succeeded
			combined.Failed += sample.Failed
			seconds[second] = combined
		}
		for code, count := range entry.errors {
			result.Errors[code] += count
		}
		if len(entry.errors) != 0 {
			failure := workerFailure{Worker: entry.index, Channel: entry.index % len(clients), Errors: entry.errors, FirstFailureSeconds: entry.failures, OperationFailures: entry.operationFailures}
			result.WorkerFailures = append(result.WorkerFailures, failure)
		}
	}
	for _, sample := range seconds {
		result.Seconds = append(result.Seconds, sample)
	}
	slices.SortFunc(result.Seconds, func(a, b secondReport) int { return a.Second - b.Second })
	result.RPCsPerSecond = float64(result.SuccessfulRPCs) / result.Elapsed
	result.OpsPerSecond = float64(result.Operations) / result.Elapsed
	if opts.Rate > 0 {
		result.NotIssued = max(0, int64(opts.Rate*opts.Duration.Seconds())-result.RPCs)
	}
	slices.Sort(latencies)
	slices.Sort(wire)
	result.P50, result.P95, result.P99, result.Max = percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99), percentile(latencies, 100)
	result.WireP99 = percentile(wire, 99)
	verified, reconciled, err := verify(ctx, opts, workers)
	result.Verified, result.Reconciled = verified, reconciled
	result.Healthy = verified && len(result.Errors) == 0 && result.NotIssued == 0
	return result, err
}

func percentile(values []time.Duration, percent int) float64 {
	if len(values) == 0 {
		return 0
	}
	index := (len(values) - 1) * percent / 100
	return float64(values[index]) / float64(time.Millisecond)
}

func seed(ctx context.Context, opts settings, client sink.SinkClient) error {
	if opts.Store == "search" {
		indexOptions := map[string]any{"number_of_shards": opts.Shards, "number_of_replicas": opts.Replicas, "refresh_interval": "1s", "mapping.total_fields.limit": max(1000, opts.Concurrency+opts.Fields+32), "translog.durability": "request"}
		if opts.ActiveShards != "" {
			indexOptions["write.wait_for_active_shards"] = opts.ActiveShards
		}
		indexSettings := map[string]any{"settings": indexOptions}
		payload, err := json.Marshal(indexSettings)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, opts.SearchEndpoint+"/"+opts.Dataset, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		httpClient := &http.Client{Timeout: 60 * time.Second}
		response, err := httpClient.Do(request)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("create fresh search index: HTTP %d", response.StatusCode)
		}
		healthRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.SearchEndpoint+"/_cluster/health/"+opts.Dataset+"?wait_for_status=green&timeout=60s", nil)
		if err != nil {
			return err
		}
		healthResponse, err := httpClient.Do(healthRequest)
		if err != nil {
			return err
		}
		var health struct {
			Status   string `json:"status"`
			TimedOut bool   `json:"timed_out"`
		}
		decodeErr := json.NewDecoder(healthResponse.Body).Decode(&health)
		_ = healthResponse.Body.Close()
		if decodeErr != nil || healthResponse.StatusCode != http.StatusOK || health.TimedOut || health.Status != "green" {
			return errors.New("search index did not reach green health before load")
		}
	}
	value := document{Padding: strings.Repeat("x", opts.Padding), Seen: make(map[string]int64), Fields: extraFields(opts.Fields)}
	batchSize := min(256, max(1, (8<<20)/(opts.Padding+256)))
	for begin := 0; begin < opts.Keys; begin += batchSize {
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
		for key := begin; key < min(opts.Keys, begin+batchSize); key++ {
			if opts.RandomPadding {
				value.Padding = randomPadding(opts.Padding, key)
			}
			if !opts.ColdMapping {
				value.Seen = make(map[string]int64)
				if opts.HotKeys > 0 {
					for writer := range opts.Concurrency {
						value.Seen[fmt.Sprintf("w%d", writer)] = 0
					}
				} else {
					writer := key / (opts.Keys / opts.Concurrency)
					value.Seen[fmt.Sprintf("w%d", writer)] = 0
				}
			}
			encoded, err := encodeDocument(opts, value)
			if err != nil {
				return err
			}
			put := &sink.PutOperation{Document: encoded, Mode: sink.WriteMode_WRITE_MODE_CREATE}
			action := &sink.WriteOperation_Put{Put: put}
			operation := &sink.WriteOperation{Address: address(opts, key), Action: action}
			request.Operations = append(request.Operations, operation)
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		response, err := client.Write(call, request)
		cancel()
		if err != nil {
			return fmt.Errorf("seed disposable dataset: %w", err)
		}
		if len(response.Results) != len(request.Operations) {
			return errors.New("seed returned an incomplete result")
		}
		for _, item := range response.Results {
			if item.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				return fmt.Errorf("seed failed: %v", item)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "seeded %d records\n", opts.Keys)
	return nil
}

func (w *worker) run(ctx context.Context, opts settings, started time.Time, tickets *atomic.Int64) {
	deadline := started.Add(opts.Duration)
	position := 0
	for iteration := int64(0); ctx.Err() == nil && time.Now().Before(deadline); iteration++ {
		due := time.Now()
		if opts.Rate > 0 {
			ticket := tickets.Add(1) - 1
			due = started.Add(time.Duration(float64(ticket) / opts.Rate * float64(time.Second)))
			if !due.Before(deadline) {
				break
			}
		} else if due.Before(started) {
			due = started
		}
		if delay := time.Until(due); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		keys := make([]int, opts.Batch)
		for index := range keys {
			keys[index] = (position + index) % len(w.keys)
		}
		position = (position + opts.Batch) % len(w.keys)
		read := opts.Workload == "read" || (opts.Workload == "mixed" && iteration%5 == 0)
		call, cancel := context.WithTimeout(ctx, opts.Timeout)
		wireStarted := time.Now()
		var success bool
		if read {
			success = w.read(call, opts, keys)
		} else {
			success = w.write(call, opts, keys)
		}
		cancel()
		finished := time.Now()
		w.latencies = append(w.latencies, finished.Sub(due))
		w.wire = append(w.wire, finished.Sub(wireStarted))
		w.rpcs++
		second := int(finished.Sub(started) / time.Second)
		sample := w.seconds[second]
		if sample == nil {
			sample = &secondReport{Second: second}
			w.seconds[second] = sample
		}
		if success {
			w.succeeded++
			sample.Succeeded++
		} else {
			sample.Failed++
			if len(w.failures) < 8 {
				w.failures = append(w.failures, finished.Sub(started).Seconds())
			}
			if opts.ErrorBackoff > 0 {
				timer := time.NewTimer(opts.ErrorBackoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
		}
	}
}

func (w *worker) read(ctx context.Context, opts settings, keys []int) bool {
	request := &sink.ReadRequest{}
	for _, key := range keys {
		operation := &sink.ReadOperation{Address: address(opts, w.keys[key])}
		request.Operations = append(request.Operations, operation)
	}
	response, err := w.client.Read(ctx, request)
	if err != nil {
		w.errors[status.Code(err).String()]++
		return false
	}
	if len(response.Results) != len(keys) {
		w.errors["incomplete_read"]++
		return false
	}
	success := true
	for _, item := range response.Results {
		if item.Status != sink.ReadStatus_READ_STATUS_FOUND {
			w.errors["read_"+item.Status.String()]++
			success = false
		} else {
			w.ops++
			w.reads++
			w.returned += int64(len(item.GetDocument().GetPayload()))
		}
	}
	return success
}

func (w *worker) write(ctx context.Context, opts settings, keys []int) bool {
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	if opts.Visible {
		mode = sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
	}
	request := &sink.WriteRequest{CompletionMode: mode}
	writer := fmt.Sprintf("w%d", w.index)
	var programReference *sink.LuaProgram
	if opts.Workload != "upsert" {
		source := mergeSource
		if opts.Workload == "heavy-merge" {
			source = strings.Replace(source, "current.value =", "local total=0; for i=1,1000 do total=total+i end; current.value =", 1)
		}
		if opts.FullIncoming {
			source = strings.Replace(source, "current.value =", "current.padding=incoming.padding; current.fields=incoming.fields; current.value =", 1)
		}
		digest := sha256.Sum256([]byte(source))
		program := &sink.LuaProgram{Source: []byte(source), Sha256: digest[:]}
		request.LuaPrograms = []*sink.LuaProgram{program}
		programReference = &sink.LuaProgram{Sha256: digest[:]}
	}
	for _, key := range keys {
		sequence := w.counts[key] + 1
		operation := &sink.WriteOperation{Address: address(opts, w.keys[key]), ReturnDocument: opts.ReturnDocument}
		if opts.Workload == "upsert" {
			padding := w.recordPadding(opts, key)
			value := document{Value: sequence, Padding: padding, Seen: map[string]int64{writer: sequence}, Fields: w.fields}
			encoded, err := encodeDocument(opts, value)
			if err != nil {
				w.errors["encode"]++
				return false
			}
			put := &sink.PutOperation{Document: encoded, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
			operation.Action = &sink.WriteOperation_Put{Put: put}
		} else {
			incoming := map[string]any{"writer": writer, "seq": sequence}
			if opts.FullIncoming {
				padding := w.recordPadding(opts, key)
				incoming["padding"] = padding
				incoming["fields"] = w.fields
			}
			encoded, err := encodeDocument(opts, incoming)
			if err != nil {
				w.errors["encode"]++
				return false
			}
			merge := &sink.MergeOperation{IncomingDocument: encoded, LuaProgram: programReference, MissingDocumentMode: sink.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL}
			operation.Action = &sink.WriteOperation_Merge{Merge: merge}
		}
		request.Operations = append(request.Operations, operation)
	}
	response, err := w.client.Write(ctx, request)
	if err != nil {
		w.errors[status.Code(err).String()]++
		for _, key := range keys {
			w.pending[key] = true
		}
		return false
	}
	if len(response.Results) != len(keys) {
		w.errors["incomplete_write"]++
		for _, key := range keys {
			w.pending[key] = true
		}
		return false
	}
	success := true
	for index, item := range response.Results {
		key := keys[index]
		if item.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			code := item.Status.String() + ":" + item.GetFailure().GetCode().String()
			w.errors[code]++
			if w.operationFailures == nil {
				w.operationFailures = make(map[string]string)
			}
			if _, found := w.operationFailures[code]; !found {
				detail := item.GetFailure().String()
				w.operationFailures[code] = detail[:min(len(detail), 2048)]
			}
			if w.index == 0 && w.errors[code] == 1 {
				fmt.Fprintf(os.Stderr, "first operation failure: %v\n", item.GetFailure())
			}
			w.pending[key] = w.pending[key] || mayHaveCommitted(item)
			success = false
			continue
		}
		w.counts[key]++
		w.pending[key] = false
		w.ops++
		if opts.ReturnDocument {
			stored, decodeErr := decodeDocument(item.GetDocument())
			padding := w.recordPadding(opts, key)
			if decodeErr != nil || stored.Seen[writer] != w.counts[key] || stored.Padding != padding || !maps.Equal(stored.Fields, w.fields) {
				w.errors["invalid_returned_document"]++
				success = false
			}
			w.returned += int64(len(item.GetDocument().GetPayload()))
		}
	}
	return success
}

func mayHaveCommitted(result *sink.WriteResult) bool {
	switch result.GetFailure().GetCode() {
	case sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED,
		sink.FailureCode_FAILURE_CODE_NOT_FOUND, sink.FailureCode_FAILURE_CODE_CONFLICT:
		return false
	default:
		return result.Status != sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
	}
}

func verify(ctx context.Context, opts settings, workers []*worker) (bool, int64, error) {
	var reconciled int64
	padding := strings.Repeat("x", opts.Padding)
	fields := extraFields(opts.Fields)
	batchSize := min(128, max(1, (16<<20)/(opts.Padding+512)))
	for begin := 0; begin < opts.Keys; {
		request := &sink.ReadRequest{}
		for key := begin; key < min(opts.Keys, begin+batchSize); key++ {
			operation := &sink.ReadOperation{Address: address(opts, key)}
			request.Operations = append(request.Operations, operation)
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		response, err := workers[0].client.Read(call, request)
		cancel()
		limited := status.Code(err) == codes.ResourceExhausted
		for _, item := range response.GetResults() {
			limited = limited || item.GetFailure().GetCode() == sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
		}
		// Deployments intentionally tune their read/response byte limit. Retry
		// only the read, before accounting any record from this response.
		if limited && len(request.Operations) > 1 {
			batchSize = max(1, len(request.Operations)/2)
			continue
		}
		if err != nil {
			return false, reconciled, fmt.Errorf("reconciliation read: %w", err)
		}
		if len(response.Results) != len(request.Operations) {
			return false, reconciled, errors.New("incomplete reconciliation read")
		}
		for offset, item := range response.Results {
			if item.Status != sink.ReadStatus_READ_STATUS_FOUND {
				return false, reconciled, fmt.Errorf("reconciliation record %d: %s, %s: %s", begin+offset,
					item.Status, item.GetFailure().GetCode(), item.GetFailure().GetMessage())
			}
			stored, err := decodeDocument(item.Document)
			if err != nil {
				return false, reconciled, err
			}
			key := begin + offset
			if opts.RandomPadding {
				padding = randomPadding(opts.Padding, key)
			}
			var expected int64
			for _, entry := range workers {
				localKey := key - entry.index*len(entry.keys)
				if opts.HotKeys > 0 {
					localKey = key
				}
				if localKey < 0 || localKey >= len(entry.keys) {
					continue
				}
				count := entry.counts[localKey]
				actual := stored.Seen[fmt.Sprintf("w%d", entry.index)]
				if actual == count+1 && entry.pending[localKey] {
					count++
					reconciled++
				}
				if actual != count {
					return false, reconciled, fmt.Errorf("record %d writer %d persisted sequence %d, acknowledged %d", key, entry.index, actual, entry.counts[localKey])
				}
				expected += count
			}
			if stored.Value != expected || stored.Padding != padding || !maps.Equal(stored.Fields, fields) {
				return false, reconciled, fmt.Errorf("record %d did not reconcile: value=%d want=%d padding=%d", key, stored.Value, expected, len(stored.Padding))
			}
		}
		begin += len(request.Operations)
	}
	return true, reconciled, nil
}

func extraFields(count int) map[string]string {
	fields := make(map[string]string, count)
	for index := range count {
		fields[fmt.Sprintf("field-%d", index)] = "0123456789abcdef"
	}
	return fields
}

func randomPadding(size, key int) string {
	// Reproducible synthetic ASCII, different for every record. This is not a
	// source of security-sensitive randomness. Spaces produce ordinary search
	// tokens instead of a single unusually long token.
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789  "
	state := uint64(key+1) * 0x9e3779b97f4a7c15
	payload := make([]byte, size)
	for index := range payload {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		payload[index] = alphabet[(state*0x2545f4914f6cdd1d)>>58]
	}
	return string(payload)
}

func (w *worker) recordPadding(opts settings, key int) string {
	if !opts.RandomPadding {
		return w.padding
	}
	if padding, found := w.paddingCache[key]; found {
		return padding
	}
	padding := randomPadding(opts.Padding, w.keys[key])
	// Avoid making synthetic payload generation the bottleneck for large
	// writes, while bounding the total client cache independently of key count.
	limit := (256 << 20) / max(1, opts.Concurrency)
	if w.paddingCacheBytes+len(padding) <= limit {
		if w.paddingCache == nil {
			w.paddingCache = make(map[int]string)
		}
		w.paddingCache[key] = padding
		w.paddingCacheBytes += len(padding)
	}
	return padding
}
