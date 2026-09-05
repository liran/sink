// Package metrics exposes bounded-cardinality Prometheus metrics for Sink.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const namespace = "sink"

type Metrics struct {
	registry               *prometheus.Registry
	requests               *prometheus.CounterVec
	requestDuration        *prometheus.HistogramVec
	operationResults       *prometheus.CounterVec
	batcherBatches         *prometheus.CounterVec
	batcherOperations      *prometheus.HistogramVec
	batcherBytes           *prometheus.HistogramVec
	batcherQueueDuration   *prometheus.HistogramVec
	batcherExecution       *prometheus.HistogramVec
	batcherQueuedOps       *prometheus.GaugeVec
	batcherQueuedBytes     *prometheus.GaugeVec
	batcherRejected        *prometheus.CounterVec
	mergeConflicts         prometheus.Counter
	mergeExhausted         prometheus.Counter
	kafkaPublished         *prometheus.CounterVec
	kafkaPublishDuration   prometheus.Histogram
	kafkaWorkerMutations   *prometheus.CounterVec
	kafkaWorkerRetries     prometheus.Counter
	kafkaWorkerDeadLetters prometheus.Counter
	admissionRequests      prometheus.Gauge
	admissionBytes         prometheus.Gauge
	admissionRejected      prometheus.Counter
	workerLastPoll         *prometheus.GaugeVec
	workerLastCommit       *prometheus.GaugeVec
	workerOldest           *prometheus.GaugeVec
	workerPending          *prometheus.GaugeVec
	workerRecoveries       *prometheus.CounterVec
	workerFetchErrors      *prometheus.CounterVec
	workerDelivery         *prometheus.HistogramVec
	workerQuarantined      *prometheus.CounterVec
	workerOffsetGap        *prometheus.GaugeVec
}

type BatchObservation struct {
	Method            string
	Reason            string
	Operations        int
	Bytes             int
	QueueDuration     time.Duration
	ExecutionDuration time.Duration
}

func New(version string) (*Metrics, error) {
	requestOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "requests_total",
		Help:      "Total number of completed Sink gRPC requests.",
	}
	requests := prometheus.NewCounterVec(requestOptions, []string{"method", "code"})
	durationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "request_duration_seconds",
		Help:      "Duration of completed Sink gRPC requests in seconds.",
		Buckets:   prometheus.DefBuckets,
	}
	requestDuration := prometheus.NewHistogramVec(durationOptions, []string{"method"})
	resultOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "operation_results_total",
		Help:      "Total number of per-operation results returned by Sink gRPC requests.",
	}
	operationResults := prometheus.NewCounterVec(resultOptions, []string{"method", "status"})
	batchOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "batches_total",
		Help:      "Total number of synchronous batches by flush reason.",
	}
	batcherBatches := prometheus.NewCounterVec(batchOptions, []string{"method", "reason"})
	batchOperationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "operations",
		Help:      "Number of operations in each synchronous batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11),
	}
	batcherOperations := prometheus.NewHistogramVec(batchOperationOptions, []string{"method"})
	batchByteOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "bytes",
		Help:      "Encoded request bytes represented by each synchronous batch.",
		Buckets:   prometheus.ExponentialBuckets(1024, 4, 9),
	}
	batcherBytes := prometheus.NewHistogramVec(batchByteOptions, []string{"method"})
	batchQueueDurationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queue_duration_seconds",
		Help:      "Oldest request queue duration before a synchronous batch starts.",
		Buckets:   prometheus.ExponentialBuckets(0.00025, 2, 12),
	}
	batcherQueueDuration := prometheus.NewHistogramVec(batchQueueDurationOptions, []string{"method"})
	batchExecutionOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "execution_duration_seconds",
		Help:      "Execution duration of synchronous batches.",
		Buckets:   prometheus.DefBuckets,
	}
	batcherExecution := prometheus.NewHistogramVec(batchExecutionOptions, []string{"method"})
	queuedOperationOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queued_operations",
		Help:      "Current number of synchronous operations waiting for execution.",
	}
	batcherQueuedOps := prometheus.NewGaugeVec(queuedOperationOptions, []string{"method"})
	queuedByteOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queued_bytes",
		Help:      "Current encoded request bytes waiting for synchronous execution.",
	}
	batcherQueuedBytes := prometheus.NewGaugeVec(queuedByteOptions, []string{"method"})
	rejectedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "rejected_total",
		Help:      "Total number of synchronous requests rejected before batching.",
	}
	batcherRejected := prometheus.NewCounterVec(rejectedOptions, []string{"method", "reason"})
	mergeConflictOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "merge",
		Name:      "conflicts_total",
		Help:      "Total number of revision conflicts retried by Lua merges.",
	}
	mergeConflicts := prometheus.NewCounter(mergeConflictOptions)
	mergeExhaustedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "merge",
		Name:      "exhausted_total",
		Help:      "Total number of Lua merges that exhausted the configured revision-conflict attempts.",
	}
	mergeExhausted := prometheus.NewCounter(mergeExhaustedOptions)
	publishedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_publisher",
		Name:      "records_total",
		Help:      "Total number of Kafka mutation records by publish result.",
	}
	kafkaPublished := prometheus.NewCounterVec(publishedOptions, []string{"status"})
	publishDurationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "kafka_publisher",
		Name:      "duration_seconds",
		Help:      "Duration of synchronous Kafka publish batches in seconds.",
		Buckets:   prometheus.DefBuckets,
	}
	kafkaPublishDuration := prometheus.NewHistogram(publishDurationOptions)
	workerOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "mutations_total",
		Help:      "Total number of Kafka mutations by processing result.",
	}
	kafkaWorkerMutations := prometheus.NewCounterVec(workerOptions, []string{"status"})
	retryOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "retries_total",
		Help:      "Total number of Kafka mutation retry attempts.",
	}
	kafkaWorkerRetries := prometheus.NewCounter(retryOptions)
	deadLetterOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "dead_letters_total",
		Help:      "Total number of Kafka mutations published to the dead-letter topic.",
	}
	kafkaWorkerDeadLetters := prometheus.NewCounter(deadLetterOptions)
	buildOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "Sink build information.",
	}
	buildInfo := prometheus.NewGaugeVec(buildOptions, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)

	offsetGapOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_offset_gap", Help: "A committed source offset fell outside retention; explicit recovery is required."}
	workerOffsetGap := prometheus.NewGaugeVec(offsetGapOptions, []string{"store"})
	quarantineOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_quarantined_total", Help: "Records durably quarantined by configured store."}
	workerQuarantined := prometheus.NewCounterVec(quarantineOptions, []string{"store"})
	registry := prometheus.NewRegistry()
	registry.MustRegister(workerQuarantined, workerOffsetGap)
	admissionRequestsOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "in_flight_requests", Help: "Core requests currently executing, including cross-store and asynchronous requests."}
	admissionRequests := prometheus.NewGauge(admissionRequestsOptions)
	admissionBytesOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "in_flight_bytes", Help: "Request and output bytes reserved by executing core requests."}
	admissionBytes := prometheus.NewGauge(admissionBytesOptions)
	admissionRejectedOptions := prometheus.CounterOpts{Namespace: namespace, Name: "admission_rejected_total", Help: "Requests rejected by global or configured-store execution limits."}
	admissionRejected := prometheus.NewCounter(admissionRejectedOptions)
	lastPollOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_last_poll_timestamp_seconds", Help: "Last completed Kafka poll by configured store."}
	workerLastPoll := prometheus.NewGaugeVec(lastPollOptions, []string{"store"})
	lastCommitOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_last_commit_timestamp_seconds", Help: "Last successful source offset commit by configured store."}
	workerLastCommit := prometheus.NewGaugeVec(lastCommitOptions, []string{"store"})
	oldestOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_oldest_pending_timestamp_seconds", Help: "Oldest timestamp in the current uncommitted fetch, zero after commit; use Kafka lag monitoring for unpolled backlog."}
	workerOldest := prometheus.NewGaugeVec(oldestOptions, []string{"store"})
	pendingOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_pending_records", Help: "Records in the current uncommitted fetch, including records retained for retry."}
	workerPending := prometheus.NewGaugeVec(pendingOptions, []string{"store"})
	recoveryOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_recoveries_total", Help: "Batches retained for retry without advancing source offsets."}
	workerRecoveries := prometheus.NewCounterVec(recoveryOptions, []string{"store"})
	fetchErrorOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_fetch_errors_total", Help: "Kafka fetch errors by configured store, including offset retention gaps."}
	workerFetchErrors := prometheus.NewCounterVec(fetchErrorOptions, []string{"store"})
	deliveryOptions := prometheus.HistogramOpts{Namespace: namespace, Name: "kafka_worker_delivery_seconds", Help: "Age of the oldest record when a batch is resolved and its source offsets commit.", Buckets: []float64{0.1, 1, 5, 10, 30, 60, 300, 1800, 3600, 86400}}
	workerDelivery := prometheus.NewHistogramVec(deliveryOptions, []string{"store"})
	processOptions := collectors.ProcessCollectorOpts{}
	registeredCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(processOptions),
		buildInfo,
		requests,
		requestDuration,
		operationResults,
		batcherBatches,
		batcherOperations,
		batcherBytes,
		batcherQueueDuration,
		batcherExecution,
		batcherQueuedOps,
		batcherQueuedBytes,
		batcherRejected,
		mergeConflicts,
		mergeExhausted,
		kafkaPublished,
		kafkaPublishDuration,
		kafkaWorkerMutations,
		kafkaWorkerRetries,
		kafkaWorkerDeadLetters,
		admissionRequests,
		admissionBytes,
		admissionRejected,
		workerLastPoll, workerLastCommit, workerOldest, workerPending, workerRecoveries, workerFetchErrors, workerDelivery,
	}
	for _, collector := range registeredCollectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Prometheus collector: %w", err)
		}
	}
	metrics := &Metrics{
		workerOffsetGap:        workerOffsetGap,
		workerQuarantined:      workerQuarantined,
		registry:               registry,
		requests:               requests,
		requestDuration:        requestDuration,
		operationResults:       operationResults,
		batcherBatches:         batcherBatches,
		batcherOperations:      batcherOperations,
		batcherBytes:           batcherBytes,
		batcherQueueDuration:   batcherQueueDuration,
		batcherExecution:       batcherExecution,
		batcherQueuedOps:       batcherQueuedOps,
		batcherQueuedBytes:     batcherQueuedBytes,
		batcherRejected:        batcherRejected,
		mergeConflicts:         mergeConflicts,
		mergeExhausted:         mergeExhausted,
		kafkaPublished:         kafkaPublished,
		kafkaPublishDuration:   kafkaPublishDuration,
		kafkaWorkerMutations:   kafkaWorkerMutations,
		kafkaWorkerRetries:     kafkaWorkerRetries,
		kafkaWorkerDeadLetters: kafkaWorkerDeadLetters,
		admissionRequests:      admissionRequests,
		admissionBytes:         admissionBytes,
		admissionRejected:      admissionRejected,
		workerLastPoll:         workerLastPoll, workerLastCommit: workerLastCommit, workerOldest: workerOldest,
		workerPending: workerPending, workerRecoveries: workerRecoveries, workerFetchErrors: workerFetchErrors, workerDelivery: workerDelivery,
	}
	return metrics, nil
}

func (m *Metrics) AdjustAdmission(requests int, bytes int) {
	if m == nil {
		return
	}
	m.admissionRequests.Add(float64(requests))
	m.admissionBytes.Add(float64(bytes))
}

func (m *Metrics) ObserveAdmissionRejected() {
	if m == nil {
		return
	}
	m.admissionRejected.Inc()
}

func (m *Metrics) ObserveWorkerPoll(store string, errors int) {
	if m == nil {
		return
	}
	m.workerLastPoll.WithLabelValues(store).SetToCurrentTime()
	m.workerFetchErrors.WithLabelValues(store).Add(float64(errors))
}

func (m *Metrics) SetWorkerPending(store string, oldest time.Time, count int) {
	if m == nil {
		return
	}
	timestamp := float64(0)
	if !oldest.IsZero() {
		timestamp = float64(oldest.UnixMilli()) / 1000
	}
	m.workerOldest.WithLabelValues(store).Set(timestamp)
	m.workerPending.WithLabelValues(store).Set(float64(count))
}

func (m *Metrics) ObserveWorkerRecovery(store string) {
	if m == nil {
		return
	}
	m.workerRecoveries.WithLabelValues(store).Inc()
}

func (m *Metrics) ObserveWorkerCommitted(store string, oldest time.Time) {
	if m == nil {
		return
	}
	m.workerLastCommit.WithLabelValues(store).SetToCurrentTime()
	if !oldest.IsZero() {
		m.workerDelivery.WithLabelValues(store).Observe(max(0, time.Since(oldest).Seconds()))
	}
	var cleared time.Time
	m.SetWorkerPending(store, cleared, 0)
}

func (m *Metrics) ObserveMergeConflict(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mergeConflicts.Add(float64(count))
}

func (m *Metrics) ObserveMergeExhausted(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mergeExhausted.Add(float64(count))
}

func (m *Metrics) AdjustBatchQueue(method string, operations int, bytes int) {
	if m == nil {
		return
	}
	m.batcherQueuedOps.WithLabelValues(method).Add(float64(operations))
	m.batcherQueuedBytes.WithLabelValues(method).Add(float64(bytes))
}

func (m *Metrics) ObserveBatch(observation BatchObservation) {
	if m == nil {
		return
	}
	m.batcherBatches.WithLabelValues(observation.Method, observation.Reason).Inc()
	m.batcherOperations.WithLabelValues(observation.Method).Observe(float64(observation.Operations))
	m.batcherBytes.WithLabelValues(observation.Method).Observe(float64(observation.Bytes))
	m.batcherQueueDuration.WithLabelValues(observation.Method).Observe(observation.QueueDuration.Seconds())
	m.batcherExecution.WithLabelValues(observation.Method).Observe(observation.ExecutionDuration.Seconds())
}

func (m *Metrics) ObserveBatchRejected(method string, reason string) {
	if m == nil {
		return
	}
	m.batcherRejected.WithLabelValues(method, reason).Inc()
}

func (m *Metrics) ObserveKafkaPublish(duration time.Duration, accepted int, failed int) {
	if m == nil {
		return
	}
	m.kafkaPublishDuration.Observe(duration.Seconds())
	if accepted > 0 {
		m.kafkaPublished.WithLabelValues("accepted").Add(float64(accepted))
	}
	if failed > 0 {
		m.kafkaPublished.WithLabelValues("failed").Add(float64(failed))
	}
}

func (m *Metrics) ObserveKafkaWorker(status string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerMutations.WithLabelValues(status).Add(float64(count))
}

func (m *Metrics) ObserveKafkaRetry(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerRetries.Add(float64(count))
}

func (m *Metrics) ObserveKafkaDeadLetter(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerDeadLetters.Add(float64(count))
}

func (m *Metrics) Handler() http.Handler {
	handlerOptions := promhttp.HandlerOpts{EnableOpenMetrics: true}
	return promhttp.HandlerFor(m.registry, handlerOptions)
}

func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	interceptor := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		method, observed := sinkMethod(info.FullMethod)
		if !observed {
			return handler(ctx, req)
		}

		started := time.Now()
		response, err := handler(ctx, req)
		code := status.Code(err).String()
		m.requests.WithLabelValues(method, code).Inc()
		m.requestDuration.WithLabelValues(method).Observe(time.Since(started).Seconds())
		if err == nil {
			m.observeOperationResults(method, response)
		}
		return response, err
	}
	return interceptor
}

func sinkMethod(fullMethod string) (string, bool) {
	switch fullMethod {
	case sink.Sink_Read_FullMethodName:
		return "Read", true
	case sink.Sink_Write_FullMethodName:
		return "Write", true
	case sink.Sink_Delete_FullMethodName:
		return "Delete", true
	default:
		return "", false
	}
}

func (m *Metrics) observeOperationResults(method string, response any) {
	switch typed := response.(type) {
	case *sink.ReadResponse:
		for _, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(method, readStatus(result.GetStatus())).Inc()
		}
	case *sink.WriteResponse:
		for _, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(method, writeStatus(result.GetStatus())).Inc()
		}
	case *sink.DeleteResponse:
		for _, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(method, deleteStatus(result.GetStatus())).Inc()
		}
	}
}

func readStatus(value sink.ReadStatus) string {
	switch value {
	case sink.ReadStatus_READ_STATUS_FOUND:
		return "found"
	case sink.ReadStatus_READ_STATUS_NOT_FOUND:
		return "not_found"
	case sink.ReadStatus_READ_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func writeStatus(value sink.WriteStatus) string {
	switch value {
	case sink.WriteStatus_WRITE_STATUS_APPLIED:
		return "applied"
	case sink.WriteStatus_WRITE_STATUS_ACCEPTED:
		return "accepted"
	case sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED:
		return "precondition_failed"
	case sink.WriteStatus_WRITE_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func deleteStatus(value sink.DeleteStatus) string {
	switch value {
	case sink.DeleteStatus_DELETE_STATUS_APPLIED:
		return "applied"
	case sink.DeleteStatus_DELETE_STATUS_ACCEPTED:
		return "accepted"
	case sink.DeleteStatus_DELETE_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func (m *Metrics) ObserveQuarantined(store string) {
	if m != nil {
		m.workerQuarantined.WithLabelValues(store).Inc()
	}
}

func (m *Metrics) SetWorkerOffsetGap(store string, gap bool) {
	if m == nil {
		return
	}
	value := 0.0
	if gap {
		value = 1
	}
	m.workerOffsetGap.WithLabelValues(store).Set(value)
}
