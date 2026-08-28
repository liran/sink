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
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	operationResults *prometheus.CounterVec
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
	buildOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "Sink build information.",
	}
	buildInfo := prometheus.NewGaugeVec(buildOptions, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)

	registry := prometheus.NewRegistry()
	processOptions := collectors.ProcessCollectorOpts{}
	registeredCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(processOptions),
		buildInfo,
		requests,
		requestDuration,
		operationResults,
	}
	for _, collector := range registeredCollectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Prometheus collector: %w", err)
		}
	}
	metrics := &Metrics{
		registry:         registry,
		requests:         requests,
		requestDuration:  requestDuration,
		operationResults: operationResults,
	}
	return metrics, nil
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
