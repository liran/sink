package metrics_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricsExposeBuildRequestAndOperationResults(t *testing.T) {
	observed, err := sinkmetrics.New("test-version")
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	request := &sink.ReadRequest{
		Operations: []*sink.ReadOperation{{}, {}},
	}
	info := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Read_FullMethodName}
	handler := func(context.Context, any) (any, error) {
		response := &sink.ReadResponse{
			Results: []*sink.ReadResult{
				{Status: sink.ReadStatus_READ_STATUS_FOUND},
				{Status: sink.ReadStatus_READ_STATUS_NOT_FOUND},
			},
		}
		return response, nil
	}
	interceptor := observed.UnaryServerInterceptor()
	_, err = interceptor(t.Context(), request, info, handler)
	if err != nil {
		t.Fatalf("interceptor() error = %v", err)
	}
	writeRequest := &sink.WriteRequest{}
	writeInfo := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Write_FullMethodName}
	writeHandler := func(context.Context, any) (any, error) {
		response := &sink.WriteResponse{
			Results: []*sink.WriteResult{
				{Status: sink.WriteStatus_WRITE_STATUS_APPLIED},
				{Status: sink.WriteStatus_WRITE_STATUS_ACCEPTED},
				{Status: sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED},
				{Status: sink.WriteStatus_WRITE_STATUS_FAILED},
			},
		}
		return response, nil
	}
	_, err = interceptor(t.Context(), writeRequest, writeInfo, writeHandler)
	if err != nil {
		t.Fatalf("interceptor(Write) error = %v", err)
	}
	deleteRequest := &sink.DeleteRequest{}
	deleteInfo := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Delete_FullMethodName}
	deleteHandler := func(context.Context, any) (any, error) {
		response := &sink.DeleteResponse{
			Results: []*sink.DeleteResult{
				{Status: sink.DeleteStatus_DELETE_STATUS_APPLIED},
				{Status: sink.DeleteStatus_DELETE_STATUS_ACCEPTED},
				{Status: sink.DeleteStatus_DELETE_STATUS_FAILED},
			},
		}
		return response, nil
	}
	_, err = interceptor(t.Context(), deleteRequest, deleteInfo, deleteHandler)
	if err != nil {
		t.Fatalf("interceptor(Delete) error = %v", err)
	}
	observed.ObserveKafkaPublish(time.Second, 2, 1)
	observed.ObserveKafkaWorker("applied", 2)
	observed.ObserveKafkaWorker("failed", 1)
	observed.ObserveKafkaRetry(3)
	observed.ObserveKafkaDeadLetter(1)
	observed.AdjustBatchQueue("Read", 7, 4096)
	observed.AdjustBatchQueue("Read", -7, -4096)
	batchObservation := sinkmetrics.BatchObservation{
		Method:            "Read",
		Reason:            "max_wait",
		Operations:        7,
		Bytes:             4096,
		QueueDuration:     time.Millisecond,
		ExecutionDuration: 2 * time.Millisecond,
	}
	observed.ObserveBatch(batchObservation)
	observed.ObserveBatchRejected("Read", "queue_full")

	body := scrape(t, observed)
	wanted := []string{
		`sink_build_info{version="test-version"} 1`,
		`sink_grpc_server_requests_total{code="OK",method="Read"} 1`,
		`sink_grpc_server_operation_results_total{method="Read",status="found"} 1`,
		`sink_grpc_server_operation_results_total{method="Read",status="not_found"} 1`,
		`sink_grpc_server_request_duration_seconds_count{method="Read"} 1`,
		`sink_grpc_server_requests_total{code="OK",method="Write"} 1`,
		`sink_grpc_server_operation_results_total{method="Write",status="applied"} 1`,
		`sink_grpc_server_operation_results_total{method="Write",status="accepted"} 1`,
		`sink_grpc_server_operation_results_total{method="Write",status="precondition_failed"} 1`,
		`sink_grpc_server_operation_results_total{method="Write",status="failed"} 1`,
		`sink_grpc_server_requests_total{code="OK",method="Delete"} 1`,
		`sink_grpc_server_operation_results_total{method="Delete",status="applied"} 1`,
		`sink_grpc_server_operation_results_total{method="Delete",status="accepted"} 1`,
		`sink_grpc_server_operation_results_total{method="Delete",status="failed"} 1`,
		`sink_kafka_publisher_records_total{status="accepted"} 2`,
		`sink_kafka_publisher_records_total{status="failed"} 1`,
		`sink_kafka_publisher_duration_seconds_count 1`,
		`sink_kafka_worker_mutations_total{status="applied"} 2`,
		`sink_kafka_worker_mutations_total{status="failed"} 1`,
		`sink_kafka_worker_retries_total 3`,
		`sink_kafka_worker_dead_letters_total 1`,
		`sink_batcher_batches_total{method="Read",reason="max_wait"} 1`,
		`sink_batcher_operations_count{method="Read"} 1`,
		`sink_batcher_bytes_count{method="Read"} 1`,
		`sink_batcher_queue_duration_seconds_count{method="Read"} 1`,
		`sink_batcher_execution_duration_seconds_count{method="Read"} 1`,
		`sink_batcher_queued_operations{method="Read"} 0`,
		`sink_batcher_queued_bytes{method="Read"} 0`,
		`sink_batcher_rejected_total{method="Read",reason="queue_full"} 1`,
	}
	for _, value := range wanted {
		if !strings.Contains(body, value) {
			t.Errorf("metrics body does not contain %q", value)
		}
	}
}

func TestMetricsRecordGRPCFailuresAndIgnoreOtherServices(t *testing.T) {
	observed, err := sinkmetrics.New("test-version")
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	interceptor := observed.UnaryServerInterceptor()
	request := &sink.WriteRequest{}
	info := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Write_FullMethodName}
	handler := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	_, err = interceptor(t.Context(), request, info, handler)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("interceptor() error = %v", err)
	}

	healthInfo := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	healthHandler := func(context.Context, any) (any, error) {
		return nil, errors.New("health failure")
	}
	_, _ = interceptor(t.Context(), nil, healthInfo, healthHandler)

	body := scrape(t, observed)
	if !strings.Contains(body, `sink_grpc_server_requests_total{code="InvalidArgument",method="Write"} 1`) {
		t.Fatal("metrics body does not contain the failed Write request")
	}
	if strings.Contains(body, "Health") {
		t.Fatal("metrics body contains a health service label")
	}
}

func scrape(t *testing.T, observed *sinkmetrics.Metrics) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	return recorder.Body.String()
}
