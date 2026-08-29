package main

import (
	"context"
	"errors"
	"testing"

	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type healthStorage struct {
	pingErr error
}

func (s *healthStorage) Ping(context.Context) error {
	return s.pingErr
}

func (s *healthStorage) Read(_ context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	response := storage.ReadResponse{Results: make([]storage.ReadResult, len(req.Operations))}
	return response, nil
}

func (s *healthStorage) Write(_ context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	return response, nil
}

func (s *healthStorage) Delete(_ context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{Results: make([]storage.DeleteResult, len(req.Operations))}
	return response, nil
}

func TestUpdateHealthIsolatesStorageFailure(t *testing.T) {
	failedStore := &healthStorage{pingErr: errors.New("storage unavailable")}
	healthyStore := &healthStorage{}
	failedCheck := configuredHealthCheck{service: storageHealthService("failed"), pinger: failedStore}
	healthyCheck := configuredHealthCheck{service: storageHealthService("healthy"), pinger: healthyStore}
	healthChecks := []configuredHealthCheck{failedCheck, healthyCheck}
	app := &application{health: health.NewServer(), healthChecks: healthChecks}
	app.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	app.health.SetServingStatus(failedCheck.service, healthpb.HealthCheckResponse_SERVING)
	app.health.SetServingStatus(healthyCheck.service, healthpb.HealthCheckResponse_SERVING)

	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, "", healthpb.HealthCheckResponse_SERVING)
	assertHealthStatus(t, app.health, failedCheck.service, healthpb.HealthCheckResponse_NOT_SERVING)
	assertHealthStatus(t, app.health, healthyCheck.service, healthpb.HealthCheckResponse_SERVING)

	failedStore.pingErr = nil
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, "", healthpb.HealthCheckResponse_SERVING)
	assertHealthStatus(t, app.health, failedCheck.service, healthpb.HealthCheckResponse_SERVING)
	assertHealthStatus(t, app.health, healthyCheck.service, healthpb.HealthCheckResponse_SERVING)
}

func TestCloseMarksEveryHealthServiceNotServing(t *testing.T) {
	store := &healthStorage{}
	healthCheck := configuredHealthCheck{service: kafkaHealthService("primary"), pinger: store}
	app := &application{health: health.NewServer(), healthChecks: []configuredHealthCheck{healthCheck}}
	app.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	app.health.SetServingStatus(healthCheck.service, healthpb.HealthCheckResponse_SERVING)

	app.close()

	assertHealthStatus(t, app.health, "", healthpb.HealthCheckResponse_NOT_SERVING)
	assertHealthStatus(t, app.health, healthCheck.service, healthpb.HealthCheckResponse_NOT_SERVING)
}

func assertHealthStatus(t *testing.T, server *health.Server, service string, wanted healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	request := &healthpb.HealthCheckRequest{Service: service}
	response, err := server.Check(t.Context(), request)
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if response.GetStatus() != wanted {
		t.Fatalf("health status = %s, want %s", response.GetStatus(), wanted)
	}
}
