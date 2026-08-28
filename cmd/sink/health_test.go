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

func TestUpdateHealthTracksStorageRecovery(t *testing.T) {
	store := &healthStorage{pingErr: errors.New("storage unavailable")}
	app := &application{storage: store, health: health.NewServer()}
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, healthpb.HealthCheckResponse_NOT_SERVING)

	store.pingErr = nil
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, healthpb.HealthCheckResponse_SERVING)
}

func assertHealthStatus(t *testing.T, server *health.Server, wanted healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	request := &healthpb.HealthCheckRequest{}
	response, err := server.Check(t.Context(), request)
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if response.GetStatus() != wanted {
		t.Fatalf("health status = %s, want %s", response.GetStatus(), wanted)
	}
}
