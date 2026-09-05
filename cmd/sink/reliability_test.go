package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	searchstorage "github.com/liran/sink/internal/storage/search"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestUnavailableStoreDoesNotBlockStartup(t *testing.T) {
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer unavailable.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer healthy.Close()
	failedConfig := backendConfig{name: "failed", driver: driverOpenSearch, searchDriver: searchstorage.DriverOpenSearch, searchEndpoints: []string{unavailable.URL}}
	healthyConfig := backendConfig{name: "healthy", driver: driverOpenSearch, searchDriver: searchstorage.DriverOpenSearch, searchEndpoints: []string{healthy.URL}}
	loaded := config{mode: modeServer, grpcAddress: "127.0.0.1:0", grpcMaxReceiveBytes: 64 << 20, grpcMaxSendBytes: 64 << 20,
		storages: []backendConfig{failedConfig, healthyConfig}, shutdownTimeout: time.Second}
	app, err := newApplication(t.Context(), loaded)
	if err != nil {
		t.Fatalf("healthy store could not start alongside outage: %v", err)
	}
	defer app.close()
	app.health = health.NewServer()
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, storageHealthService("failed"), healthpb.HealthCheckResponse_NOT_SERVING)
	assertHealthStatus(t, app.health, storageHealthService("healthy"), healthpb.HealthCheckResponse_SERVING)
	request := httptest.NewRequest(http.MethodGet, "/readyz?service=sink.storage.healthy", nil)
	response := httptest.NewRecorder()
	app.serveReadiness(response, request)
	if response.Code != http.StatusOK {
		t.Fatal("healthy store readiness was coupled to failed store")
	}
}

func TestMongoClientStartsWithoutRequiringAvailability(t *testing.T) {
	configured := backendConfig{name: "primary", driver: driverMongoDB, mongoURI: "mongodb://127.0.0.1:1/?w=1&journal=false"}
	opened, err := openMongoStorage(t.Context(), configured, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.mongoClient.Disconnect(t.Context())

}

func TestReliabilityConfigurationRejectsIncompatibleLimits(t *testing.T) {
	readBytes := 1024
	file := serviceConfigFile{MaxReadBytes: &readBytes}
	loaded := config{grpcMaxSendBytes: 1024}
	if err := loaded.loadReliabilityConfig(file); err == nil {
		t.Fatal("read budget exceeded transport budget")
	}
	minISR := 3
	kafkaFile := kafkaConfigFile{MinInSyncReplicas: &minISR}
	kafka := backendKafkaConfig{topicReplicationFactor: 2}
	if err := kafka.loadReliabilityConfig("kafka", kafkaFile); err == nil {
		t.Fatal("min ISR exceeded replica factor")
	}
}
