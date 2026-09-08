package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	searchstorage "github.com/liran/sink/internal/storage/search"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type stalledHealthProbe struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (p *stalledHealthProbe) Ping(context.Context) error {
	if p.calls.Add(1) == 1 {
		close(p.started)
	}
	<-p.release
	return nil
}

func TestReadinessHonorsDeadlineWhenDependencyIgnoresCancellation(t *testing.T) {
	probe := &stalledHealthProbe{started: make(chan struct{}), release: make(chan struct{})}
	check := &configuredHealthCheck{service: "blocked", pinger: probe}
	app := &application{healthChecks: []*configuredHealthCheck{check}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { app.serveReadiness(response, request); close(done) }()
	defer func() { close(probe.release); <-done }()
	<-probe.started
	select {
	case <-done:
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("stalled dependency readiness = %d", response.Code)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("readiness waited for dependency beyond request deadline")
	}
}

func TestReadinessSharesOutstandingProbeAndRecovers(t *testing.T) {
	probe := &stalledHealthProbe{started: make(chan struct{}), release: make(chan struct{})}
	check := &configuredHealthCheck{service: "blocked", pinger: probe}
	var callers sync.WaitGroup
	for range 32 {
		callers.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if err := check.check(ctx); err != context.DeadlineExceeded {
				t.Errorf("stalled probe result: %v", err)
			}
		})
	}
	callers.Wait()
	if calls := probe.calls.Load(); calls != 1 {
		t.Errorf("32 timed-out callers started %d probes, want one outstanding probe", calls)
	}
	check.mu.Lock()
	attempt := check.active
	check.mu.Unlock()
	close(probe.release)
	if attempt == nil {
		t.Fatal("stalled probe was discarded before its completion")
	}
	<-attempt.done
	if err := check.check(t.Context()); err != nil {
		t.Fatalf("readiness did not recover: %v", err)
	}
	if calls := probe.calls.Load(); calls != 2 {
		t.Fatalf("completed probe was cached: calls=%d", calls)
	}
}

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

func TestScanTimeoutConfiguration(t *testing.T) {
	for _, seconds := range []int{0, 1, 900, 3600, 3601, -1} {
		file := serviceConfigFile{ScanTimeoutSeconds: &seconds}
		loaded := config{grpcMaxSendBytes: 64 << 20}
		err := loaded.loadReliabilityConfig(file)
		valid := seconds > 0 && seconds <= 3600
		if (err == nil) != valid {
			t.Fatalf("scan timeout %d: %v", seconds, err)
		}
		if valid && loaded.scanTimeout != time.Duration(seconds)*time.Second {
			t.Fatalf("scan timeout=%s", loaded.scanTimeout)
		}
	}
	file := serviceConfigFile{}
	loaded := config{grpcMaxSendBytes: 64 << 20}
	if err := loaded.loadReliabilityConfig(file); err != nil || loaded.scanTimeout != 15*time.Minute {
		t.Fatalf("default scan timeout=%s err=%v", loaded.scanTimeout, err)
	}
}
