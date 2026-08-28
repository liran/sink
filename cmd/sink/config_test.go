package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaultsToSynchronousServer(t *testing.T) {
	path := writeConfig(t, `
storage:
  mongodb:
    uri: mongodb://mongodb:27017
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeServer || loaded.grpcAddress != ":8080" || loaded.storageDriver != driverMongoDB || loaded.mongoStore != "primary" {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if loaded.maxOperations != 1000 || loaded.maxMergeAttempts != 3 || loaded.shutdownTimeout != 15*time.Second {
		t.Fatalf("loadConfig() service defaults = %#v", loaded)
	}
	if loaded.kafkaMaxPollRecords != 500 || len(loaded.kafkaBrokers) != 0 || loaded.kafkaTopic != "" {
		t.Fatalf("loadConfig() Kafka configuration = %#v", loaded)
	}
}

func TestLoadConfigElasticsearchStorage(t *testing.T) {
	path := writeConfig(t, `
storage:
  driver: elasticsearch
  search:
    endpoints:
      - http://search-1:9200
      - http://search-2:9200
    api_key: test-api-key
    bindings:
      - namespace: logical
        dataset: records
        index: legacy-records
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.storageDriver != driverElasticsearch || len(loaded.searchEndpoints) != 2 || loaded.searchAPIKey != "test-api-key" {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if len(loaded.searchBindings) != 1 || loaded.searchStore != "primary" {
		t.Fatalf("loadConfig() search bindings = %#v", loaded.searchBindings)
	}
}

func TestLoadConfigOpenSearchBasicAuthentication(t *testing.T) {
	path := writeConfig(t, `
storage:
  driver: opensearch
  search:
    endpoints:
      - https://search:9200
    username: sink
    password: test-password
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.storageDriver != driverOpenSearch || loaded.searchUsername != "sink" || loaded.searchPassword != "test-password" {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
}

func TestLoadConfigRejectsConflictingSearchAuthentication(t *testing.T) {
	path := writeConfig(t, `
storage:
  driver: elasticsearch
  search:
    endpoints: [http://search:9200]
    username: sink
    password: test-password
    api_key: test-api-key
`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatal("loadConfig() error = nil")
	}
}

func TestLoadConfigWorkerAndBindings(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storage:
  mongodb:
    uri: mongodb://mongodb:27017
    bindings:
      - namespace: logical
        dataset: records
        database: legacy
        collection: documents
kafka:
  brokers:
    - kafka-1:9092
    - kafka-2:9092
  topic: sink-mutations
  group_id: sink-workers
  max_poll_records: 250
service:
  max_operations: 2000
  max_merge_attempts: 5
shutdown_timeout_seconds: 30
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeWorker || len(loaded.kafkaBrokers) != 2 || loaded.kafkaMaxPollRecords != 250 {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if len(loaded.mongoBindings) != 1 || loaded.maxOperations != 2000 || loaded.maxMergeAttempts != 5 {
		t.Fatalf("loadConfig() bindings and service = %#v", loaded)
	}
	if loaded.shutdownTimeout != 30*time.Second {
		t.Fatalf("loadConfig() shutdown timeout = %v", loaded.shutdownTimeout)
	}
}

func TestLoadConfigRejectsPartialKafkaConfiguration(t *testing.T) {
	path := writeConfig(t, `
storage:
  mongodb:
    uri: mongodb://mongodb:27017
kafka:
  brokers: [kafka:9092]
`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatal("loadConfig() error = nil")
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `
storage:
  mongodb:
    uri: mongodb://mongodb:27017
unexpected: true
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsNonPositiveValues(t *testing.T) {
	path := writeConfig(t, `
storage:
  mongodb:
    uri: mongodb://mongodb:27017
service:
  max_operations: 0
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "service.max_operations must be a positive integer") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigDoesNotReadLegacyEnvironmentVariables(t *testing.T) {
	t.Setenv("SINK_MODE", "worker")
	t.Setenv("SINK_MONGODB_URI", "mongodb://legacy-environment:27017")
	path := writeConfig(t, `
mode: server
storage:
  mongodb:
    uri: mongodb://configured:27017
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeServer || loaded.mongoURI != "mongodb://configured:27017" {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
}

func TestExampleConfigurationFilesLoad(t *testing.T) {
	paths := []string{
		"../../config.example.yaml",
		"../../examples/quickstart/sink.yaml",
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			_, err := loadConfig(path)
			if err != nil {
				t.Fatalf("loadConfig(%q) error = %v", path, err)
			}
		})
	}
}

func TestParseConfigPath(t *testing.T) {
	args := []string{"--config", "/etc/sink/config.yaml"}
	path, err := parseConfigPath(args)
	if err != nil {
		t.Fatalf("parseConfigPath() error = %v", err)
	}
	if path != "/etc/sink/config.yaml" {
		t.Fatalf("parseConfigPath() = %q", path)
	}
}

func TestParseConfigPathRequiresFlag(t *testing.T) {
	_, err := parseConfigPath(nil)
	if err == nil || err.Error() != "--config is required" {
		t.Fatalf("parseConfigPath() error = %v", err)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
