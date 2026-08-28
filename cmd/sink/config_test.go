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
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeServer || loaded.grpcAddress != ":8080" || len(loaded.storages) != 1 {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	configured := loaded.storages[0]
	if configured.name != "primary" || configured.driver != driverMongoDB || configured.mongoURI != "mongodb://mongodb:27017" {
		t.Fatalf("loadConfig() storage = %#v", configured)
	}
	if loaded.maxOperations != 1000 || loaded.maxMergeAttempts != 3 || loaded.shutdownTimeout != 15*time.Second {
		t.Fatalf("loadConfig() service defaults = %#v", loaded)
	}
	if loaded.kafkaMaxPollRecords != 500 || len(loaded.kafkaBrokers) != 0 || loaded.kafkaTopic != "" {
		t.Fatalf("loadConfig() Kafka configuration = %#v", loaded)
	}
}

func TestLoadConfigMultipleStorages(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: mongo-main
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-main:27017
      metadata_field: __revision
  - name: mongo-archive
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-archive:27017
  - name: search-main
    driver: elasticsearch
    search:
      endpoints:
        - http://search-1:9200
        - http://search-2:9200
      api_key: test-api-key
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if len(loaded.storages) != 3 {
		t.Fatalf("loadConfig() storages = %#v", loaded.storages)
	}
	if loaded.storages[0].name != "mongo-main" || loaded.storages[0].mongoMetadataField != "__revision" {
		t.Fatalf("loadConfig() first storage = %#v", loaded.storages[0])
	}
	search := loaded.storages[2]
	if search.driver != driverElasticsearch || len(search.searchEndpoints) != 2 || search.searchAPIKey != "test-api-key" {
		t.Fatalf("loadConfig() search storage = %#v", search)
	}
}

func TestLoadConfigOpenSearchBasicAuthentication(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: search-main
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
	configured := loaded.storages[0]
	if configured.driver != driverOpenSearch || configured.searchUsername != "sink" || configured.searchPassword != "test-password" {
		t.Fatalf("loadConfig() storage = %#v", configured)
	}
}

func TestLoadConfigRejectsConflictingSearchAuthentication(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: search-main
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

func TestLoadConfigWorkerSettings(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
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
	if loaded.maxOperations != 2000 || loaded.maxMergeAttempts != 5 || loaded.shutdownTimeout != 30*time.Second {
		t.Fatalf("loadConfig() service settings = %#v", loaded)
	}
}

func TestLoadConfigRejectsDuplicateStorageNames(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-1:27017
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-2:27017
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), `duplicate name "primary"`) {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRequiresAtLeastOneStorage(t *testing.T) {
	path := writeConfig(t, `
storages: []
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "storages must contain at least one storage") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRequiresStorageNameAndDriver(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: ""
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "storages[0].name is required") {
		t.Fatalf("loadConfig() error = %v", err)
	}

	path = writeConfig(t, `
storages:
  - name: primary
    mongodb:
      uri: mongodb://mongodb:27017
`)
	_, err = loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "storages[0].driver must be") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsBindings(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
      bindings: []
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field bindings not found") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsPartialKafkaConfiguration(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
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
storages:
  - name: primary
    driver: mongodb
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
storages:
  - name: primary
    driver: mongodb
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
storages:
  - name: configured
    driver: mongodb
    mongodb:
      uri: mongodb://configured:27017
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeServer || loaded.storages[0].mongoURI != "mongodb://configured:27017" {
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
