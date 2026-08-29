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
	if loaded.prometheusAddress != "" {
		t.Fatalf("loadConfig() Prometheus address = %q", loaded.prometheusAddress)
	}
	configured := loaded.storages[0]
	if configured.name != "primary" || configured.driver != driverMongoDB || configured.mongoURI != "mongodb://mongodb:27017" {
		t.Fatalf("loadConfig() storage = %#v", configured)
	}
	if loaded.maxOperations != 1000 || loaded.maxMergeAttempts != 3 || loaded.shutdownTimeout != 15*time.Second {
		t.Fatalf("loadConfig() service defaults = %#v", loaded)
	}
	if !loaded.batchingEnabled || loaded.batchingMaxWait != 2*time.Millisecond ||
		loaded.batchingMaxOperations != 1000 || loaded.batchingMaxBytes != 16<<20 ||
		loaded.batchingMaxQueuedOps != 10_000 || loaded.batchingMaxQueuedBytes != 128<<20 {
		t.Fatalf("loadConfig() batching defaults = %#v", loaded)
	}
	if loaded.luaOptions.Timeout != 100*time.Millisecond || loaded.luaOptions.MaxSourceBytes != 64<<10 ||
		loaded.luaOptions.MaxResultBytes != 16<<20 || loaded.luaOptions.MaxCachedPrograms != 256 ||
		loaded.luaOptions.MaxInstructions != 1_000_000 {
		t.Fatalf("loadConfig() Lua defaults = %#v", loaded.luaOptions)
	}
	if loaded.grpcMaxReceiveBytes != 64<<20 || loaded.grpcMaxSendBytes != 64<<20 {
		t.Fatalf("loadConfig() gRPC limits = %#v", loaded)
	}
	if configured.kafka.configured {
		t.Fatalf("loadConfig() Kafka configuration = %#v", configured.kafka)
	}
}

func TestLoadConfigBatchingSettings(t *testing.T) {
	path := writeConfig(t, `
grpc:
  max_receive_message_bytes: 1048576
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
service:
  max_operations: 2000
  batching:
    enabled: false
    max_wait_milliseconds: 5
    max_operations: 500
    max_bytes: 524288
    max_queued_operations: 2500
    max_queued_bytes: 2097152
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.batchingEnabled || loaded.batchingMaxWait != 5*time.Millisecond ||
		loaded.batchingMaxOperations != 500 || loaded.batchingMaxBytes != 524288 ||
		loaded.batchingMaxQueuedOps != 2500 || loaded.batchingMaxQueuedBytes != 2097152 {
		t.Fatalf("loadConfig() batching settings = %#v", loaded)
	}
}

func TestLoadConfigRejectsUnsafeBatchingLimits(t *testing.T) {
	tests := []struct {
		name      string
		batching  string
		wantError string
	}{
		{
			name:      "batch exceeds service operation limit",
			batching:  "max_operations: 1001",
			wantError: "service.batching.max_operations cannot exceed service.max_operations",
		},
		{
			name:      "queue cannot hold one request",
			batching:  "max_queued_operations: 999",
			wantError: "service.batching.max_queued_operations must cover one server request and one batch",
		},
		{
			name:      "byte queue cannot hold one gRPC message",
			batching:  "max_queued_bytes: 1048576",
			wantError: "service.batching.max_queued_bytes must cover one gRPC request and one batch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
service:
  batching:
    ` + test.batching + "\n"
			path := writeConfig(t, contents)
			_, err := loadConfig(path)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("loadConfig() error = %v", err)
			}
		})
	}
}

func TestLoadConfigMultipleStorages(t *testing.T) {
	path := writeConfig(t, `
prometheus:
  address: ":9090"
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
	if loaded.prometheusAddress != ":9090" {
		t.Fatalf("loadConfig() Prometheus address = %q", loaded.prometheusAddress)
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
      dead_letter_topic: sink-dead-letters
      max_retry_attempts: 4
      retry_backoff_milliseconds: 20
      max_retry_backoff_milliseconds: 200
service:
  max_operations: 2000
  max_merge_attempts: 5
  lua:
    timeout_milliseconds: 250
    max_source_bytes: 32768
    max_result_bytes: 1048576
    max_cached_programs: 128
    max_instructions: 2000000
shutdown_timeout_seconds: 30
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	configured := loaded.storages[0]
	if loaded.mode != modeWorker || len(configured.kafka.brokers) != 2 || configured.kafka.maxPollRecords != 250 {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if configured.kafka.deadLetterTopic != "sink-dead-letters" || configured.kafka.maxRetryAttempts != 4 || configured.kafka.retryBackoff != 20*time.Millisecond || configured.kafka.maxRetryBackoff != 200*time.Millisecond {
		t.Fatalf("loadConfig() Kafka retry settings = %#v", configured.kafka)
	}
	if loaded.maxOperations != 2000 || loaded.maxMergeAttempts != 5 || loaded.shutdownTimeout != 30*time.Second {
		t.Fatalf("loadConfig() service settings = %#v", loaded)
	}
	if loaded.luaOptions.Timeout != 250*time.Millisecond || loaded.luaOptions.MaxSourceBytes != 32768 ||
		loaded.luaOptions.MaxResultBytes != 1048576 || loaded.luaOptions.MaxCachedPrograms != 128 ||
		loaded.luaOptions.MaxInstructions != 2_000_000 {
		t.Fatalf("loadConfig() Lua settings = %#v", loaded.luaOptions)
	}
}

func TestLoadConfigSupportsIndependentStoreKafkaClusters(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: catalog
    driver: mongodb
    mongodb:
      uri: mongodb://catalog:27017
    kafka:
      brokers: [catalog-kafka:9092]
      topic: mutations
      group_id: workers
  - name: search
    driver: opensearch
    search:
      endpoints: [https://search:9200]
    kafka:
      brokers: [search-kafka:9092]
      topic: mutations
      group_id: workers
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if len(loaded.storages) != 2 {
		t.Fatalf("loadConfig() storages = %#v", loaded.storages)
	}
	for _, configured := range loaded.storages {
		if !configured.kafka.configured || configured.kafka.topic != "mutations" || configured.kafka.groupID != "workers" {
			t.Fatalf("loadConfig() storage Kafka = %#v", configured.kafka)
		}
	}
}

func TestLoadConfigRejectsDuplicateKafkaResourcesOnSameCluster(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: first
    driver: mongodb
    mongodb:
      uri: mongodb://first:27017
    kafka:
      brokers: [kafka-1:9092, kafka-2:9092]
      topic: shared-mutations
      group_id: first-workers
  - name: second
    driver: mongodb
    mongodb:
      uri: mongodb://second:27017
    kafka:
      brokers: [kafka-2:9092, kafka-1:9092]
      topic: shared-mutations
      group_id: second-workers
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "duplicate Kafka topic") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigServerAllowsStoreKafkaWithoutConsumerGroup(t *testing.T) {
	path := writeConfig(t, `
mode: server
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
    kafka:
      brokers: [kafka:9092]
      topic: sink-mutations
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	configured := loaded.storages[0].kafka
	if configured.groupID != "" || configured.deadLetterTopic != "sink-mutations.dlq" {
		t.Fatalf("loadConfig() Kafka = %#v", configured)
	}
}

func TestLoadConfigRejectsNonPositiveLuaLimits(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
service:
  lua:
    max_source_bytes: 0
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "service.lua.max_source_bytes") {
		t.Fatalf("loadConfig() error = %v", err)
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
	if err == nil || !strings.Contains(err.Error(), "storages[0].kafka") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigWorkerRequiresGroupForEveryKafkaStore(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
    kafka:
      brokers: [kafka:9092]
      topic: sink-mutations
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "storages[0].kafka.group_id") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsTopLevelKafkaConfiguration(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
kafka:
  brokers: [kafka:9092]
  topic: sink-mutations
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field kafka not found") {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigWorkerRequiresAtLeastOneKafkaStore(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
`)
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "worker and all modes require Kafka settings") {
		t.Fatalf("loadConfig() error = %v", err)
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

func TestLoadConfigDefaultsWorkerDeadLetterTopic(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongodb:27017
    kafka:
      brokers: [kafka:9092]
      topic: sink-mutations
      group_id: sink-workers
`)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	configured := loaded.storages[0].kafka
	if configured.deadLetterTopic != "sink-mutations.dlq" || configured.maxPollRecords != 500 ||
		configured.maxRetryAttempts != 10 || configured.retryBackoff != 100*time.Millisecond ||
		configured.maxRetryBackoff != 10*time.Second {
		t.Fatalf("Kafka defaults = %#v", configured)
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
