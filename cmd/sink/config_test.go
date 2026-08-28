package main

import (
	"testing"
)

var configEnvironment = []string{
	"SINK_MODE",
	"SINK_GRPC_ADDRESS",
	"SINK_MONGODB_URI",
	"SINK_MONGODB_STORE",
	"SINK_MONGODB_HIDDEN_FIELD",
	"SINK_MONGODB_BINDINGS",
	"SINK_MAX_OPERATIONS",
	"SINK_MAX_MERGE_ATTEMPTS",
	"SINK_KAFKA_BROKERS",
	"SINK_KAFKA_TOPIC",
	"SINK_KAFKA_GROUP_ID",
	"SINK_KAFKA_MAX_POLL_RECORDS",
	"SINK_SHUTDOWN_TIMEOUT_SECONDS",
}

func TestLoadConfigDefaultsToSynchronousServer(t *testing.T) {
	resetConfigEnvironment(t)
	t.Setenv("SINK_MONGODB_URI", "mongodb://mongodb:27017")
	loaded, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeServer || loaded.grpcAddress != ":8080" || loaded.mongoStore != "primary" {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if len(loaded.kafkaBrokers) != 0 || loaded.kafkaTopic != "" {
		t.Fatalf("loadConfig() Kafka configuration = %#v", loaded)
	}
}

func TestLoadConfigWorkerAndBindings(t *testing.T) {
	resetConfigEnvironment(t)
	t.Setenv("SINK_MODE", "worker")
	t.Setenv("SINK_MONGODB_URI", "mongodb://mongodb:27017")
	t.Setenv("SINK_KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("SINK_KAFKA_TOPIC", "sink-mutations")
	t.Setenv("SINK_KAFKA_GROUP_ID", "sink-workers")
	t.Setenv("SINK_MONGODB_BINDINGS", `[{"namespace":"logical","dataset":"records","database":"legacy","collection":"documents"}]`)
	loaded, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if loaded.mode != modeWorker || len(loaded.kafkaBrokers) != 2 {
		t.Fatalf("loadConfig() = %#v", loaded)
	}
	if len(loaded.mongoBindings) != 1 {
		t.Fatalf("loadConfig() bindings = %#v", loaded.mongoBindings)
	}
}

func TestLoadConfigRejectsPartialKafkaConfiguration(t *testing.T) {
	resetConfigEnvironment(t)
	t.Setenv("SINK_MONGODB_URI", "mongodb://mongodb:27017")
	t.Setenv("SINK_KAFKA_BROKERS", "kafka:9092")
	_, err := loadConfig()
	if err == nil {
		t.Fatal("loadConfig() error = nil")
	}
}

func resetConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range configEnvironment {
		t.Setenv(name, "")
	}
}
