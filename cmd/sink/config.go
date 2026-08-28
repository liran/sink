package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/liran/sink/internal/storage/mongodb"
)

type runMode string

const (
	modeServer runMode = "server"
	modeWorker runMode = "worker"
	modeAll    runMode = "all"
)

type config struct {
	mode                runMode
	grpcAddress         string
	mongoURI            string
	mongoStore          string
	mongoHiddenField    string
	mongoBindings       map[mongodb.Dataset]mongodb.Binding
	maxOperations       int
	maxMergeAttempts    int
	kafkaBrokers        []string
	kafkaTopic          string
	kafkaGroupID        string
	kafkaMaxPollRecords int
	shutdownTimeout     time.Duration
}

type mongoBindingConfig struct {
	Namespace  string `json:"namespace"`
	Dataset    string `json:"dataset"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

func loadConfig() (config, error) {
	var loaded config
	loaded.mode = runMode(envOrDefault("SINK_MODE", string(modeServer)))
	if loaded.mode != modeServer && loaded.mode != modeWorker && loaded.mode != modeAll {
		return loaded, errors.New("SINK_MODE must be server, worker, or all")
	}
	loaded.grpcAddress = envOrDefault("SINK_GRPC_ADDRESS", ":8080")
	loaded.mongoURI = strings.TrimSpace(os.Getenv("SINK_MONGODB_URI"))
	if loaded.mongoURI == "" {
		return loaded, errors.New("SINK_MONGODB_URI is required")
	}
	loaded.mongoStore = envOrDefault("SINK_MONGODB_STORE", "primary")
	loaded.mongoHiddenField = strings.TrimSpace(os.Getenv("SINK_MONGODB_HIDDEN_FIELD"))

	bindings, err := parseMongoBindings(os.Getenv("SINK_MONGODB_BINDINGS"))
	if err != nil {
		return loaded, err
	}
	loaded.mongoBindings = bindings
	loaded.maxOperations, err = positiveIntEnv("SINK_MAX_OPERATIONS", 1000)
	if err != nil {
		return loaded, err
	}
	loaded.maxMergeAttempts, err = positiveIntEnv("SINK_MAX_MERGE_ATTEMPTS", 3)
	if err != nil {
		return loaded, err
	}
	loaded.kafkaMaxPollRecords, err = positiveIntEnv("SINK_KAFKA_MAX_POLL_RECORDS", 500)
	if err != nil {
		return loaded, err
	}
	shutdownSeconds, err := positiveIntEnv("SINK_SHUTDOWN_TIMEOUT_SECONDS", 15)
	if err != nil {
		return loaded, err
	}
	loaded.shutdownTimeout = time.Duration(shutdownSeconds) * time.Second

	loaded.kafkaBrokers = splitNonEmpty(os.Getenv("SINK_KAFKA_BROKERS"))
	loaded.kafkaTopic = strings.TrimSpace(os.Getenv("SINK_KAFKA_TOPIC"))
	loaded.kafkaGroupID = strings.TrimSpace(os.Getenv("SINK_KAFKA_GROUP_ID"))
	if err := validateKafkaConfig(loaded); err != nil {
		return loaded, err
	}
	return loaded, nil
}

func envOrDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func validateKafkaConfig(loaded config) error {
	hasBrokers := len(loaded.kafkaBrokers) > 0
	hasTopic := loaded.kafkaTopic != ""
	if hasBrokers != hasTopic {
		return errors.New("SINK_KAFKA_BROKERS and SINK_KAFKA_TOPIC must be configured together")
	}
	if loaded.mode == modeWorker || loaded.mode == modeAll {
		if !hasBrokers || loaded.kafkaGroupID == "" {
			return errors.New("worker mode requires SINK_KAFKA_BROKERS, SINK_KAFKA_TOPIC, and SINK_KAFKA_GROUP_ID")
		}
	}
	return nil
}

func parseMongoBindings(raw string) (map[mongodb.Dataset]mongodb.Binding, error) {
	bindings := make(map[mongodb.Dataset]mongodb.Binding)
	if strings.TrimSpace(raw) == "" {
		return bindings, nil
	}
	var entries []mongoBindingConfig
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("parse SINK_MONGODB_BINDINGS: %w", err)
	}
	for _, entry := range entries {
		if entry.Namespace == "" || entry.Dataset == "" || entry.Database == "" || entry.Collection == "" {
			return nil, errors.New("SINK_MONGODB_BINDINGS contains an incomplete binding")
		}
		dataset := mongodb.Dataset{Namespace: entry.Namespace, Dataset: entry.Dataset}
		if _, exists := bindings[dataset]; exists {
			return nil, fmt.Errorf("SINK_MONGODB_BINDINGS contains duplicate dataset %q/%q", entry.Namespace, entry.Dataset)
		}
		binding := mongodb.Binding{Database: entry.Database, Collection: entry.Collection}
		bindings[dataset] = binding
	}
	return bindings, nil
}
