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
	searchstorage "github.com/liran/sink/internal/storage/search"
)

type runMode string

const (
	modeServer runMode = "server"
	modeWorker runMode = "worker"
	modeAll    runMode = "all"
)

type storageDriver string

const (
	driverMongoDB       storageDriver = "mongodb"
	driverElasticsearch storageDriver = "elasticsearch"
	driverOpenSearch    storageDriver = "opensearch"
)

type config struct {
	mode                runMode
	grpcAddress         string
	storageDriver       storageDriver
	mongoURI            string
	mongoStore          string
	mongoHiddenField    string
	mongoBindings       map[mongodb.Dataset]mongodb.Binding
	searchDriver        searchstorage.Driver
	searchEndpoints     []string
	searchStore         string
	searchBindings      map[searchstorage.Dataset]searchstorage.Binding
	searchUsername      string
	searchPassword      string
	searchAPIKey        string
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

type searchBindingConfig struct {
	Namespace string `json:"namespace"`
	Dataset   string `json:"dataset"`
	Index     string `json:"index"`
}

func loadConfig() (config, error) {
	var loaded config
	loaded.mode = runMode(envOrDefault("SINK_MODE", string(modeServer)))
	if loaded.mode != modeServer && loaded.mode != modeWorker && loaded.mode != modeAll {
		return loaded, errors.New("SINK_MODE must be server, worker, or all")
	}
	loaded.grpcAddress = envOrDefault("SINK_GRPC_ADDRESS", ":8080")
	loaded.storageDriver = storageDriver(envOrDefault("SINK_STORAGE_DRIVER", string(driverMongoDB)))
	if err := loadStorageConfig(&loaded); err != nil {
		return loaded, err
	}

	var err error
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

func loadStorageConfig(loaded *config) error {
	switch loaded.storageDriver {
	case driverMongoDB:
		loaded.mongoURI = strings.TrimSpace(os.Getenv("SINK_MONGODB_URI"))
		if loaded.mongoURI == "" {
			return errors.New("SINK_MONGODB_URI is required for mongodb storage")
		}
		loaded.mongoStore = envOrDefault("SINK_MONGODB_STORE", "primary")
		loaded.mongoHiddenField = strings.TrimSpace(os.Getenv("SINK_MONGODB_HIDDEN_FIELD"))
		bindings, err := parseMongoBindings(os.Getenv("SINK_MONGODB_BINDINGS"))
		if err != nil {
			return err
		}
		loaded.mongoBindings = bindings
		return nil
	case driverElasticsearch:
		loaded.searchDriver = searchstorage.DriverElasticsearch
	case driverOpenSearch:
		loaded.searchDriver = searchstorage.DriverOpenSearch
	default:
		return errors.New("SINK_STORAGE_DRIVER must be mongodb, elasticsearch, or opensearch")
	}

	loaded.searchEndpoints = splitNonEmpty(os.Getenv("SINK_SEARCH_ENDPOINTS"))
	if len(loaded.searchEndpoints) == 0 {
		return errors.New("SINK_SEARCH_ENDPOINTS is required for search storage")
	}
	loaded.searchStore = envOrDefault("SINK_SEARCH_STORE", "primary")
	bindings, err := parseSearchBindings(os.Getenv("SINK_SEARCH_BINDINGS"))
	if err != nil {
		return err
	}
	loaded.searchBindings = bindings
	loaded.searchUsername = strings.TrimSpace(os.Getenv("SINK_SEARCH_USERNAME"))
	loaded.searchPassword = strings.TrimSpace(os.Getenv("SINK_SEARCH_PASSWORD"))
	loaded.searchAPIKey = strings.TrimSpace(os.Getenv("SINK_SEARCH_API_KEY"))
	if (loaded.searchUsername == "") != (loaded.searchPassword == "") {
		return errors.New("SINK_SEARCH_USERNAME and SINK_SEARCH_PASSWORD must be configured together")
	}
	if loaded.searchAPIKey != "" && loaded.searchUsername != "" {
		return errors.New("SINK_SEARCH_API_KEY cannot be combined with basic authentication")
	}
	return nil
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

func parseSearchBindings(raw string) (map[searchstorage.Dataset]searchstorage.Binding, error) {
	bindings := make(map[searchstorage.Dataset]searchstorage.Binding)
	if strings.TrimSpace(raw) == "" {
		return bindings, nil
	}
	var entries []searchBindingConfig
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("parse SINK_SEARCH_BINDINGS: %w", err)
	}
	for _, entry := range entries {
		if entry.Namespace == "" || entry.Dataset == "" || entry.Index == "" {
			return nil, errors.New("SINK_SEARCH_BINDINGS contains an incomplete binding")
		}
		dataset := searchstorage.Dataset{Namespace: entry.Namespace, Dataset: entry.Dataset}
		if _, exists := bindings[dataset]; exists {
			return nil, fmt.Errorf("SINK_SEARCH_BINDINGS contains duplicate dataset %q/%q", entry.Namespace, entry.Dataset)
		}
		binding := searchstorage.Binding{Index: entry.Index}
		bindings[dataset] = binding
	}
	return bindings, nil
}
