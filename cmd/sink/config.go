package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	searchstorage "github.com/liran/sink/internal/storage/search"
	"gopkg.in/yaml.v3"
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
	storages            []backendConfig
	maxOperations       int
	maxMergeAttempts    int
	kafkaBrokers        []string
	kafkaTopic          string
	kafkaGroupID        string
	kafkaMaxPollRecords int
	shutdownTimeout     time.Duration
}

type backendConfig struct {
	name               string
	driver             storageDriver
	mongoURI           string
	mongoMetadataField string
	searchDriver       searchstorage.Driver
	searchEndpoints    []string
	searchUsername     string
	searchPassword     string
	searchAPIKey       string
}

type configFile struct {
	Mode                   string              `yaml:"mode"`
	GRPC                   grpcConfigFile      `yaml:"grpc"`
	Storages               []storageConfigFile `yaml:"storages"`
	Service                serviceConfigFile   `yaml:"service"`
	Kafka                  kafkaConfigFile     `yaml:"kafka"`
	ShutdownTimeoutSeconds *int                `yaml:"shutdown_timeout_seconds"`
}

type grpcConfigFile struct {
	Address string `yaml:"address"`
}

type storageConfigFile struct {
	Name    string            `yaml:"name"`
	Driver  string            `yaml:"driver"`
	MongoDB mongoDBConfigFile `yaml:"mongodb"`
	Search  searchConfigFile  `yaml:"search"`
}

type mongoDBConfigFile struct {
	URI           string `yaml:"uri"`
	MetadataField string `yaml:"metadata_field"`
}

type searchConfigFile struct {
	Endpoints []string `yaml:"endpoints"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	APIKey    string   `yaml:"api_key"`
}

type serviceConfigFile struct {
	MaxOperations    *int `yaml:"max_operations"`
	MaxMergeAttempts *int `yaml:"max_merge_attempts"`
}

type kafkaConfigFile struct {
	Brokers        []string `yaml:"brokers"`
	Topic          string   `yaml:"topic"`
	GroupID        string   `yaml:"group_id"`
	MaxPollRecords *int     `yaml:"max_poll_records"`
}

func loadConfig(path string) (config, error) {
	var loaded config
	raw, err := os.ReadFile(path)
	if err != nil {
		return loaded, fmt.Errorf("read config file %q: %w", path, err)
	}

	var file configFile
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		return loaded, fmt.Errorf("decode config file %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return loaded, fmt.Errorf("decode config file %q: %w", path, err)
		}
		return loaded, fmt.Errorf("decode config file %q: multiple YAML documents are not supported", path)
	}

	loaded.mode = runMode(valueOrDefault(file.Mode, string(modeServer)))
	if loaded.mode != modeServer && loaded.mode != modeWorker && loaded.mode != modeAll {
		return loaded, errors.New("mode must be server, worker, or all")
	}
	loaded.grpcAddress = valueOrDefault(file.GRPC.Address, ":8080")
	loaded.storages, err = loadStorageConfigs(file.Storages)
	if err != nil {
		return loaded, err
	}

	loaded.maxOperations, err = positiveIntOrDefault("service.max_operations", file.Service.MaxOperations, 1000)
	if err != nil {
		return loaded, err
	}
	loaded.maxMergeAttempts, err = positiveIntOrDefault("service.max_merge_attempts", file.Service.MaxMergeAttempts, 3)
	if err != nil {
		return loaded, err
	}
	loaded.kafkaMaxPollRecords, err = positiveIntOrDefault("kafka.max_poll_records", file.Kafka.MaxPollRecords, 500)
	if err != nil {
		return loaded, err
	}
	shutdownSeconds, err := positiveIntOrDefault("shutdown_timeout_seconds", file.ShutdownTimeoutSeconds, 15)
	if err != nil {
		return loaded, err
	}
	loaded.shutdownTimeout = time.Duration(shutdownSeconds) * time.Second

	loaded.kafkaBrokers = nonEmptyValues(file.Kafka.Brokers)
	loaded.kafkaTopic = strings.TrimSpace(file.Kafka.Topic)
	loaded.kafkaGroupID = strings.TrimSpace(file.Kafka.GroupID)
	if err := validateKafkaConfig(loaded); err != nil {
		return loaded, err
	}
	return loaded, nil
}

func loadStorageConfigs(files []storageConfigFile) ([]backendConfig, error) {
	if len(files) == 0 {
		return nil, errors.New("storages must contain at least one storage")
	}
	loaded := make([]backendConfig, 0, len(files))
	names := make(map[string]struct{}, len(files))
	for index, file := range files {
		configured, err := loadStorageConfig(index, file)
		if err != nil {
			return nil, err
		}
		if _, exists := names[configured.name]; exists {
			return nil, fmt.Errorf("storages contains duplicate name %q", configured.name)
		}
		names[configured.name] = struct{}{}
		loaded = append(loaded, configured)
	}
	return loaded, nil
}

func loadStorageConfig(index int, file storageConfigFile) (backendConfig, error) {
	var loaded backendConfig
	prefix := fmt.Sprintf("storages[%d]", index)
	loaded.name = strings.TrimSpace(file.Name)
	if loaded.name == "" {
		return loaded, fmt.Errorf("%s.name is required", prefix)
	}
	loaded.driver = storageDriver(strings.TrimSpace(file.Driver))
	switch loaded.driver {
	case driverMongoDB:
		loaded.mongoURI = strings.TrimSpace(file.MongoDB.URI)
		if loaded.mongoURI == "" {
			return loaded, fmt.Errorf("%s.mongodb.uri is required when driver is mongodb", prefix)
		}
		loaded.mongoMetadataField = strings.TrimSpace(file.MongoDB.MetadataField)
		return loaded, nil
	case driverElasticsearch:
		loaded.searchDriver = searchstorage.DriverElasticsearch
	case driverOpenSearch:
		loaded.searchDriver = searchstorage.DriverOpenSearch
	default:
		return loaded, fmt.Errorf("%s.driver must be mongodb, elasticsearch, or opensearch", prefix)
	}

	loaded.searchEndpoints = nonEmptyValues(file.Search.Endpoints)
	if len(loaded.searchEndpoints) == 0 {
		return loaded, fmt.Errorf("%s.search.endpoints is required for a search driver", prefix)
	}
	loaded.searchUsername = strings.TrimSpace(file.Search.Username)
	loaded.searchPassword = strings.TrimSpace(file.Search.Password)
	loaded.searchAPIKey = strings.TrimSpace(file.Search.APIKey)
	if (loaded.searchUsername == "") != (loaded.searchPassword == "") {
		return loaded, fmt.Errorf("%s.search.username and %s.search.password must be configured together", prefix, prefix)
	}
	if loaded.searchAPIKey != "" && loaded.searchUsername != "" {
		return loaded, fmt.Errorf("%s.search.api_key cannot be combined with basic authentication", prefix)
	}
	return loaded, nil
}

func valueOrDefault(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func positiveIntOrDefault(name string, value *int, fallback int) (int, error) {
	if value == nil {
		return fallback, nil
	}
	if *value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return *value, nil
}

func nonEmptyValues(raw []string) []string {
	values := make([]string, 0, len(raw))
	for _, entry := range raw {
		value := strings.TrimSpace(entry)
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
		return errors.New("kafka.brokers and kafka.topic must be configured together")
	}
	if loaded.mode == modeWorker || loaded.mode == modeAll {
		if !hasBrokers || loaded.kafkaGroupID == "" {
			return errors.New("worker and all modes require kafka.brokers, kafka.topic, and kafka.group_id")
		}
	}
	return nil
}
