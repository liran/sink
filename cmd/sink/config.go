package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/liran/sink/internal/storage/mongodb"
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

type configFile struct {
	Mode                   string            `yaml:"mode"`
	GRPC                   grpcConfigFile    `yaml:"grpc"`
	Storage                storageConfigFile `yaml:"storage"`
	Service                serviceConfigFile `yaml:"service"`
	Kafka                  kafkaConfigFile   `yaml:"kafka"`
	ShutdownTimeoutSeconds *int              `yaml:"shutdown_timeout_seconds"`
}

type grpcConfigFile struct {
	Address string `yaml:"address"`
}

type storageConfigFile struct {
	Driver  string            `yaml:"driver"`
	MongoDB mongoDBConfigFile `yaml:"mongodb"`
	Search  searchConfigFile  `yaml:"search"`
}

type mongoDBConfigFile struct {
	URI         string               `yaml:"uri"`
	Store       string               `yaml:"store"`
	HiddenField string               `yaml:"hidden_field"`
	Bindings    []mongoBindingConfig `yaml:"bindings"`
}

type searchConfigFile struct {
	Endpoints []string              `yaml:"endpoints"`
	Store     string                `yaml:"store"`
	Bindings  []searchBindingConfig `yaml:"bindings"`
	Username  string                `yaml:"username"`
	Password  string                `yaml:"password"`
	APIKey    string                `yaml:"api_key"`
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

type mongoBindingConfig struct {
	Namespace  string `yaml:"namespace"`
	Dataset    string `yaml:"dataset"`
	Database   string `yaml:"database"`
	Collection string `yaml:"collection"`
}

type searchBindingConfig struct {
	Namespace string `yaml:"namespace"`
	Dataset   string `yaml:"dataset"`
	Index     string `yaml:"index"`
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
	loaded.storageDriver = storageDriver(valueOrDefault(file.Storage.Driver, string(driverMongoDB)))
	if err := loadStorageConfig(&loaded, file.Storage); err != nil {
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

func loadStorageConfig(loaded *config, file storageConfigFile) error {
	switch loaded.storageDriver {
	case driverMongoDB:
		loaded.mongoURI = strings.TrimSpace(file.MongoDB.URI)
		if loaded.mongoURI == "" {
			return errors.New("storage.mongodb.uri is required when storage.driver is mongodb")
		}
		loaded.mongoStore = valueOrDefault(file.MongoDB.Store, "primary")
		loaded.mongoHiddenField = strings.TrimSpace(file.MongoDB.HiddenField)
		bindings, err := parseMongoBindings(file.MongoDB.Bindings)
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
		return errors.New("storage.driver must be mongodb, elasticsearch, or opensearch")
	}

	loaded.searchEndpoints = nonEmptyValues(file.Search.Endpoints)
	if len(loaded.searchEndpoints) == 0 {
		return errors.New("storage.search.endpoints is required when storage.driver is elasticsearch or opensearch")
	}
	loaded.searchStore = valueOrDefault(file.Search.Store, "primary")
	bindings, err := parseSearchBindings(file.Search.Bindings)
	if err != nil {
		return err
	}
	loaded.searchBindings = bindings
	loaded.searchUsername = strings.TrimSpace(file.Search.Username)
	loaded.searchPassword = strings.TrimSpace(file.Search.Password)
	loaded.searchAPIKey = strings.TrimSpace(file.Search.APIKey)
	if (loaded.searchUsername == "") != (loaded.searchPassword == "") {
		return errors.New("storage.search.username and storage.search.password must be configured together")
	}
	if loaded.searchAPIKey != "" && loaded.searchUsername != "" {
		return errors.New("storage.search.api_key cannot be combined with basic authentication")
	}
	return nil
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

func parseMongoBindings(entries []mongoBindingConfig) (map[mongodb.Dataset]mongodb.Binding, error) {
	bindings := make(map[mongodb.Dataset]mongodb.Binding, len(entries))
	for _, entry := range entries {
		namespace := strings.TrimSpace(entry.Namespace)
		datasetName := strings.TrimSpace(entry.Dataset)
		database := strings.TrimSpace(entry.Database)
		collection := strings.TrimSpace(entry.Collection)
		if namespace == "" || datasetName == "" || database == "" || collection == "" {
			return nil, errors.New("storage.mongodb.bindings contains an incomplete binding")
		}
		dataset := mongodb.Dataset{
			Namespace: namespace,
			Dataset:   datasetName,
		}
		if _, exists := bindings[dataset]; exists {
			return nil, fmt.Errorf("storage.mongodb.bindings contains duplicate dataset %q/%q", namespace, datasetName)
		}
		binding := mongodb.Binding{
			Database:   database,
			Collection: collection,
		}
		bindings[dataset] = binding
	}
	return bindings, nil
}

func parseSearchBindings(entries []searchBindingConfig) (map[searchstorage.Dataset]searchstorage.Binding, error) {
	bindings := make(map[searchstorage.Dataset]searchstorage.Binding, len(entries))
	for _, entry := range entries {
		namespace := strings.TrimSpace(entry.Namespace)
		datasetName := strings.TrimSpace(entry.Dataset)
		index := strings.TrimSpace(entry.Index)
		if namespace == "" || datasetName == "" || index == "" {
			return nil, errors.New("storage.search.bindings contains an incomplete binding")
		}
		dataset := searchstorage.Dataset{
			Namespace: namespace,
			Dataset:   datasetName,
		}
		if _, exists := bindings[dataset]; exists {
			return nil, fmt.Errorf("storage.search.bindings contains duplicate dataset %q/%q", namespace, datasetName)
		}
		binding := searchstorage.Binding{Index: index}
		bindings[dataset] = binding
	}
	return bindings, nil
}
