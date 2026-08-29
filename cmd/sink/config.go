package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/liran/sink/internal/merge"
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
	mode                   runMode
	grpcAddress            string
	grpcMaxReceiveBytes    int
	grpcMaxSendBytes       int
	prometheusAddress      string
	storages               []backendConfig
	maxOperations          int
	maxMergeAttempts       int
	batchingEnabled        bool
	batchingMaxWait        time.Duration
	batchingMaxOperations  int
	batchingMaxBytes       int
	batchingMaxQueuedOps   int
	batchingMaxQueuedBytes int
	luaOptions             merge.LuaOptions
	shutdownTimeout        time.Duration
}

type backendConfig struct {
	name                     string
	driver                   storageDriver
	mongoURI                 string
	mongoMetadataField       string
	mongoMaxConcurrentWrites int
	mongoMaxConcurrentGroups int
	searchDriver             searchstorage.Driver
	searchEndpoints          []string
	searchUsername           string
	searchPassword           string
	searchAPIKey             string
	kafka                    backendKafkaConfig
}

type backendKafkaConfig struct {
	configured       bool
	brokers          []string
	topic            string
	groupID          string
	deadLetterTopic  string
	maxPollRecords   int
	maxRetryAttempts int
	retryBackoff     time.Duration
	maxRetryBackoff  time.Duration
}

type configFile struct {
	Mode                   string               `yaml:"mode"`
	GRPC                   grpcConfigFile       `yaml:"grpc"`
	Prometheus             prometheusConfigFile `yaml:"prometheus"`
	Storages               []storageConfigFile  `yaml:"storages"`
	Service                serviceConfigFile    `yaml:"service"`
	ShutdownTimeoutSeconds *int                 `yaml:"shutdown_timeout_seconds"`
}

type grpcConfigFile struct {
	Address                string `yaml:"address"`
	MaxReceiveMessageBytes *int   `yaml:"max_receive_message_bytes"`
	MaxSendMessageBytes    *int   `yaml:"max_send_message_bytes"`
}

type prometheusConfigFile struct {
	Address string `yaml:"address"`
}

type storageConfigFile struct {
	Name    string            `yaml:"name"`
	Driver  string            `yaml:"driver"`
	MongoDB mongoDBConfigFile `yaml:"mongodb"`
	Search  searchConfigFile  `yaml:"search"`
	Kafka   kafkaConfigFile   `yaml:"kafka"`
}

type mongoDBConfigFile struct {
	URI                 string `yaml:"uri"`
	MetadataField       string `yaml:"metadata_field"`
	MaxConcurrentWrites *int   `yaml:"max_concurrent_writes"`
	MaxConcurrentGroups *int   `yaml:"max_concurrent_groups"`
}

type searchConfigFile struct {
	Endpoints []string `yaml:"endpoints"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	APIKey    string   `yaml:"api_key"`
}

type serviceConfigFile struct {
	MaxOperations    *int               `yaml:"max_operations"`
	MaxMergeAttempts *int               `yaml:"max_merge_attempts"`
	Batching         batchingConfigFile `yaml:"batching"`
	Lua              luaConfigFile      `yaml:"lua"`
}

type batchingConfigFile struct {
	Enabled             *bool `yaml:"enabled"`
	MaxWaitMilliseconds *int  `yaml:"max_wait_milliseconds"`
	MaxOperations       *int  `yaml:"max_operations"`
	MaxBytes            *int  `yaml:"max_bytes"`
	MaxQueuedOperations *int  `yaml:"max_queued_operations"`
	MaxQueuedBytes      *int  `yaml:"max_queued_bytes"`
}

type luaConfigFile struct {
	TimeoutMilliseconds *int `yaml:"timeout_milliseconds"`
	MaxSourceBytes      *int `yaml:"max_source_bytes"`
	MaxResultBytes      *int `yaml:"max_result_bytes"`
	MaxCachedPrograms   *int `yaml:"max_cached_programs"`
	MaxInstructions     *int `yaml:"max_instructions"`
}

type kafkaConfigFile struct {
	Brokers                     []string `yaml:"brokers"`
	Topic                       string   `yaml:"topic"`
	GroupID                     string   `yaml:"group_id"`
	DeadLetterTopic             string   `yaml:"dead_letter_topic"`
	MaxPollRecords              *int     `yaml:"max_poll_records"`
	MaxRetryAttempts            *int     `yaml:"max_retry_attempts"`
	RetryBackoffMilliseconds    *int     `yaml:"retry_backoff_milliseconds"`
	MaxRetryBackoffMilliseconds *int     `yaml:"max_retry_backoff_milliseconds"`
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
	loaded.grpcMaxReceiveBytes, err = positiveIntOrDefault("grpc.max_receive_message_bytes", file.GRPC.MaxReceiveMessageBytes, 64<<20)
	if err != nil {
		return loaded, err
	}
	loaded.grpcMaxSendBytes, err = positiveIntOrDefault("grpc.max_send_message_bytes", file.GRPC.MaxSendMessageBytes, 64<<20)
	if err != nil {
		return loaded, err
	}
	loaded.prometheusAddress = strings.TrimSpace(file.Prometheus.Address)
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
	loaded.batchingEnabled = boolOrDefault(file.Service.Batching.Enabled, true)
	batchWaitMilliseconds, err := positiveIntOrDefault("service.batching.max_wait_milliseconds", file.Service.Batching.MaxWaitMilliseconds, 2)
	if err != nil {
		return loaded, err
	}
	loaded.batchingMaxWait = time.Duration(batchWaitMilliseconds) * time.Millisecond
	loaded.batchingMaxOperations, err = positiveIntOrDefault("service.batching.max_operations", file.Service.Batching.MaxOperations, loaded.maxOperations)
	if err != nil {
		return loaded, err
	}
	loaded.batchingMaxBytes, err = positiveIntOrDefault("service.batching.max_bytes", file.Service.Batching.MaxBytes, 16<<20)
	if err != nil {
		return loaded, err
	}
	defaultQueuedOperations := max(10_000, loaded.maxOperations)
	loaded.batchingMaxQueuedOps, err = positiveIntOrDefault("service.batching.max_queued_operations", file.Service.Batching.MaxQueuedOperations, defaultQueuedOperations)
	if err != nil {
		return loaded, err
	}
	defaultQueuedBytes := max(128<<20, loaded.grpcMaxReceiveBytes)
	loaded.batchingMaxQueuedBytes, err = positiveIntOrDefault("service.batching.max_queued_bytes", file.Service.Batching.MaxQueuedBytes, defaultQueuedBytes)
	if err != nil {
		return loaded, err
	}
	if err := validateBatchingConfig(loaded); err != nil {
		return loaded, err
	}
	luaTimeoutMilliseconds, err := positiveIntOrDefault("service.lua.timeout_milliseconds", file.Service.Lua.TimeoutMilliseconds, 100)
	if err != nil {
		return loaded, err
	}
	loaded.luaOptions.Timeout = time.Duration(luaTimeoutMilliseconds) * time.Millisecond
	loaded.luaOptions.MaxSourceBytes, err = positiveIntOrDefault("service.lua.max_source_bytes", file.Service.Lua.MaxSourceBytes, 64<<10)
	if err != nil {
		return loaded, err
	}
	loaded.luaOptions.MaxResultBytes, err = positiveIntOrDefault("service.lua.max_result_bytes", file.Service.Lua.MaxResultBytes, 16<<20)
	if err != nil {
		return loaded, err
	}
	loaded.luaOptions.MaxCachedPrograms, err = positiveIntOrDefault("service.lua.max_cached_programs", file.Service.Lua.MaxCachedPrograms, 256)
	if err != nil {
		return loaded, err
	}
	maxInstructions, err := positiveIntOrDefault("service.lua.max_instructions", file.Service.Lua.MaxInstructions, 1_000_000)
	if err != nil {
		return loaded, err
	}
	loaded.luaOptions.MaxInstructions = int64(maxInstructions)
	shutdownSeconds, err := positiveIntOrDefault("shutdown_timeout_seconds", file.ShutdownTimeoutSeconds, 15)
	if err != nil {
		return loaded, err
	}
	loaded.shutdownTimeout = time.Duration(shutdownSeconds) * time.Second

	if err := validateKafkaConfig(loaded); err != nil {
		return loaded, err
	}
	return loaded, nil
}

func loadKafkaConfig(prefix string, file kafkaConfigFile) (backendKafkaConfig, error) {
	var loaded backendKafkaConfig
	loaded.brokers = nonEmptyValues(file.Brokers)
	loaded.topic = strings.TrimSpace(file.Topic)
	loaded.groupID = strings.TrimSpace(file.GroupID)
	loaded.deadLetterTopic = strings.TrimSpace(file.DeadLetterTopic)
	loaded.configured = len(file.Brokers) > 0 || loaded.topic != "" || loaded.groupID != "" || loaded.deadLetterTopic != "" ||
		file.MaxPollRecords != nil || file.MaxRetryAttempts != nil || file.RetryBackoffMilliseconds != nil ||
		file.MaxRetryBackoffMilliseconds != nil
	if !loaded.configured {
		return loaded, nil
	}
	if loaded.topic != "" && loaded.deadLetterTopic == "" {
		loaded.deadLetterTopic = loaded.topic + ".dlq"
	}
	var err error
	loaded.maxPollRecords, err = positiveIntOrDefault(prefix+".max_poll_records", file.MaxPollRecords, 500)
	if err != nil {
		return loaded, err
	}
	loaded.maxRetryAttempts, err = positiveIntOrDefault(prefix+".max_retry_attempts", file.MaxRetryAttempts, 10)
	if err != nil {
		return loaded, err
	}
	retryBackoffMilliseconds, err := positiveIntOrDefault(prefix+".retry_backoff_milliseconds", file.RetryBackoffMilliseconds, 100)
	if err != nil {
		return loaded, err
	}
	maxRetryBackoffMilliseconds, err := positiveIntOrDefault(prefix+".max_retry_backoff_milliseconds", file.MaxRetryBackoffMilliseconds, 10000)
	if err != nil {
		return loaded, err
	}
	if maxRetryBackoffMilliseconds < retryBackoffMilliseconds {
		return loaded, fmt.Errorf("%s.max_retry_backoff_milliseconds must be at least %s.retry_backoff_milliseconds", prefix, prefix)
	}
	loaded.retryBackoff = time.Duration(retryBackoffMilliseconds) * time.Millisecond
	loaded.maxRetryBackoff = time.Duration(maxRetryBackoffMilliseconds) * time.Millisecond
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
	kafkaConfig, err := loadKafkaConfig(prefix+".kafka", file.Kafka)
	if err != nil {
		return loaded, err
	}
	loaded.kafka = kafkaConfig
	loaded.driver = storageDriver(strings.TrimSpace(file.Driver))
	switch loaded.driver {
	case driverMongoDB:
		loaded.mongoURI = strings.TrimSpace(file.MongoDB.URI)
		if loaded.mongoURI == "" {
			return loaded, fmt.Errorf("%s.mongodb.uri is required when driver is mongodb", prefix)
		}
		loaded.mongoMetadataField = strings.TrimSpace(file.MongoDB.MetadataField)
		maxConcurrentWrites, err := positiveIntOrDefault(prefix+".mongodb.max_concurrent_writes", file.MongoDB.MaxConcurrentWrites, 64)
		if err != nil {
			return loaded, err
		}
		maxConcurrentGroups, err := positiveIntOrDefault(prefix+".mongodb.max_concurrent_groups", file.MongoDB.MaxConcurrentGroups, 16)
		if err != nil {
			return loaded, err
		}
		loaded.mongoMaxConcurrentWrites = maxConcurrentWrites
		loaded.mongoMaxConcurrentGroups = maxConcurrentGroups
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

func boolOrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func validateBatchingConfig(loaded config) error {
	if loaded.batchingMaxOperations > loaded.maxOperations {
		return errors.New("service.batching.max_operations cannot exceed service.max_operations")
	}
	if loaded.batchingMaxQueuedOps < loaded.maxOperations || loaded.batchingMaxQueuedOps < loaded.batchingMaxOperations {
		return errors.New("service.batching.max_queued_operations must cover one server request and one batch")
	}
	if loaded.batchingMaxQueuedBytes < loaded.grpcMaxReceiveBytes || loaded.batchingMaxQueuedBytes < loaded.batchingMaxBytes {
		return errors.New("service.batching.max_queued_bytes must cover one gRPC request and one batch")
	}
	return nil
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

type kafkaResourceKey struct {
	cluster string
	name    string
}

func validateKafkaConfig(loaded config) error {
	configuredStores := 0
	topics := make(map[kafkaResourceKey]string)
	groups := make(map[kafkaResourceKey]string)
	deadLetterTopics := make(map[kafkaResourceKey]string)
	for index, configured := range loaded.storages {
		prefix := fmt.Sprintf("storages[%d].kafka", index)
		if !configured.kafka.configured {
			continue
		}
		if len(configured.kafka.brokers) == 0 || configured.kafka.topic == "" {
			return fmt.Errorf("%s.brokers and %s.topic are required", prefix, prefix)
		}
		if (loaded.mode == modeWorker || loaded.mode == modeAll) && configured.kafka.groupID == "" {
			return fmt.Errorf("%s.group_id is required in worker and all modes", prefix)
		}
		if configured.kafka.deadLetterTopic == configured.kafka.topic {
			return fmt.Errorf("%s.dead_letter_topic must differ from topic", prefix)
		}
		cluster := kafkaClusterKey(configured.kafka.brokers)
		topicKey := kafkaResourceKey{cluster: cluster, name: configured.kafka.topic}
		if otherStore, exists := topics[topicKey]; exists {
			return fmt.Errorf("stores %q and %q configure duplicate Kafka topic %q on the same cluster", otherStore, configured.name, configured.kafka.topic)
		}
		topics[topicKey] = configured.name
		if configured.kafka.groupID != "" {
			groupKey := kafkaResourceKey{cluster: cluster, name: configured.kafka.groupID}
			if otherStore, exists := groups[groupKey]; exists {
				return fmt.Errorf("stores %q and %q configure duplicate Kafka group ID %q on the same cluster", otherStore, configured.name, configured.kafka.groupID)
			}
			groups[groupKey] = configured.name
		}
		deadLetterKey := kafkaResourceKey{cluster: cluster, name: configured.kafka.deadLetterTopic}
		if otherStore, exists := deadLetterTopics[deadLetterKey]; exists {
			return fmt.Errorf("stores %q and %q configure duplicate Kafka dead-letter topic %q on the same cluster", otherStore, configured.name, configured.kafka.deadLetterTopic)
		}
		deadLetterTopics[deadLetterKey] = configured.name
		configuredStores++
	}
	if (loaded.mode == modeWorker || loaded.mode == modeAll) && configuredStores == 0 {
		return errors.New("worker and all modes require Kafka settings on at least one store")
	}
	for topicKey, store := range topics {
		if deadLetterStore, exists := deadLetterTopics[topicKey]; exists {
			return fmt.Errorf("store %q Kafka topic %q conflicts with store %q dead-letter topic on the same cluster", store, topicKey.name, deadLetterStore)
		}
	}
	return nil
}

func kafkaClusterKey(brokers []string) string {
	ordered := append([]string(nil), brokers...)
	sort.Strings(ordered)
	return strings.Join(ordered, "\x00")
}
