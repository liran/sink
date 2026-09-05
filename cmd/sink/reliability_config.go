package main

import (
	"fmt"
	"time"
)

func (c *config) loadReliabilityConfig(file serviceConfigFile) error {
	seconds, err := boundedInt("service.request_timeout_seconds", file.RequestTimeoutSeconds, 30, 300)
	if err != nil {
		return err
	}
	c.requestTimeout = time.Duration(seconds) * time.Second
	c.maxInFlightRequests, err = boundedInt("service.max_in_flight_requests", file.MaxInFlightRequests, 128, 10000)
	if err != nil {
		return err
	}
	c.maxInFlightBytes, err = boundedInt("service.max_in_flight_bytes", file.MaxInFlightBytes, 256<<20, 16<<30)
	if err != nil {
		return err
	}
	c.maxStoreRequests, err = boundedInt("service.max_store_requests", file.MaxStoreRequests, 32, 10000)
	if err != nil {
		return err
	}
	// Leave room for status entries and protocol framing at the transport boundary.
	limit := min(32<<20, c.grpcMaxSendBytes/2)
	c.maxReadBytes, err = boundedInt("service.max_read_bytes", file.MaxReadBytes, limit, c.grpcMaxSendBytes/2)
	return err
}

func (c *backendKafkaConfig) loadReliabilityConfig(prefix string, file kafkaConfigFile) error {
	hours, err := boundedInt(prefix+".dead_letter_retention_hours", file.DeadLetterRetentionHours, 720, int(maxKafkaTopicRetentionHours))
	if err != nil {
		return err
	}
	c.deadLetterRetention = time.Duration(hours) * time.Hour
	c.minInSyncReplicas, err = boundedInt(prefix+".min_insync_replicas", file.MinInSyncReplicas, min(2, c.topicReplicationFactor), c.topicReplicationFactor)
	if err != nil {
		return err
	}
	c.maxBufferedBytes, err = boundedInt(prefix+".max_buffered_bytes", file.MaxBufferedBytes, 64<<20, 1<<30)
	if err != nil {
		return err
	}
	c.maxRecordBytes, err = boundedInt(prefix+".max_record_bytes", file.MaxRecordBytes, 900<<10, min(64<<20, c.maxBufferedBytes))
	if err != nil {
		return err
	}
	milliseconds, err := boundedInt(prefix+".processing_timeout_milliseconds", file.ProcessingTimeoutMilliseconds, 20000, 20000)
	if err != nil {
		return err
	}
	c.processingTimeout = time.Duration(milliseconds) * time.Millisecond
	return nil
}

func boundedInt(name string, value *int, fallback int, maximum int) (int, error) {
	result, err := positiveIntOrDefault(name, value, fallback)
	if err != nil {
		return 0, err
	}
	if result <= 0 || result > maximum {
		return 0, fmt.Errorf("%s must be between 1 and %d", name, maximum)
	}
	return result, nil
}
