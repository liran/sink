// Package kafka provides a durable Kafka mutation publisher and consumer.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PublisherOptions struct {
	Topics           *TopicManager
	Brokers          []string
	Topic            string
	ClientOptions    []kgo.Opt
	Metrics          *sinkmetrics.Metrics
	MaxRecordBytes   int
	MaxBufferedBytes int
}

type Publisher struct {
	topics         *TopicManager
	client         *kgo.Client
	topic          string
	metrics        *sinkmetrics.Metrics
	maxRecordBytes int
}

func NewPublisher(opts PublisherOptions) (*Publisher, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka publisher: brokers are required")
	}
	if opts.Topic == "" {
		return nil, errors.New("create Kafka publisher: topic is required")
	}
	if opts.MaxRecordBytes < 0 || opts.MaxBufferedBytes < 0 {
		return nil, errors.New("create Kafka publisher: byte limits cannot be negative")
	}
	if opts.MaxRecordBytes == 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	if opts.MaxBufferedBytes == 0 {
		opts.MaxBufferedBytes = 64 << 20
	}
	if opts.MaxRecordBytes > opts.MaxBufferedBytes || opts.MaxRecordBytes > 64<<20 {
		return nil, errors.New("create Kafka publisher: record limit must fit the buffer and cannot exceed 64 MiB")
	}
	clientOptions := append([]kgo.Opt(nil), opts.ClientOptions...)
	requiredOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedBytes(opts.MaxBufferedBytes),
		kgo.ProducerBatchMaxBytes(int32(opts.MaxRecordBytes + kafkaRecordOverhead)),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	publisher := &Publisher{topics: opts.Topics, client: client, topic: opts.Topic, metrics: opts.Metrics, maxRecordBytes: opts.MaxRecordBytes}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	started := time.Now()
	response := queue.PublishResponse{
		Results: make([]queue.PublishResult, len(req.Mutations)),
	}
	if err := p.topics.Ping(ctx); err != nil {
		p.metrics.ObserveKafkaPublish(time.Since(started), 0, len(req.Mutations))
		for index := range response.Results {
			response.Results[index].Status = queue.PublishStatusFailed
			response.Results[index].Err = storage.BackendError(err)
		}
		return response, nil
	}
	records := make([]*kgo.Record, 0, len(req.Mutations))
	indexes := make(map[*kgo.Record]int, len(req.Mutations))
	for index, mutation := range req.Mutations {
		key, err := queue.MutationKey(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(err)}
			continue
		}
		messageBytes := queue.MutationSize(mutation)
		if messageBytes > p.maxRecordBytes-len(key) {
			cause := fmt.Errorf("kafka mutation exceeds %d bytes including its address and expanded Lua source", p.maxRecordBytes)
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(cause)}
			continue
		}
		value, err := queue.MarshalMutation(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(err)}
			continue
		}
		record := &kgo.Record{Topic: p.topic, Key: key, Value: value}
		records = append(records, record)
		indexes[record] = index
	}
	if len(records) == 0 {
		p.metrics.ObserveKafkaPublish(time.Since(started), 0, len(req.Mutations))
		return response, nil
	}

	produced := make([]kgo.ProduceResult, len(records))
	var pending sync.WaitGroup
	pending.Add(len(records))
	for index, record := range records {
		p.client.TryProduce(ctx, record, func(record *kgo.Record, err error) {
			produced[index] = kgo.ProduceResult{Record: record, Err: err}
			pending.Done()
		})
	}
	pending.Wait()
	accepted := 0
	failed := len(req.Mutations) - len(records)
	for _, result := range produced {
		index, exists := indexes[result.Record]
		if !exists {
			continue
		}
		if result.Err != nil {
			failed++
			response.Results[index] = queue.PublishResult{
				Status: queue.PublishStatusFailed,
				Err:    publishError(result.Err),
			}
			continue
		}
		response.Results[index].Status = queue.PublishStatusAccepted
		accepted++
	}
	p.metrics.ObserveKafkaPublish(time.Since(started), accepted, failed)
	return response, nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	if err := p.topics.Ping(ctx); err != nil {
		return err
	}
	return p.client.Ping(ctx)
}

func (p *Publisher) Close() {
	p.client.Close()
}
