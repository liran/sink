// Package kafka provides a durable Kafka mutation publisher and consumer.
package kafka

import (
	"context"
	"errors"

	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PublisherOptions struct {
	Brokers       []string
	Topic         string
	ClientOptions []kgo.Opt
}

type Publisher struct {
	client *kgo.Client
	topic  string
}

func NewPublisher(opts PublisherOptions) (*Publisher, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka publisher: brokers are required")
	}
	if opts.Topic == "" {
		return nil, errors.New("create Kafka publisher: topic is required")
	}
	clientOptions := append([]kgo.Opt(nil), opts.ClientOptions...)
	requiredOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	publisher := &Publisher{client: client, topic: opts.Topic}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	response := queue.PublishResponse{
		Results: make([]queue.PublishResult, len(req.Mutations)),
	}
	records := make([]*kgo.Record, 0, len(req.Mutations))
	indexes := make(map[*kgo.Record]int, len(req.Mutations))
	for index, mutation := range req.Mutations {
		key, err := queue.MutationKey(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: err}
			continue
		}
		value, err := queue.MarshalMutation(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: err}
			continue
		}
		record := &kgo.Record{Topic: p.topic, Key: key, Value: value}
		records = append(records, record)
		indexes[record] = index
	}
	if len(records) == 0 {
		return response, nil
	}

	produced := p.client.ProduceSync(ctx, records...)
	for _, result := range produced {
		index, exists := indexes[result.Record]
		if !exists {
			continue
		}
		if result.Err != nil {
			response.Results[index] = queue.PublishResult{
				Status: queue.PublishStatusFailed,
				Err:    result.Err,
			}
			continue
		}
		response.Results[index].Status = queue.PublishStatusAccepted
	}
	return response, nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

func (p *Publisher) Close() {
	p.client.Close()
}
