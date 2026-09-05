package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// TopicManager gates one store until its durable topic policy has been applied.
// Each store reconciles independently so an unavailable cluster does not prevent
// other stores from starting. The closed channel also publishes readiness safely.
type TopicManager struct {
	options TopicOptions
	ready   chan struct{}
}

func NewTopicManager(options TopicOptions) *TopicManager {
	manager := &TopicManager{options: options, ready: make(chan struct{})}
	return manager
}

func (m *TopicManager) Run(ctx context.Context) {
	for {
		if err := EnsureTopics(ctx, m.options); err == nil {
			close(m.ready)
			return
		} else if ctx.Err() == nil {
			slog.Error("Kafka topic policy unavailable; store remains gated", "topics", m.options.Topics, "error", err)
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *TopicManager) Ping(ctx context.Context) error {
	if m == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ready:
		return nil
	default:
		return errors.New("kafka topic policy has not been established")
	}
}

func (m *TopicManager) Wait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ready:
		return nil
	}
}
