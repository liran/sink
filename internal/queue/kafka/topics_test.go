package kafka_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/queue/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestEnsureTopicsCreatesAndReconcilesTopicSettings(t *testing.T) {
	const sourceTopic = "sink-managed-mutations"
	const deadLetterTopic = "sink-managed-mutations.dlq"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(3))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	clientOptions := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	initialRetention := strconv.FormatInt(time.Hour.Milliseconds(), 10)
	configs := map[string]*string{"retention.ms": &initialRetention}
	_, err = admin.CreateTopic(t.Context(), 2, 2, configs, sourceTopic)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	topics := []string{sourceTopic, deadLetterTopic}
	topicOptions := kafka.TopicOptions{
		Brokers:           cluster.ListenAddrs(),
		Topics:            topics,
		Partitions:        4,
		ReplicationFactor: 2,
		Retention:         72 * time.Hour,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := kafka.EnsureTopics(ctx, topicOptions); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	if err := kafka.EnsureTopics(ctx, topicOptions); err != nil {
		t.Fatalf("EnsureTopics() second error = %v", err)
	}

	details, err := admin.ListTopics(ctx, topics...)
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	for _, topic := range topics {
		detail := details[topic]
		if len(detail.Partitions) != 4 {
			t.Fatalf("topic %q partitions = %d", topic, len(detail.Partitions))
		}
		for partition, configured := range detail.Partitions {
			if len(configured.Replicas) != 2 {
				t.Fatalf("topic %q partition %d replicas = %v", topic, partition, configured.Replicas)
			}
		}
	}

	resources, err := admin.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs() error = %v", err)
	}
	expectedRetention := strconv.FormatInt((72 * time.Hour).Milliseconds(), 10)
	for _, topic := range topics {
		resource, resourceErr := resources.On(topic, nil)
		if resourceErr != nil {
			t.Fatalf("DescribeTopicConfigs(%q) error = %v", topic, resourceErr)
		}
		if actual := topicConfig(resource, "retention.ms"); actual != expectedRetention {
			t.Fatalf("topic %q retention.ms = %q", topic, actual)
		}
	}
}

func TestEnsureTopicsCreatesMissingTopic(t *testing.T) {
	const topic = "missing-topic"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(2))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)
	topicOptions := kafka.TopicOptions{
		Brokers:           cluster.ListenAddrs(),
		Topics:            []string{topic},
		Partitions:        4,
		ReplicationFactor: 2,
		Retention:         72 * time.Hour,
	}
	err = kafka.EnsureTopics(t.Context(), topicOptions)
	if err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}

	clientOptions := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	details, err := kadm.NewClient(client).ListTopics(t.Context(), topic)
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	if !details.Has(topic) || len(details[topic].Partitions) != 4 {
		t.Fatalf("topic details = %#v", details[topic])
	}
}

func TestEnsureTopicsRejectsPartitionDecrease(t *testing.T) {
	const topic = "too-many-partitions"
	clusterOptions := []kfake.Opt{
		kfake.NumBrokers(2),
		kfake.SeedTopics(5, topic),
	}
	cluster, err := kfake.NewCluster(clusterOptions...)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)
	topicOptions := kafka.TopicOptions{
		Brokers:           cluster.ListenAddrs(),
		Topics:            []string{topic},
		Partitions:        4,
		ReplicationFactor: 2,
		Retention:         72 * time.Hour,
	}
	err = kafka.EnsureTopics(t.Context(), topicOptions)
	if err == nil || !strings.Contains(err.Error(), "cannot decrease partition count") {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
}

func TestEnsureTopicsRejectsReplicationFactorAboveBrokerCount(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)
	topicOptions := kafka.TopicOptions{
		Brokers:           cluster.ListenAddrs(),
		Topics:            []string{"sink-mutations"},
		Partitions:        4,
		ReplicationFactor: 2,
		Retention:         72 * time.Hour,
	}
	err = kafka.EnsureTopics(t.Context(), topicOptions)
	if err == nil || !strings.Contains(err.Error(), "exceeds 1 available brokers") {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
}

func topicConfig(resource kadm.ResourceConfig, name string) string {
	for _, configured := range resource.Configs {
		if configured.Key == name && configured.Value != nil {
			return *configured.Value
		}
	}
	return ""
}
