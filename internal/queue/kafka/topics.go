package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topicPollInterval         = 250 * time.Millisecond
	maxTopicPartitions        = int(1<<31 - 1)
	maxTopicReplicationFactor = int(1<<15 - 1)
)

type TopicOptions struct {
	Brokers             []string
	Topics              []string
	Partitions          int
	ReplicationFactor   int
	Retention           time.Duration
	DeadLetterTopic     string
	DeadLetterRetention time.Duration
	MinInSyncReplicas   int
	MaxRecordBytes      int
}

func EnsureTopics(ctx context.Context, opts TopicOptions) error {
	if opts.DeadLetterRetention == 0 {
		opts.DeadLetterRetention = 30 * 24 * time.Hour
	}
	if opts.MinInSyncReplicas == 0 {
		opts.MinInSyncReplicas = min(2, opts.ReplicationFactor)
	}
	if opts.MaxRecordBytes == 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := validateTopicOptions(opts); err != nil {
		return err
	}
	clientOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return fmt.Errorf("create Kafka topic client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	return ensureTopics(ctx, admin, opts)
}

func validateTopicOptions(opts TopicOptions) error {
	if len(opts.Brokers) == 0 {
		return errors.New("configure Kafka topics: brokers are required")
	}
	if len(opts.Topics) == 0 {
		return errors.New("configure Kafka topics: at least one topic is required")
	}
	seen := make(map[string]struct{}, len(opts.Topics))
	for _, topic := range opts.Topics {
		if strings.TrimSpace(topic) == "" {
			return errors.New("configure Kafka topics: topic names cannot be empty")
		}
		if _, exists := seen[topic]; exists {
			return fmt.Errorf("configure Kafka topics: duplicate topic %q", topic)
		}
		seen[topic] = struct{}{}
	}
	if opts.Partitions <= 0 {
		return errors.New("configure Kafka topics: partitions must be positive")
	}
	if opts.Partitions > maxTopicPartitions {
		return fmt.Errorf("configure Kafka topics: partitions must not exceed %d", maxTopicPartitions)
	}
	if opts.ReplicationFactor <= 0 {
		return errors.New("configure Kafka topics: replication factor must be positive")
	}
	if opts.ReplicationFactor > maxTopicReplicationFactor {
		return fmt.Errorf("configure Kafka topics: replication factor must not exceed %d", maxTopicReplicationFactor)
	}
	if opts.MinInSyncReplicas < 1 || opts.MinInSyncReplicas > opts.ReplicationFactor {
		return errors.New("configure Kafka topics: min ISR must be between 1 and the replication factor")
	}
	if opts.MaxRecordBytes < 1 || opts.MaxRecordBytes > 64<<20 {
		return errors.New("configure Kafka topics: record limit must not exceed 64 MiB")
	}
	if opts.DeadLetterRetention < time.Millisecond {
		return errors.New("configure Kafka topics: dead-letter retention must be positive")
	}
	if opts.Retention < time.Millisecond {
		return errors.New("configure Kafka topics: retention must be at least one millisecond")
	}
	return nil
}

func ensureTopics(ctx context.Context, admin *kadm.Client, opts TopicOptions) error {
	brokerIDs, err := loadBrokerIDs(ctx, admin)
	if err != nil {
		return err
	}
	if opts.ReplicationFactor > len(brokerIDs) {
		return fmt.Errorf(
			"configure Kafka topics: replication factor %d exceeds %d available brokers",
			opts.ReplicationFactor,
			len(brokerIDs),
		)
	}

	details, err := loadTopicDetails(ctx, admin, opts.Topics)
	if err != nil {
		return err
	}
	missing := missingTopics(details, opts.Topics)
	if len(missing) > 0 {
		createErr := createTopics(ctx, admin, opts, missing)
		if createErr != nil {
			return createErr
		}
		details, err = waitForTopics(ctx, admin, opts.Topics)
		if err != nil {
			return err
		}
	}

	partitionErr := reconcilePartitionCounts(ctx, admin, opts, details)
	if partitionErr != nil {
		return partitionErr
	}
	replicationErr := reconcileReplicationFactors(ctx, admin, opts, brokerIDs)
	if replicationErr != nil {
		return replicationErr
	}
	if err := reconcileTopicPolicy(ctx, admin, opts); err != nil {
		return err
	}
	return verifyTopicPolicy(ctx, admin, opts)
}

func loadBrokerIDs(ctx context.Context, admin *kadm.Client) ([]int32, error) {
	brokers, err := admin.ListBrokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Kafka brokers: %w", err)
	}
	brokerIDs := make([]int32, 0, len(brokers))
	for _, broker := range brokers {
		brokerIDs = append(brokerIDs, broker.NodeID)
	}
	sort.Slice(brokerIDs, func(left int, right int) bool {
		return brokerIDs[left] < brokerIDs[right]
	})
	return brokerIDs, nil
}

func loadTopicDetails(ctx context.Context, admin *kadm.Client, topics []string) (kadm.TopicDetails, error) {
	details, err := admin.ListTopics(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("list Kafka topics: %w", err)
	}
	for _, topic := range topics {
		detail, exists := details[topic]
		if !exists || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		if detail.Err != nil {
			return nil, fmt.Errorf("describe Kafka topic %q: %w", topic, detail.Err)
		}
	}
	return details, nil
}

func missingTopics(details kadm.TopicDetails, topics []string) []string {
	missing := make([]string, 0, len(topics))
	for _, topic := range topics {
		if !details.Has(topic) {
			missing = append(missing, topic)
		}
	}
	return missing
}

func waitForTopics(ctx context.Context, admin *kadm.Client, topics []string) (kadm.TopicDetails, error) {
	for {
		details, err := loadTopicDetails(ctx, admin, topics)
		if err != nil {
			return nil, err
		}
		if len(missingTopics(details, topics)) == 0 {
			return details, nil
		}
		if err := waitForTopicPoll(ctx); err != nil {
			return nil, err
		}
	}
}

func createTopics(ctx context.Context, admin *kadm.Client, opts TopicOptions, topics []string) error {
	for _, topic := range topics {
		policy := topicPolicy(opts, topic)
		configs := make(map[string]*string, len(policy))
		for name, value := range policy {
			configs[name] = &value
		}
		response, err := admin.CreateTopic(ctx, int32(opts.Partitions), int16(opts.ReplicationFactor), configs, topic)
		if err != nil {
			return fmt.Errorf("create Kafka topic %q: %w", topic, err)
		}
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create Kafka topic %q: %w", topic, response.Err)
		}
	}
	return nil
}

func topicPolicy(opts TopicOptions, topic string) map[string]string {
	retention := opts.Retention
	if topic == opts.DeadLetterTopic {
		retention = opts.DeadLetterRetention
	}
	policy := map[string]string{
		"retention.ms":                   strconv.FormatInt(retention.Milliseconds(), 10),
		"min.insync.replicas":            strconv.Itoa(opts.MinInSyncReplicas),
		"cleanup.policy":                 "delete",
		"unclean.leader.election.enable": "false",
		"max.message.bytes":              strconv.Itoa(opts.MaxRecordBytes + kafkaRecordOverhead),
	}
	return policy
}

func reconcilePartitionCounts(_ context.Context, _ *kadm.Client, opts TopicOptions, details kadm.TopicDetails) error {
	for _, topic := range opts.Topics {
		partitions := len(details[topic].Partitions)
		if partitions != opts.Partitions {
			return fmt.Errorf("kafka topic %q has %d partitions, configured %d; online partition changes are refused to preserve record ordering; pause all publishers, drain consumers, and migrate the topic explicitly", topic, partitions, opts.Partitions)
		}
	}
	return nil
}

func reconcileReplicationFactors(
	ctx context.Context,
	admin *kadm.Client,
	opts TopicOptions,
	brokerIDs []int32,
) error {
	for {
		details, err := loadTopicDetails(ctx, admin, opts.Topics)
		if err != nil {
			return err
		}
		missing := missingTopics(details, opts.Topics)
		if len(missing) > 0 {
			return fmt.Errorf("configure Kafka topics: topics disappeared during replication-factor reconciliation: %s", strings.Join(missing, ", "))
		}
		assignments := make(kadm.AlterPartitionAssignmentsReq)
		assignmentCount := 0
		for _, topic := range opts.Topics {
			for partition, detail := range details[topic].Partitions {
				if len(detail.Replicas) == opts.ReplicationFactor {
					continue
				}
				replicas := desiredReplicas(partition, detail.Replicas, brokerIDs, opts.ReplicationFactor)
				assignments.Assign(topic, partition, replicas)
				assignmentCount++
			}
		}
		if len(assignments) == 0 {
			return nil
		}
		slog.Info(
			"reassigning Kafka topic replicas",
			"partitions", assignmentCount,
			"target_replication_factor", opts.ReplicationFactor,
		)

		responses, alterErr := admin.AlterPartitionAssignments(ctx, assignments)
		if alterErr != nil {
			return fmt.Errorf("alter Kafka topic replication factors: %w", alterErr)
		}
		responseErr := responses.Error()
		if responseErr == nil {
			waitErr := waitForReassignments(ctx, admin, details.TopicsSet())
			if waitErr != nil {
				return waitErr
			}
			return waitForReplicationFactors(ctx, admin, opts)
		}
		if !errors.Is(responseErr, kerr.ReassignmentInProgress) {
			return fmt.Errorf("alter Kafka topic replication factors: %w", responseErr)
		}
		waitErr := waitForReassignments(ctx, admin, details.TopicsSet())
		if waitErr != nil {
			return waitErr
		}
	}
}

func desiredReplicas(partition int32, current []int32, brokers []int32, count int) []int32 {
	available := make(map[int32]struct{}, len(brokers))
	for _, broker := range brokers {
		available[broker] = struct{}{}
	}
	selected := make(map[int32]struct{}, count)
	replicas := make([]int32, 0, count)
	for _, broker := range current {
		if _, exists := available[broker]; !exists {
			continue
		}
		if _, exists := selected[broker]; exists {
			continue
		}
		replicas = append(replicas, broker)
		selected[broker] = struct{}{}
		if len(replicas) == count {
			return replicas
		}
	}
	start := int(partition) % len(brokers)
	for offset := range len(brokers) {
		broker := brokers[(start+offset)%len(brokers)]
		if _, exists := selected[broker]; exists {
			continue
		}
		replicas = append(replicas, broker)
		selected[broker] = struct{}{}
		if len(replicas) == count {
			break
		}
	}
	return replicas
}

func waitForReassignments(ctx context.Context, admin *kadm.Client, topics kadm.TopicsSet) error {
	for {
		active, err := admin.ListPartitionReassignments(ctx, topics)
		if err != nil {
			return fmt.Errorf("list Kafka partition reassignments: %w", err)
		}
		if len(active.Sorted()) == 0 {
			return nil
		}
		if err := waitForTopicPoll(ctx); err != nil {
			return err
		}
	}
}

func waitForReplicationFactors(ctx context.Context, admin *kadm.Client, opts TopicOptions) error {
	for {
		details, err := loadTopicDetails(ctx, admin, opts.Topics)
		if err != nil {
			return err
		}
		missing := missingTopics(details, opts.Topics)
		if len(missing) > 0 {
			return fmt.Errorf("configure Kafka topics: topics disappeared while waiting for replication factors: %s", strings.Join(missing, ", "))
		}
		matched := true
		for _, topic := range opts.Topics {
			for _, partition := range details[topic].Partitions {
				if len(partition.Replicas) != opts.ReplicationFactor {
					matched = false
					break
				}
			}
			if !matched {
				break
			}
		}
		if matched {
			return nil
		}
		if err := waitForTopicPoll(ctx); err != nil {
			return err
		}
	}
}

func waitForTopicPoll(ctx context.Context) error {
	timer := time.NewTimer(topicPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("configure Kafka topics: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func reconcileTopicPolicy(ctx context.Context, admin *kadm.Client, opts TopicOptions) error {
	resources, err := admin.DescribeTopicConfigs(ctx, opts.Topics...)
	if err != nil {
		return fmt.Errorf("describe Kafka topic policy: %w", err)
	}
	for _, topic := range opts.Topics {
		resource, err := resources.On(topic, nil)
		if err != nil {
			return err
		}
		current := make(map[string]string)
		for _, config := range resource.Configs {
			if config.Value != nil {
				current[config.Key] = *config.Value
			}
		}
		changes := make([]kadm.AlterConfig, 0)
		for name, value := range topicPolicy(opts, topic) {
			if current[name] == value {
				continue
			}
			change := kadm.AlterConfig{Name: name, Value: &value, Op: kadm.SetConfig}
			changes = append(changes, change)
		}
		if len(changes) == 0 {
			continue
		}
		responses, err := admin.AlterTopicConfigs(ctx, changes, topic)
		if err != nil {
			return fmt.Errorf("configure Kafka topic %q: %w", topic, err)
		}
		if response, err := responses.On(topic, nil); err != nil {
			return err
		} else if response.Err != nil {
			return response.Err
		}
	}
	return nil
}

func verifyTopicPolicy(ctx context.Context, admin *kadm.Client, opts TopicOptions) error {
	for {
		resources, err := admin.DescribeTopicConfigs(ctx, opts.Topics...)
		if err != nil {
			return err
		}
		matched := true
		for _, topic := range opts.Topics {
			resource, err := resources.On(topic, nil)
			if err != nil {
				return err
			}
			current := make(map[string]string)
			for _, configured := range resource.Configs {
				if configured.Value != nil {
					current[configured.Key] = *configured.Value
				}
			}
			for key, expected := range topicPolicy(opts, topic) {
				if current[key] != expected {
					matched = false
				}
			}
		}
		if matched {
			return nil
		}
		if err := waitForTopicPoll(ctx); err != nil {
			return err
		}
	}
}
