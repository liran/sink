package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type DeadLetterRange struct {
	Brokers   []string
	Topic     string
	Partition int32
	Offset    int64
	Count     int
}

// ReadDeadLetters uses explicit positions and never joins or advances a group.
// Missing/expired offsets fail instead of silently inspecting a different range.
func ReadDeadLetters(ctx context.Context, selection DeadLetterRange) ([]*kgo.Record, error) {
	if len(selection.Brokers) == 0 || selection.Topic == "" || selection.Partition < 0 || selection.Offset < 0 || selection.Count < 1 || selection.Count > 100 {
		return nil, errors.New("dead-letter selection requires brokers, topic, nonnegative partition/offset, and count between 1 and 100")
	}
	partitions := map[int32]kgo.Offset{selection.Partition: kgo.NoResetOffset().At(selection.Offset)}
	topics := map[string]map[int32]kgo.Offset{selection.Topic: partitions}
	opts := []kgo.Opt{kgo.SeedBrokers(selection.Brokers...), kgo.ConsumePartitions(topics), kgo.FetchMaxBytes(8 << 20)}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	starts, err := admin.ListStartOffsets(ctx, selection.Topic)
	if err != nil {
		return nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, selection.Topic)
	if err != nil {
		return nil, err
	}
	start, exists := starts[selection.Topic][selection.Partition]
	end := ends[selection.Topic][selection.Partition]
	if !exists || start.Err != nil || end.Err != nil || selection.Offset < start.Offset || selection.Offset >= end.Offset || int64(selection.Count) > end.Offset-selection.Offset {
		return nil, errors.New("requested dead-letter range is unavailable or expired")
	}
	records := make([]*kgo.Record, 0, selection.Count)
	bytes := 0
	for len(records) < selection.Count {
		fetches := client.PollRecords(ctx, selection.Count-len(records))
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		for _, record := range fetches.Records() {
			if record.Offset != selection.Offset+int64(len(records)) {
				return nil, errors.New("dead-letter range contains a missing offset")
			}
			bytes += len(record.Key) + len(record.Value)
			if bytes > 64<<20 {
				return nil, errors.New("dead-letter selection exceeds 64 MiB; select fewer records")
			}
			records = append(records, record)
		}
	}
	return records, nil
}

// ValidateDeadLetterReplay validates the entire selection before the caller
// publishes anything. It preserves the business operation, including its Lua.
func ValidateDeadLetterReplay(records []*kgo.Record, store string) ([]queue.Mutation, error) {
	mutations := make([]queue.Mutation, 0, len(records))
	for _, record := range records {
		mutation, err := queue.UnmarshalMutation(record.Value)
		if err != nil {
			return nil, fmt.Errorf("dead letter %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		owner, err := queue.MutationStore(mutation)
		if err != nil {
			return nil, err
		}
		if owner != store {
			return nil, fmt.Errorf("dead letter targets store %q, selected %q", owner, store)
		}
		mutations = append(mutations, mutation)
	}
	return mutations, nil
}
