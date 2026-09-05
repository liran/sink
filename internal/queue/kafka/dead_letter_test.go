package kafka

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDeadLetterFailureDoesNotCommitSource(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source", "source.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	handler := &retryHandler{}
	opts := WorkerOptions{Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "source", GroupID: "dlq-failure", DeadLetterTopic: "source.dlq", Handler: handler}
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	record := &kgo.Record{Topic: "source", Value: []byte("invalid envelope")}
	if err := w.client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		produce := request.(*kmsg.ProduceRequest)
		response := kmsg.NewPtrProduceResponse()
		response.SetVersion(produce.GetVersion())
		for _, topic := range produce.Topics {
			result := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
			for _, partition := range topic.Partitions {
				failure := kmsg.ProduceResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.TopicAuthorizationFailed.Code}
				result.Partitions = append(result.Partitions, failure)
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil, true
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fetches := w.client.PollRecords(ctx, 1)
	if len(fetches.Records()) != 1 {
		t.Fatalf("fetches: %v", fetches.Errors())
	}
	retained, err := w.handleFetches(ctx, fetches)
	if err == nil || len(retained) != 1 {
		t.Fatal("failed DLQ publish did not retain source")
	}
	admin := kadm.NewClient(w.client)
	committed, err := admin.FetchOffsets(ctx, "dlq-failure")
	if err != nil {
		t.Fatal(err)
	}
	if committed["source"][0].At > 0 {
		t.Fatal("source was committed without DLQ acknowledgement")
	}
	// The injected authorization failure is one-shot. Successful retry must
	// persist the original envelope before allowing the source offset forward.
	if _, err := w.handleFetches(ctx, fetches); err != nil {
		t.Fatal(err)
	}
	committed, err = admin.FetchOffsets(ctx, "dlq-failure")
	if err != nil || committed["source"][0].At != 1 {
		t.Fatalf("commit after recovery: %v %v", committed, err)
	}
	selection := DeadLetterRange{Brokers: cluster.ListenAddrs(), Topic: "source.dlq", Partition: 0, Offset: 0, Count: 1}
	letters, err := ReadDeadLetters(ctx, selection)
	if err != nil {
		t.Fatal(err)
	}
	if string(letters[0].Value) != "invalid envelope" {
		t.Fatal("DLQ did not preserve original envelope")
	}
	if _, err := ValidateDeadLetterReplay(letters, "primary"); err == nil {
		t.Fatal("malformed DLQ was replayable")
	}
}

func TestDeadLetterInspectionAndReplayValidation(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "inspect.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	opts := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{}`)
	value, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "inspect.dlq", Value: value}
	if err := client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	selection := DeadLetterRange{Brokers: cluster.ListenAddrs(), Topic: "inspect.dlq", Partition: 0, Offset: 0, Count: 1}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	letters, err := ReadDeadLetters(ctx, selection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateDeadLetterReplay(letters, "wrong-store"); err == nil {
		t.Fatal("cross-store replay was allowed")
	}
	mutations, err := ValidateDeadLetterReplay(letters, "primary")
	if err != nil || len(mutations) != 1 {
		t.Fatalf("valid replay: %v", err)
	}
	selection.Offset = 1
	if _, err := ReadDeadLetters(ctx, selection); err == nil {
		t.Fatal("unavailable offset was silently replaced")
	}
}
