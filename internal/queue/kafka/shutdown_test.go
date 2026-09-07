package kafka

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestWorkerShutdownBoundsLostLeaveGroupResponse(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "shutdown", "shutdown.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	handler := &outageHandler{}
	opts := WorkerOptions{Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "shutdown", GroupID: "shutdown-workers",
		DeadLetterTopic: "shutdown.dlq", Handler: handler, ShutdownTimeout: 100 * time.Millisecond}
	worker, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{}`)
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "shutdown", Value: payload}
	if err := worker.client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	running := make(chan error, 1)
	go func() { running <- worker.Run(ctx) }()
	waitRecovery(t, func() bool { return handler.calls.Load() > 0 })
	cancel()
	select {
	case err := <-running:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish processing before close")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	cluster.ControlKey(int16(kmsg.LeaveGroup), func(request kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	closed := make(chan struct{})
	go func() { worker.Close(); close(closed) }()
	defer func() { close(release); <-closed }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not reach the LeaveGroup response boundary")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("worker shutdown waited beyond its configured deadline for a lost LeaveGroup response")
	}
}
