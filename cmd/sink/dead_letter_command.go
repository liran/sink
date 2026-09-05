package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/liran/sink/internal/queue"
	queuekafka "github.com/liran/sink/internal/queue/kafka"
)

type deadLetterReport struct {
	Topic        string            `json:"topic"`
	Partition    int32             `json:"partition"`
	Offset       int64             `json:"offset"`
	SHA256       string            `json:"sha256"`
	Headers      map[string]string `json:"headers"`
	ReplayStatus string            `json:"replay_status,omitempty"`
	Error        string            `json:"error,omitempty"`
}

func runDeadLetterCommand(args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "inspect" && args[0] != "replay") {
		return errors.New("usage: sink dlq inspect|replay --config FILE --store NAME --partition N --offset N --count N")
	}
	flags := flag.NewFlagSet("dlq "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "server configuration")
	store := flags.String("store", "", "configured store")
	partition := flags.Int("partition", -1, "exact dead-letter partition")
	offset := flags.Int64("offset", -1, "first dead-letter offset")
	count := flags.Int("count", 1, "consecutive records, at most 100")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *configPath == "" || *store == "" || *partition < 0 || *partition > 1<<31-1 || *offset < 0 || *count < 1 || *count > 100 {
		return errors.New("config, store, partition, offset, and a count between 1 and 100 are required")
	}
	loaded, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	var selected *backendConfig
	for index := range loaded.storages {
		if loaded.storages[index].name == *store {
			selected = &loaded.storages[index]
			break
		}
	}
	if selected == nil || !selected.kafka.enabled {
		return errors.New("selected store does not enable Kafka")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	selection := queuekafka.DeadLetterRange{Brokers: selected.kafka.brokers, Topic: selected.kafka.deadLetterTopic,
		Partition: int32(*partition), Offset: *offset, Count: *count}
	records, err := queuekafka.ReadDeadLetters(ctx, selection)
	if err != nil {
		return err
	}
	var replay queue.PublishResponse
	if args[0] == "replay" {
		mutations, err := queuekafka.ValidateDeadLetterReplay(records, *store)
		if err != nil {
			return err
		}
		opts := queuekafka.PublisherOptions{Brokers: selected.kafka.brokers, Topic: selected.kafka.topic,
			MaxRecordBytes: selected.kafka.maxRecordBytes, MaxBufferedBytes: selected.kafka.maxBufferedBytes}
		publisher, err := queuekafka.NewPublisher(opts)
		if err != nil {
			return err
		}
		defer publisher.Close()
		request := queue.PublishRequest{Mutations: mutations}
		replay, err = publisher.Publish(ctx, request)
		if err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(stdout)
	failed := false
	for index, record := range records {
		digest := sha256.Sum256(record.Value)
		report := deadLetterReport{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
			SHA256: hex.EncodeToString(digest[:]), Headers: make(map[string]string)}
		for _, header := range record.Headers {
			report.Headers[header.Key] = string(header.Value)
		}
		if args[0] == "replay" {
			report.ReplayStatus = "accepted"
			if result := replay.Results[index]; result.Status != queue.PublishStatusAccepted {
				failed = true
				report.ReplayStatus = "failed_or_unknown"
				report.Error = fmt.Sprint(result.Err)
			}
		}
		if err := encoder.Encode(report); err != nil {
			return err
		}
	}
	if failed {
		return errors.New("some replays failed or have unknown outcomes; inspect the JSON report before retrying")
	}
	return nil
}
