package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultAddress = "sink:8080"
)

type exampleRecord struct {
	key      string
	document demoDocument
}

type demoDocument struct {
	ID     string   `bson:"_id,omitempty"`
	Name   string   `bson:"name"`
	Stage  string   `bson:"stage"`
	Labels []string `bson:"labels"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatalf("quickstart failed: %v", err)
	}
}

func run() error {
	address := os.Getenv("SINK_ADDRESS")
	if address == "" {
		address = defaultAddress
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	credentials := insecure.NewCredentials()
	transportOption := grpc.WithTransportCredentials(credentials)
	connection, err := grpc.NewClient(address, transportOption)
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer connection.Close()

	healthClient := healthpb.NewHealthClient(connection)
	if err := waitForSink(ctx, healthClient); err != nil {
		return err
	}
	fmt.Println("PASS Sink gRPC health check is serving")

	client := sink.NewSinkClient(connection)
	return runScenario(ctx, client)
}

func waitForSink(ctx context.Context, client healthpb.HealthClient) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	request := &healthpb.HealthCheckRequest{}
	var lastErr error
	for {
		attemptContext, cancel := context.WithTimeout(ctx, time.Second)
		response, err := client.Check(attemptContext, request)
		cancel()
		if err == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("health status is %s", response.GetStatus())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Sink: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func runScenario(ctx context.Context, client sink.SinkClient) error {
	synchronous := []exampleRecord{
		{
			key: "sync-alpha",
			document: demoDocument{
				Name:   "alpha",
				Stage:  "synchronous",
				Labels: []string{"batch", "write"},
			},
		},
		{
			key: "sync-beta",
			document: demoDocument{
				Name:   "beta",
				Stage:  "synchronous",
				Labels: []string{"batch", "read"},
			},
		},
	}
	if err := writeRecords(ctx, client, synchronous, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED); err != nil {
		return err
	}
	fmt.Println("PASS batch synchronous write applied 2 records")
	if err := readAndVerify(ctx, client, synchronous); err != nil {
		return err
	}
	fmt.Println("PASS batch read returned both BSON documents")

	asynchronous := exampleRecord{
		key: "async-gamma",
		document: demoDocument{
			Name:   "gamma",
			Stage:  "asynchronous",
			Labels: []string{"kafka", "worker"},
		},
	}
	asyncRecords := []exampleRecord{asynchronous}
	if err := writeRecords(ctx, client, asyncRecords, sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED); err != nil {
		return err
	}
	fmt.Println("PASS asynchronous write accepted by Kafka")
	if err := waitForRecord(ctx, client, asynchronous); err != nil {
		return err
	}
	fmt.Println("PASS Kafka worker applied the asynchronous write")

	allRecords := append(synchronous, asynchronous)
	if err := deleteRecords(ctx, client, allRecords); err != nil {
		return err
	}
	fmt.Println("PASS batch hard delete applied 3 records")
	if err := verifyMissing(ctx, client, allRecords); err != nil {
		return err
	}
	fmt.Println("PASS deleted records are no longer readable")
	fmt.Println("Quickstart completed successfully; Sink remains available at localhost:8080")
	return nil
}

func writeRecords(
	ctx context.Context,
	client sink.SinkClient,
	records []exampleRecord,
	completionMode sink.CompletionMode,
) error {
	operations := make([]*sink.WriteOperation, 0, len(records))
	for _, record := range records {
		document, err := encodeDocument(record.document)
		if err != nil {
			return err
		}
		put := &sink.PutOperation{
			Document: document,
			Mode:     sink.WriteMode_WRITE_MODE_UPSERT,
		}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{
			Address: recordAddress(record.key),
			Action:  action,
		}
		operations = append(operations, operation)
	}
	request := &sink.WriteRequest{
		CompletionMode: completionMode,
		Operations:     operations,
	}
	response, err := client.Write(ctx, request)
	if err != nil {
		return fmt.Errorf("write records: %w", err)
	}
	if len(response.GetResults()) != len(records) {
		return fmt.Errorf("write returned %d results for %d records", len(response.GetResults()), len(records))
	}
	expected := sink.WriteStatus_WRITE_STATUS_APPLIED
	if completionMode == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		expected = sink.WriteStatus_WRITE_STATUS_ACCEPTED
	}
	for _, result := range response.GetResults() {
		if result.GetStatus() != expected {
			return fmt.Errorf("write operation %d returned %s: %s", result.GetOperationIndex(), result.GetStatus(), failureMessage(result.GetFailure()))
		}
	}
	return nil
}

func readAndVerify(ctx context.Context, client sink.SinkClient, records []exampleRecord) error {
	request := readRequest(records)
	response, err := client.Read(ctx, request)
	if err != nil {
		return fmt.Errorf("read records: %w", err)
	}
	if len(response.GetResults()) != len(records) {
		return fmt.Errorf("read returned %d results for %d records", len(response.GetResults()), len(records))
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
			return fmt.Errorf("read operation %d returned %s: %s", result.GetOperationIndex(), result.GetStatus(), failureMessage(result.GetFailure()))
		}
		if err := verifyDocument(result.GetDocument(), records[index]); err != nil {
			return err
		}
	}
	return nil
}

func waitForRecord(ctx context.Context, client sink.SinkClient, record exampleRecord) error {
	pollContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	records := []exampleRecord{record}
	request := readRequest(records)
	for {
		response, err := client.Read(pollContext, request)
		if err != nil {
			return fmt.Errorf("poll asynchronous record: %w", err)
		}
		if len(response.GetResults()) != 1 {
			return fmt.Errorf("poll returned %d results", len(response.GetResults()))
		}
		result := response.GetResults()[0]
		if result.GetStatus() == sink.ReadStatus_READ_STATUS_FOUND {
			return verifyDocument(result.GetDocument(), record)
		}
		if result.GetStatus() == sink.ReadStatus_READ_STATUS_FAILED {
			return fmt.Errorf("poll failed: %s", failureMessage(result.GetFailure()))
		}
		select {
		case <-pollContext.Done():
			return fmt.Errorf("wait for asynchronous write: %w", pollContext.Err())
		case <-ticker.C:
		}
	}
}

func deleteRecords(ctx context.Context, client sink.SinkClient, records []exampleRecord) error {
	operations := make([]*sink.DeleteOperation, 0, len(records))
	for _, record := range records {
		operation := &sink.DeleteOperation{Address: recordAddress(record.key)}
		operations = append(operations, operation)
	}
	request := &sink.DeleteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		Operations:     operations,
	}
	response, err := client.Delete(ctx, request)
	if err != nil {
		return fmt.Errorf("delete records: %w", err)
	}
	if len(response.GetResults()) != len(records) {
		return fmt.Errorf("delete returned %d results for %d records", len(response.GetResults()), len(records))
	}
	for _, result := range response.GetResults() {
		if result.GetStatus() != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			return fmt.Errorf("delete operation %d returned %s: %s", result.GetOperationIndex(), result.GetStatus(), failureMessage(result.GetFailure()))
		}
	}
	return nil
}

func verifyMissing(ctx context.Context, client sink.SinkClient, records []exampleRecord) error {
	request := readRequest(records)
	response, err := client.Read(ctx, request)
	if err != nil {
		return fmt.Errorf("read deleted records: %w", err)
	}
	if len(response.GetResults()) != len(records) {
		return fmt.Errorf("deleted-record read returned %d results for %d records", len(response.GetResults()), len(records))
	}
	for _, result := range response.GetResults() {
		if result.GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
			return fmt.Errorf("deleted record operation %d returned %s", result.GetOperationIndex(), result.GetStatus())
		}
	}
	return nil
}

func readRequest(records []exampleRecord) *sink.ReadRequest {
	operations := make([]*sink.ReadOperation, 0, len(records))
	for _, record := range records {
		operation := &sink.ReadOperation{Address: recordAddress(record.key)}
		operations = append(operations, operation)
	}
	request := &sink.ReadRequest{Operations: operations}
	return request
}

func recordAddress(key string) *sink.RecordAddress {
	keyKind := &sink.RecordKey_StringValue{StringValue: key}
	recordKey := &sink.RecordKey{Kind: keyKind}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "quickstart",
		Dataset:   "records",
		Key:       recordKey,
	}
	return address
}

func encodeDocument(document demoDocument) (*sink.Document, error) {
	data, err := bson.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode BSON document: %w", err)
	}
	encoded := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON,
		Payload:  data,
	}
	return encoded, nil
}

func verifyDocument(document *sink.Document, expected exampleRecord) error {
	if document == nil {
		return fmt.Errorf("record %q returned an invalid document", expected.key)
	}
	var actual demoDocument
	if document.GetEncoding() != sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
		return fmt.Errorf("record %q returned %s encoding", expected.key, document.GetEncoding())
	}
	if err := bson.Unmarshal(document.GetPayload(), &actual); err != nil {
		return fmt.Errorf("decode record %q: %w", expected.key, err)
	}
	if actual.ID != expected.key {
		return fmt.Errorf("record %q returned logical key %q", expected.key, actual.ID)
	}
	actual.ID = ""
	if !reflect.DeepEqual(actual, expected.document) {
		return fmt.Errorf("record %q returned %+v, expected %+v", expected.key, actual, expected.document)
	}
	return nil
}

func failureMessage(failure *sink.Failure) string {
	if failure == nil {
		return "no failure details"
	}
	return failure.GetMessage()
}
