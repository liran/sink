package worker_test

import (
	"context"
	"errors"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"github.com/liran/sink/internal/worker"
)

func TestProcessorAppliesWriteAndDeleteSynchronously(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	serverOptions := service.Options{Storage: store, Lua: luaEngine}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	address := processorAddress()
	document := &sink.Document{ContentType: "text/plain", Data: []byte("value")}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_CREATE}
	writeOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Put{Put: put},
	}
	writeMutation := queue.Mutation{Write: writeOperation}
	if err := processor.Handle(context.Background(), writeMutation); err != nil {
		t.Fatalf("Handle(write) error = %v", err)
	}

	readRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddress()}},
	}
	read, err := store.Read(context.Background(), readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusFound {
		t.Fatalf("Read() status = %v", read.Results[0].Status)
	}

	duplicateErr := processor.Handle(context.Background(), writeMutation)
	var applyError *worker.ApplyError
	if !errors.As(duplicateErr, &applyError) || applyError.Retryable() {
		t.Fatalf("Handle(duplicate create) error = %v", duplicateErr)
	}

	deleteOperation := &sink.DeleteOperation{Address: address}
	deleteMutation := queue.Mutation{Delete: deleteOperation}
	if err := processor.Handle(context.Background(), deleteMutation); err != nil {
		t.Fatalf("Handle(delete) error = %v", err)
	}
	read, err = store.Read(context.Background(), readRequest)
	if err != nil {
		t.Fatalf("Read(after delete) error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read(after delete) status = %v", read.Results[0].Status)
	}
}

func TestProcessorBatchPreservesMixedMutationOrderPerRecord(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	serverOptions := service.Options{Storage: store, Lua: luaEngine}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	firstAddress := processorAddressFor("first")
	secondAddress := processorAddressFor("second")
	mutations := []queue.Mutation{
		{Write: processorPut(firstAddress, "before")},
		{Write: processorPut(secondAddress, "independent")},
		{Delete: &sink.DeleteOperation{Address: firstAddress}},
		{Write: processorPut(firstAddress, "after")},
	}
	results := processor.HandleBatch(context.Background(), mutations)
	for index, result := range results {
		if result != nil {
			t.Fatalf("HandleBatch() result[%d] = %v", index, result)
		}
	}

	firstRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddressFor("first")}},
	}
	firstRead, err := store.Read(context.Background(), firstRequest)
	if err != nil {
		t.Fatalf("Read(first) error = %v", err)
	}
	if got := string(firstRead.Results[0].Document.Data); got != "after" {
		t.Fatalf("Read(first) document = %q, want after", got)
	}
	secondRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddressFor("second")}},
	}
	secondRead, err := store.Read(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("Read(second) error = %v", err)
	}
	if got := string(secondRead.Results[0].Document.Data); got != "independent" {
		t.Fatalf("Read(second) document = %q, want independent", got)
	}
}

func processorAddress() *sink.RecordAddress {
	return processorAddressFor("record-1")
}

func processorAddressFor(value string) *sink.RecordAddress {
	key := &sink.RecordKey{Kind: &sink.RecordKey_StringValue{StringValue: value}}
	address := &sink.RecordAddress{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       key,
	}
	return address
}

func processorStorageAddress() storage.Address {
	return processorStorageAddressFor("record-1")
}

func processorStorageAddressFor(value string) storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "logical",
		Dataset:   "records",
		Key:       storage.Key{Type: "string", Data: []byte(value)},
	}
	return address
}

func processorPut(address *sink.RecordAddress, value string) *sink.WriteOperation {
	document := &sink.Document{ContentType: "text/plain", Data: []byte(value)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	return operation
}
