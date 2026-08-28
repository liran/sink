package memory_test

import (
	"context"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestStoreRevisionPrecondition(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	address := testAddress()
	documentA := testDocument("a")

	create := storage.WriteOperation{
		Address:  address,
		Document: documentA,
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRecordNotExists,
		},
	}
	createRequest := storage.WriteRequest{Operations: []storage.WriteOperation{create}}
	createResponse, err := store.Write(ctx, createRequest)
	if err != nil {
		t.Fatalf("Write(create) error = %v", err)
	}
	if createResponse.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(create) status = %v", createResponse.Results[0].Status)
	}

	documentB := testDocument("b")
	matchingUpdate := storage.WriteOperation{
		Address:  address,
		Document: documentB,
		Precondition: storage.Precondition{
			Kind:     storage.PreconditionRevisionMatches,
			Revision: createResponse.Results[0].Revision,
		},
	}
	matchingRequest := storage.WriteRequest{Operations: []storage.WriteOperation{matchingUpdate}}
	matchingResponse, err := store.Write(ctx, matchingRequest)
	if err != nil {
		t.Fatalf("Write(matching revision) error = %v", err)
	}
	if matchingResponse.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write(matching revision) status = %v", matchingResponse.Results[0].Status)
	}

	staleUpdate := matchingUpdate
	staleUpdate.Document = testDocument("stale")
	staleRequest := storage.WriteRequest{Operations: []storage.WriteOperation{staleUpdate}}
	staleResponse, err := store.Write(ctx, staleRequest)
	if err != nil {
		t.Fatalf("Write(stale revision) error = %v", err)
	}
	if staleResponse.Results[0].Status != storage.WriteStatusPreconditionFailed {
		t.Fatalf("Write(stale revision) status = %v, want precondition failed", staleResponse.Results[0].Status)
	}
}

func TestStoreUpgradesLegacyRecordWithAbsentRevision(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	address := testAddress()
	seed := memory.SeedRequest{
		Address:  address,
		Document: testDocument("legacy"),
	}
	store.Seed(seed)

	operation := storage.WriteOperation{
		Address:  address,
		Document: testDocument("upgraded"),
		Precondition: storage.Precondition{
			Kind: storage.PreconditionRevisionAbsent,
		},
	}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
	response, err := store.Write(ctx, request)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if response.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("Write() status = %v", response.Results[0].Status)
	}
	if len(response.Results[0].Revision.Data) == 0 {
		t.Fatal("Write() returned an empty revision")
	}
}

func testAddress() storage.Address {
	address := storage.Address{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   "products",
		Key: storage.Key{
			Type: "string",
			Data: []byte("record-1"),
		},
	}
	return address
}

func testDocument(value string) storage.Document {
	document := storage.Document{
		ContentType: "text/plain",
		Data:        []byte(value),
	}
	return document
}
