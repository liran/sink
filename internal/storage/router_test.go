package storage_test

import (
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestRouterRoutesMixedStoreBatch(t *testing.T) {
	mongoStore := memory.New()
	searchStore := memory.New()
	backends := map[string]storage.Storage{
		"mongo-main":  mongoStore,
		"search-main": searchStore,
	}
	router, err := storage.NewRouter(backends)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	mongoAddress := routerTestAddress("mongo-main", "mongo-record")
	searchAddress := routerTestAddress("search-main", "search-record")
	mongoDocument := storage.Document{ContentType: "application/bson", Data: []byte("mongo")}
	searchDocument := storage.Document{ContentType: "application/json", Data: []byte("search")}
	writeOperations := []storage.WriteOperation{
		{Address: searchAddress, Document: searchDocument},
		{Address: mongoAddress, Document: mongoDocument},
	}
	writeRequest := storage.WriteRequest{Operations: writeOperations}
	written, err := router.Write(t.Context(), writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.Results[0].Status != storage.WriteStatusApplied || written.Results[1].Status != storage.WriteStatusApplied {
		t.Fatalf("Write() results = %#v", written.Results)
	}

	readOperations := []storage.ReadOperation{
		{Address: mongoAddress},
		{Address: searchAddress},
	}
	readRequest := storage.ReadRequest{Operations: readOperations}
	read, err := router.Read(t.Context(), readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(read.Results[0].Document.Data) != "mongo" || string(read.Results[1].Document.Data) != "search" {
		t.Fatalf("Read() results = %#v", read.Results)
	}

	mongoRead := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: searchAddress}}}
	mongoResult, err := mongoStore.Read(t.Context(), mongoRead)
	if err != nil {
		t.Fatalf("mongoStore.Read() error = %v", err)
	}
	if mongoResult.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("mongoStore.Read() status = %v", mongoResult.Results[0].Status)
	}

	deleteOperations := []storage.DeleteOperation{
		{Address: searchAddress},
		{Address: mongoAddress},
	}
	deleteRequest := storage.DeleteRequest{Operations: deleteOperations}
	deleted, err := router.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.Results[0].Status != storage.DeleteStatusApplied || deleted.Results[1].Status != storage.DeleteStatusApplied {
		t.Fatalf("Delete() results = %#v", deleted.Results)
	}
}

func TestRouterReturnsPerOperationFailuresForUnknownStorage(t *testing.T) {
	backends := map[string]storage.Storage{"primary": memory.New()}
	router, err := storage.NewRouter(backends)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	address := routerTestAddress("missing", "record")

	readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: address}}}
	read, err := router.Read(t.Context(), readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusFailed || read.Results[0].Err == nil {
		t.Fatalf("Read() result = %#v", read.Results[0])
	}

	writeOperation := storage.WriteOperation{Address: address, Document: storage.Document{ContentType: "application/json", Data: []byte("{}")}}
	writeRequest := storage.WriteRequest{Operations: []storage.WriteOperation{writeOperation}}
	written, err := router.Write(t.Context(), writeRequest)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.Results[0].Status != storage.WriteStatusFailed || written.Results[0].Err == nil {
		t.Fatalf("Write() result = %#v", written.Results[0])
	}

	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{{Address: address}}}
	deleted, err := router.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.Results[0].Status != storage.DeleteStatusFailed || deleted.Results[0].Err == nil {
		t.Fatalf("Delete() result = %#v", deleted.Results[0])
	}
}

func TestNewRouterRequiresBackends(t *testing.T) {
	_, err := storage.NewRouter(nil)
	if err == nil {
		t.Fatal("NewRouter() error = nil")
	}
}

func routerTestAddress(store string, key string) storage.Address {
	address := storage.Address{
		Store:     store,
		Namespace: "logical",
		Dataset:   "records",
		Key:       storage.Key{Type: "string", Data: []byte(key)},
	}
	return address
}
