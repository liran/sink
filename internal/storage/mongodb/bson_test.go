package mongodb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestReplacementPreservesShapeAndAddsRevision(t *testing.T) {
	store := &Store{metadataField: defaultMetadataField}
	document := storage.Document{JSON: []byte(`{"name":"legacy","tags":["a","b"]}`)}
	revision := storage.Revision{Data: []byte("revision-1")}
	replacement, err := store.replacement(document, "record-1", revision)
	if err != nil {
		t.Fatalf("replacement() error = %v", err)
	}

	if got := replacement.Lookup("_id").StringValue(); got != "record-1" {
		t.Fatalf("replacement _id = %q", got)
	}
	metadata, ok := replacement.Lookup(defaultMetadataField).DocumentOK()
	if !ok {
		t.Fatal("replacement metadata is not a document")
	}
	_, revisionData, ok := metadata.Lookup("revision").BinaryOK()
	if !ok || !bytes.Equal(revisionData, revision.Data) {
		t.Fatalf("replacement revision = %x", revisionData)
	}

	decoded, decodedRevision, err := store.userDocument(replacement)
	if err != nil {
		t.Fatalf("userDocument() error = %v", err)
	}
	if !bytes.Equal(decodedRevision.Data, revision.Data) {
		t.Fatalf("decoded revision = %x", decodedRevision.Data)
	}
	var user map[string]any
	if err := json.Unmarshal(decoded.JSON, &user); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := user["name"]; got != "legacy" {
		t.Fatalf("decoded name = %#v", got)
	}
	if _, exists := user[defaultMetadataField]; exists {
		t.Fatal("decoded user document contains Sink metadata")
	}
}

func TestUserDocumentAcceptsLegacyDocumentWithoutRevision(t *testing.T) {
	store := &Store{metadataField: defaultMetadataField}
	legacyValue := bson.D{
		{Key: "_id", Value: "legacy-1"},
		{Key: "value", Value: int32(42)},
	}
	legacy, err := bson.Marshal(legacyValue)
	if err != nil {
		t.Fatalf("bson.Marshal() error = %v", err)
	}
	document, revision, err := store.userDocument(legacy)
	if err != nil {
		t.Fatalf("userDocument() error = %v", err)
	}
	if !json.Valid(document.JSON) {
		t.Fatalf("document JSON = %q", document.JSON)
	}
	if len(revision.Data) != 0 {
		t.Fatalf("legacy revision = %x, want empty", revision.Data)
	}
}

func TestReplacementRejectsReservedFieldAndMismatchedID(t *testing.T) {
	store := &Store{metadataField: defaultMetadataField}
	revision := storage.Revision{Data: []byte("revision")}

	reservedDocument := storage.Document{JSON: []byte(`{"__sink":{}}`)}
	_, err := store.replacement(reservedDocument, "record-1", revision)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("replacement(reserved) error = %v", err)
	}

	mismatchedDocument := storage.Document{JSON: []byte(`{"_id":"other"}`)}
	_, err = store.replacement(mismatchedDocument, "record-1", revision)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("replacement(mismatched) error = %v", err)
	}
}

func TestReplacementAcceptsEquivalentInt32ID(t *testing.T) {
	store := &Store{metadataField: defaultMetadataField}
	document := storage.Document{JSON: []byte(`{"_id":{"$numberInt":"42"}}`)}
	revision := storage.Revision{Data: []byte("revision")}
	_, err := store.replacement(document, int64(42), revision)
	if err != nil {
		t.Fatalf("replacement() error = %v", err)
	}
}

func TestMongoIDSupportsObjectID(t *testing.T) {
	want := bson.NewObjectID()
	key := storage.Key{Type: objectIDKeyType, Data: bytes.Clone(want[:])}
	got, err := mongoID(key)
	if err != nil {
		t.Fatalf("mongoID() error = %v", err)
	}
	objectID, ok := got.(bson.ObjectID)
	if !ok || objectID != want {
		t.Fatalf("mongoID() = %#v, want %s", got, want)
	}
}

func TestMongoIDDecodesInt64Key(t *testing.T) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(42))
	key := storage.Key{Type: "int64", Data: data}
	got, err := mongoID(key)
	if err != nil {
		t.Fatalf("mongoID() error = %v", err)
	}
	if got != int64(42) {
		t.Fatalf("mongoID() = %#v", got)
	}
}
