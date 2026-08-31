package mongodb

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const objectIDKeyType = "opaque:mongodb/object-id"

func mongoID(key storage.Key) (any, error) {
	switch key.Type {
	case "string":
		return string(key.Data), nil
	case "int64":
		if len(key.Data) != 8 {
			return nil, errors.New("int64 record key must contain 8 bytes")
		}
		value := int64(binary.BigEndian.Uint64(key.Data))
		return value, nil
	case "bytes":
		value := bson.Binary{Subtype: 0, Data: bytes.Clone(key.Data)}
		return value, nil
	case objectIDKeyType:
		var emptyObjectID bson.ObjectID
		if len(key.Data) != len(emptyObjectID) {
			return nil, errors.New("MongoDB ObjectID record key must contain 12 bytes")
		}
		var value bson.ObjectID
		copy(value[:], key.Data)
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported MongoDB record key type %q", key.Type)
	}
}

func rawValue(value any) (bson.RawValue, error) {
	valueType, encoded, err := bson.MarshalValue(value)
	if err != nil {
		var empty bson.RawValue
		return empty, err
	}
	raw := bson.RawValue{Type: valueType, Value: encoded}
	return raw, nil
}

func rawValueKey(value bson.RawValue) string {
	if value.Type == bson.TypeInt32 || value.Type == bson.TypeInt64 {
		integer, ok := value.AsInt64OK()
		if ok {
			normalized, err := rawValue(integer)
			if err == nil {
				value = normalized
			}
		}
	}
	key := make([]byte, 1, len(value.Value)+1)
	key[0] = byte(value.Type)
	key = append(key, value.Value...)
	return string(key)
}

func recordIDsEqual(actual bson.RawValue, expected bson.RawValue) bool {
	if actual.Equal(expected) {
		return true
	}
	actualInteger, actualOK := actual.AsInt64OK()
	expectedInteger, expectedOK := expected.AsInt64OK()
	return actualOK && expectedOK && actualInteger == expectedInteger
}

func newRevision() (storage.Revision, error) {
	data := make([]byte, 16)
	_, err := rand.Read(data)
	if err != nil {
		var empty storage.Revision
		return empty, fmt.Errorf("generate MongoDB record revision: %w", err)
	}
	revision := storage.Revision{Data: data}
	return revision, nil
}

func (s *Store) replacement(
	document storage.Document,
	id any,
	revision storage.Revision,
) (bson.Raw, error) {
	var empty bson.Raw
	if document.Encoding != storage.DocumentEncodingBSON {
		return empty, errors.New("MongoDB storage requires BSON document encoding")
	}
	if err := storage.ValidateDocument(document); err != nil {
		return empty, err
	}
	raw := bson.Raw(document.Payload)
	elements, err := raw.Elements()
	if err != nil {
		return empty, fmt.Errorf("read BSON document elements: %w", err)
	}
	expectedID, err := rawValue(id)
	if err != nil {
		return empty, fmt.Errorf("encode MongoDB record key: %w", err)
	}

	replacement := make(bson.D, 0, len(elements)+2)
	foundID := false
	for _, element := range elements {
		key := element.Key()
		if key == s.metadataField {
			return empty, fmt.Errorf("document field %q is reserved by Sink", s.metadataField)
		}
		if key == "_id" {
			if foundID {
				return empty, errors.New("document contains more than one _id field")
			}
			foundID = true
			if !recordIDsEqual(element.Value(), expectedID) {
				return empty, errors.New("document _id does not match the logical record key")
			}
		}
		field := bson.E{Key: key, Value: element.Value()}
		replacement = append(replacement, field)
	}
	if !foundID {
		idField := bson.E{Key: "_id", Value: id}
		idDocument := bson.D{idField}
		replacement = append(idDocument, replacement...)
	}
	metadata := bson.D{
		{Key: "revision", Value: bson.Binary{Subtype: 0, Data: bytes.Clone(revision.Data)}},
	}
	metadataField := bson.E{Key: s.metadataField, Value: metadata}
	replacement = append(replacement, metadataField)
	encoded, err := bson.Marshal(replacement)
	if err != nil {
		return empty, fmt.Errorf("encode MongoDB replacement document: %w", err)
	}
	return bson.Raw(encoded), nil
}

func (s *Store) userDocument(raw bson.Raw) (storage.Document, storage.Revision, error) {
	var emptyDocument storage.Document
	var emptyRevision storage.Revision
	if err := raw.Validate(); err != nil {
		return emptyDocument, emptyRevision, fmt.Errorf("validate stored BSON document: %w", err)
	}
	elements, err := raw.Elements()
	if err != nil {
		return emptyDocument, emptyRevision, fmt.Errorf("read stored BSON document elements: %w", err)
	}

	userFields := make(bson.D, 0, len(elements))
	revision := storage.Revision{}
	foundMetadata := false
	for _, element := range elements {
		if element.Key() != s.metadataField {
			field := bson.E{Key: element.Key(), Value: element.Value()}
			userFields = append(userFields, field)
			continue
		}
		if foundMetadata {
			return emptyDocument, emptyRevision, fmt.Errorf("stored document contains more than one %q field", s.metadataField)
		}
		foundMetadata = true
		metadata, ok := element.Value().DocumentOK()
		if !ok {
			return emptyDocument, emptyRevision, fmt.Errorf("stored document field %q is not a document", s.metadataField)
		}
		revisionValue, lookupErr := metadata.LookupErr("revision")
		if lookupErr != nil {
			return emptyDocument, emptyRevision, fmt.Errorf("stored document field %q has no revision", s.metadataField)
		}
		subtype, data, ok := revisionValue.BinaryOK()
		if !ok || subtype != 0 || len(data) == 0 {
			return emptyDocument, emptyRevision, fmt.Errorf("stored document field %q has an invalid revision", s.metadataField)
		}
		revision.Data = bytes.Clone(data)
	}

	encoded, err := bson.Marshal(userFields)
	if err != nil {
		return emptyDocument, emptyRevision, fmt.Errorf("encode user BSON document: %w", err)
	}
	document := storage.Document{
		Encoding: storage.DocumentEncodingBSON,
		Payload:  encoded,
	}
	return document, revision, nil
}
