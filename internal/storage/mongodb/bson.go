package mongodb

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

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
	if _, err := storage.DecodeDateTimeValues(document); err != nil {
		return empty, fmt.Errorf("validate JSON document date-time metadata: %w", err)
	}
	var decoded bson.D
	if err := bson.UnmarshalExtJSON(document.JSON, false, &decoded); err != nil {
		return empty, fmt.Errorf("decode JSON document for MongoDB: %w", err)
	}
	if err := applyDateTimePaths(decoded, document.DateTimePaths); err != nil {
		return empty, fmt.Errorf("apply MongoDB date-time values: %w", err)
	}
	encoded, err := bson.Marshal(decoded)
	if err != nil {
		return empty, fmt.Errorf("encode MongoDB BSON document: %w", err)
	}
	raw := bson.Raw(encoded)
	if err := raw.Validate(); err != nil {
		return empty, fmt.Errorf("validate BSON document: %w", err)
	}
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
	encoded, err = bson.Marshal(replacement)
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

	document, err := jsonDocumentFromBSON(userFields)
	if err != nil {
		return emptyDocument, emptyRevision, fmt.Errorf("encode user JSON document: %w", err)
	}
	return document, revision, nil
}

func applyDateTimePaths(document bson.D, paths []string) error {
	for _, rawPath := range paths {
		pointer := jsontext.Pointer(rawPath)
		tokens := make([]string, 0)
		for token := range pointer.Tokens() {
			tokens = append(tokens, token)
		}
		if len(tokens) == 0 {
			return fmt.Errorf("date-time path %q identifies the document root", rawPath)
		}
		if err := applyDateTimePath(document, tokens); err != nil {
			return fmt.Errorf("date-time path %q: %w", rawPath, err)
		}
	}
	return nil
}

func applyDateTimePath(value any, tokens []string) error {
	token := tokens[0]
	switch typed := value.(type) {
	case bson.D:
		for index := range typed {
			if typed[index].Key != token {
				continue
			}
			if len(tokens) > 1 {
				return applyDateTimePath(typed[index].Value, tokens[1:])
			}
			converted, err := bsonDateTime(typed[index].Value)
			if err != nil {
				return err
			}
			typed[index].Value = converted
			return nil
		}
		return fmt.Errorf("object member %q does not exist", token)
	case bson.A:
		index, err := bsonArrayIndex(token, len(typed))
		if err != nil {
			return err
		}
		if len(tokens) > 1 {
			return applyDateTimePath(typed[index], tokens[1:])
		}
		converted, err := bsonDateTime(typed[index])
		if err != nil {
			return err
		}
		typed[index] = converted
		return nil
	default:
		return fmt.Errorf("cannot traverse %T with token %q", value, token)
	}
}

func bsonDateTime(value any) (bson.DateTime, error) {
	text, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("date-time value has type %T, not string", value)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, fmt.Errorf("parse RFC3339 date-time: %w", err)
	}
	return bson.NewDateTimeFromTime(timestamp), nil
}

func bsonArrayIndex(token string, length int) (int, error) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	index, err := strconv.Atoi(token)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	if index >= length {
		return 0, fmt.Errorf("array index %d is out of bounds", index)
	}
	return index, nil
}

func jsonDocumentFromBSON(value bson.D) (storage.Document, error) {
	var document storage.Document
	extendedJSON, err := bson.MarshalExtJSON(value, false, false)
	if err != nil {
		return document, err
	}
	decoder := json.NewDecoder(bytes.NewReader(extendedJSON))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return document, err
	}
	dateTimePaths := make([]string, 0)
	normalized, err := normalizeExtendedJSON(decoded, "", &dateTimePaths)
	if err != nil {
		return document, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return document, err
	}
	sort.Strings(dateTimePaths)
	document.JSON = encoded
	document.DateTimePaths = dateTimePaths
	return document, nil
}

func normalizeExtendedJSON(value any, pointer jsontext.Pointer, dateTimePaths *[]string) (any, error) {
	switch typed := value.(type) {
	case []any:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			child := pointer.AppendToken(strconv.Itoa(index))
			converted, err := normalizeExtendedJSON(item, child, dateTimePaths)
			if err != nil {
				return nil, err
			}
			normalized[index] = converted
		}
		return normalized, nil
	case map[string]any:
		dateTime, isDateTime, err := extendedJSONDateTime(typed)
		if err != nil {
			return nil, err
		}
		if isDateTime {
			*dateTimePaths = append(*dateTimePaths, string(pointer))
			return dateTime, nil
		}
		normalized := make(map[string]any, len(typed))
		for key, item := range typed {
			child := pointer.AppendToken(key)
			converted, err := normalizeExtendedJSON(item, child, dateTimePaths)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}
		return normalized, nil
	default:
		return value, nil
	}
}

func extendedJSONDateTime(value map[string]any) (string, bool, error) {
	raw, exists := value["$date"]
	if !exists || len(value) != 1 {
		return "", false, nil
	}
	var timestamp time.Time
	switch typed := raw.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return "", false, fmt.Errorf("parse MongoDB Extended JSON date-time: %w", err)
		}
		timestamp = parsed
	case map[string]any:
		number, ok := typed["$numberLong"].(string)
		if !ok || len(typed) != 1 {
			return "", false, errors.New("MongoDB Extended JSON date-time has an invalid $numberLong value")
		}
		milliseconds, err := strconv.ParseInt(number, 10, 64)
		if err != nil {
			return "", false, fmt.Errorf("parse MongoDB Extended JSON date-time milliseconds: %w", err)
		}
		timestamp = time.UnixMilli(milliseconds)
	default:
		return "", false, fmt.Errorf("MongoDB Extended JSON date-time has type %T", raw)
	}
	encoded, err := timestamp.UTC().MarshalJSON()
	if err != nil {
		return "", false, fmt.Errorf("encode MongoDB date-time as RFC3339: %w", err)
	}
	var text string
	if err := json.Unmarshal(encoded, &text); err != nil {
		return "", false, err
	}
	return text, true, nil
}
