package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func CloneDocument(document Document) Document {
	cloned := Document{
		Encoding: document.Encoding,
		Payload:  bytes.Clone(document.Payload),
	}
	return cloned
}

func ValidateDocument(document Document) error {
	switch document.Encoding {
	case DocumentEncodingJSON:
		trimmed := bytes.TrimSpace(document.Payload)
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
			return errors.New("document payload must contain a valid JSON object")
		}
	case DocumentEncodingBSON:
		raw := bson.Raw(document.Payload)
		if err := raw.Validate(); err != nil {
			return fmt.Errorf("document payload must contain a valid BSON document: %w", err)
		}
	default:
		return errors.New("document encoding is required")
	}
	return nil
}
