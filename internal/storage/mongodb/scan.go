package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func scanFind(command bson.D, req storage.ScanRequest, position []byte) (bson.D, bool, error) {
	var empty bson.D
	if command[0].Key != "find" {
		return empty, false, errors.New("Scan requires find with an _id seek order; use Query for aggregates and Execute for non-cursor commands")
	}
	collection, ok := command[0].Value.(string)
	if !ok || collection == "" {
		return empty, false, errors.New("Scan requires a collection name")
	}
	result := bson.D{command[0]}
	filter := bson.D{}
	direction := int32(1)
	hideID := false
	for _, field := range command[1:] {
		switch field.Key {
		case "filter":
			filter, ok = field.Value.(bson.D)
			if !ok {
				return empty, false, errors.New("Scan filter must be a document")
			}
		case "sort":
			sort, valid := field.Value.(bson.D)
			if !valid || len(sort) != 1 || sort[0].Key != "_id" {
				return empty, false, errors.New("Scan supports only _id ascending or descending sort")
			}
			switch value := sort[0].Value.(type) {
			case int32:
				direction = value
			case int64:
				if value != 1 && value != -1 {
					return empty, false, errors.New("invalid Scan sort direction")
				}
				direction = int32(value)
			case float64:
				if value != 1 && value != -1 {
					return empty, false, errors.New("invalid Scan sort direction")
				}
				direction = int32(value)
			default:
				return empty, false, errors.New("invalid Scan sort direction")
			}
			if direction != 1 && direction != -1 {
				return empty, false, errors.New("invalid Scan sort direction")
			}
		case "batchSize":
		case "collation":
			collation, valid := field.Value.(bson.D)
			if !valid || len(collation) != 1 || collation[0].Key != "locale" || collation[0].Value != "simple" {
				return empty, false, errors.New("Scan requires simple collation for a unique _id order")
			}
		case "projection":
			projection, valid := field.Value.(bson.D)
			if !valid {
				return empty, false, errors.New("Scan projection must be a document")
			}
			projected := make(bson.D, 0, len(projection))
			for _, part := range projection {
				if strings.HasPrefix(part.Key, "_id.") {
					return empty, false, errors.New("Scan cannot project part of _id")
				}
				if part.Key == "_id" {
					switch part.Value {
					case false, int32(0), int64(0), float64(0):
						hideID = true
						continue
					case true, int32(1), int64(1), float64(1):
					default:
						return empty, false, errors.New("Scan cannot replace _id in a projection")
					}
				}
				projected = append(projected, part)
			}
			field.Value = projected
			result = append(result, field)
		case "hint", "readConcern", "let", "comment", "allowDiskUse":
			result = append(result, field)
		default:
			return empty, false, fmt.Errorf("find option %q is unsupported by resumable Scan; use Query for skip/limit pagination", field.Key)
		}
	}
	if len(position) != 0 {
		raw := bson.Raw(position)
		if err := raw.Validate(); err != nil {
			return empty, false, errors.New("invalid BSON scan position")
		}
		elements, err := raw.Elements()
		if err != nil || len(elements) != 1 || elements[0].Key() != "_id" {
			return empty, false, errors.New("invalid BSON scan position")
		}
		last := raw.Lookup("_id")
		operator := "$gt"
		if direction < 0 {
			operator = "$lt"
		}
		// Expression comparison uses BSON's total order, including mixed _id
		// types. Ordinary query $gt would silently exclude other BSON types.
		literal := bson.D{{Key: "$literal", Value: last}}
		comparison := bson.D{{Key: operator, Value: bson.A{"$_id", literal}}}
		seek := bson.D{{Key: "$expr", Value: comparison}}
		filter = bson.D{{Key: "$and", Value: bson.A{filter, seek}}}
	}
	sort := bson.D{{Key: "_id", Value: direction}}
	collation := bson.D{{Key: "locale", Value: "simple"}}
	fields := bson.D{{Key: "filter", Value: filter}, {Key: "sort", Value: sort},
		{Key: "collation", Value: collation}, {Key: "limit", Value: int64(req.BatchSize + 1)},
		{Key: "batchSize", Value: int32(req.BatchSize + 1)}}
	result = append(result, fields...)
	return result, hideID, nil
}

func (s *Store) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	var empty storage.ScanResponse
	if req.Request.Store != s.store {
		return empty, storage.InvalidArgumentError(errors.New("MongoDB store does not match request"))
	}
	seek, err := req.Resume()
	if err != nil {
		return empty, err
	}
	command, err := validateNativeCommand(req.Request, true)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	command, hideID, err := scanFind(command, req, seek.Position)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	cursor, err := s.client.Database(req.Request.Namespace).RunCommandCursor(ctx, command)
	if err != nil {
		return empty, storage.BackendError(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(cleanup)
	}()
	documents := make([]storage.Document, 0, req.BatchSize)
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	var position []byte
	more := false
	for cursor.Next(ctx) {
		if len(documents) == req.BatchSize {
			more = true
			break
		}
		raw := cursor.Current
		id := raw.Lookup("_id")
		if id.Type == 0 {
			return empty, errors.New("Scan result omitted _id")
		}
		marker := bson.D{{Key: "_id", Value: id}}
		next, err := bson.Marshal(marker)
		if err != nil {
			return empty, err
		}
		payload := bytes.Clone(raw)
		if hideID {
			var document bson.D
			if err := bson.Unmarshal(raw, &document); err != nil {
				return empty, err
			}
			filtered := make(bson.D, 0, len(document)-1)
			for _, field := range document {
				if field.Key != "_id" {
					filtered = append(filtered, field)
				}
			}
			payload, err = bson.Marshal(filtered)
			if err != nil {
				return empty, err
			}
		}
		if err := budget.Reserve(len(payload)); err != nil {
			if len(documents) == 0 {
				return empty, err
			}
			more = true
			break
		}
		document := storage.Document{Encoding: storage.DocumentEncodingBSON, Payload: payload}
		documents = append(documents, document)
		position = next
	}
	if err := cursor.Err(); err != nil {
		return empty, storage.BackendError(err)
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if !more {
		position = nil
	}
	return seek.Page(documents, position)
}
