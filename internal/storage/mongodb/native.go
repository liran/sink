package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"strings"
	"time"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func validateNativeCommand(req storage.NativeRequest, scan bool) (bson.D, error) {
	var command bson.D
	if strings.TrimSpace(req.Namespace) == "" {
		return command, errors.New("MongoDB native requests require a namespace")
	}
	if req.Method != "" || req.Path != "" || req.Query != "" || len(req.Headers) != 0 {
		return command, errors.New("MongoDB native requests do not use method, path, query or headers")
	}
	mediaType, _, err := mime.ParseMediaType(req.ContentType)
	if err != nil || mediaType != "application/bson" {
		return command, errors.New("MongoDB native requests require application/bson content_type")
	}
	if err := bson.Raw(req.Payload).Validate(); err != nil {
		return command, fmt.Errorf("invalid BSON command: %w", err)
	}
	if err := bson.Unmarshal(req.Payload, &command); err != nil {
		return command, fmt.Errorf("decode BSON command: %w", err)
	}
	if len(command) == 0 {
		return command, errors.New("BSON command is empty")
	}
	seen := make(map[string]bool, len(command))
	for _, field := range command {
		if seen[field.Key] {
			return command, fmt.Errorf("duplicate command field %q", field.Key)
		}
		seen[field.Key] = true
		switch field.Key {
		case "$db", "lsid", "txnNumber", "startTransaction", "autocommit", "apiVersion", "apiStrict", "apiDeprecationErrors", "maxTimeMS", "$readPreference":
			return command, fmt.Errorf("command field %q is managed by Sink's driver", field.Key)
		}
	}
	name := command[0].Key
	if scan {
		switch name {
		case "find", "aggregate", "listIndexes", "listCollections":
		default:
			return command, fmt.Errorf("command %q cannot be scanned", name)
		}
		if forbiddenNativeValue(command) {
			return command, errors.New("scan does not permit data-writing stages or tailable cursors")
		}
		return command, nil
	}
	switch name {
	case "find", "aggregate", "listIndexes", "listCollections", "getMore", "killCursors", "parallelCollectionScan", "bulkWrite":
		return command, fmt.Errorf("command %q uses a cursor; Execute does not manage cursor sessions, use Scan for supported cursor queries", name)
	case "startSession", "refreshSessions", "endSessions", "commitTransaction", "abortTransaction":
		return command, fmt.Errorf("command %q requires client-managed sessions, which Execute does not support", name)
	}
	return command, nil
}

func forbiddenNativeValue(value any) bool {
	switch typed := value.(type) {
	case bson.D:
		for _, field := range typed {
			switch field.Key {
			case "$out", "$merge", "$changeStream", "tailable", "awaitData", "noCursorTimeout", "singleBatch":
				return true
			}
			if forbiddenNativeValue(field.Value) {
				return true
			}
		}
	case bson.A:
		for _, item := range typed {
			if forbiddenNativeValue(item) {
				return true
			}
		}
	}
	return false
}

func (s *Store) Execute(ctx context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	var empty storage.NativeResponse
	if req.Store != s.store {
		return empty, storage.InvalidArgumentError(errors.New("MongoDB store does not match request"))
	}
	if _, err := validateNativeCommand(req, false); err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	raw, err := s.client.Database(req.Namespace).RunCommand(ctx, bson.Raw(req.Payload)).Raw()
	if len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
		}
		var writeError mongo.WriteException
		if errors.As(err, &writeError) {
			raw = writeError.Raw
		}
	}
	if len(raw) == 0 {
		if err == nil {
			err = errors.New("MongoDB command returned an empty response")
		}
		return empty, storage.BackendError(err)
	}
	budget := storage.NewReadBudget(req.MaxBytes)
	if budgetErr := budget.Reserve(len(raw)); budgetErr != nil {
		return empty, budgetErr
	}
	response := storage.NativeResponse{ContentType: "application/bson", Payload: bytes.Clone(raw), Success: err == nil}
	return response, nil
}

func scanCommand(command bson.D, batchSize int) bson.D {
	name := command[0].Key
	filtered := make(bson.D, 0, len(command)+1)
	for _, field := range command {
		if field.Key == "batchSize" || (name != "find" && field.Key == "cursor") {
			continue
		}
		filtered = append(filtered, field)
	}
	batch := bson.E{Key: "batchSize", Value: int32(batchSize)}
	if name == "find" {
		filtered = append(filtered, batch)
	} else {
		cursor := bson.D{batch}
		field := bson.E{Key: "cursor", Value: cursor}
		filtered = append(filtered, field)
	}
	return filtered
}

func (s *Store) Scan(ctx context.Context, req storage.ScanRequest, send func([]storage.Document) error) error {
	if req.BatchSize < 1 || req.BatchSize > 1000 {
		return storage.InvalidArgumentError(errors.New("scan batch size must be between 1 and 1000"))
	}
	if req.Request.Store != s.store {
		return storage.InvalidArgumentError(errors.New("MongoDB store does not match request"))
	}
	command, err := validateNativeCommand(req.Request, true)
	if err != nil {
		return storage.InvalidArgumentError(err)
	}
	command = scanCommand(command, req.BatchSize)
	cursor, err := s.client.Database(req.Request.Namespace).RunCommandCursor(ctx, command)
	if err != nil {
		return storage.BackendError(err)
	}
	cursor.SetBatchSize(int32(req.BatchSize))
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(cleanup)
	}()
	batch := make([]storage.Document, 0, req.BatchSize)
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	for cursor.Next(ctx) {
		if err := budget.Reserve(len(cursor.Current)); err != nil {
			if len(batch) == 0 {
				return err
			}
			if err := send(batch); err != nil {
				return err
			}
			batch = make([]storage.Document, 0, req.BatchSize)
			budget = storage.NewReadBudget(req.Request.MaxBytes)
			if err := budget.Reserve(len(cursor.Current)); err != nil {
				return err
			}
		}
		document := storage.Document{Encoding: storage.DocumentEncodingBSON, Payload: bytes.Clone(cursor.Current)}
		batch = append(batch, document)
		if len(batch) == req.BatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = make([]storage.Document, 0, req.BatchSize)
			budget = storage.NewReadBudget(req.Request.MaxBytes)
		}
	}
	if err := cursor.Err(); err != nil {
		return storage.BackendError(err)
	}
	if len(batch) != 0 {
		return send(batch)
	}
	return nil
}
