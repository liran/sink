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
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func validateNativeCommand(req storage.NativeRequest, scan bool) (bson.D, error) {
	var command bson.D
	if req.MongoDB == nil || req.Search != nil || strings.TrimSpace(req.MongoDB.Database) == "" {
		return command, errors.New("MongoDB native requests require a database and BSON command")
	}
	if err := bson.Unmarshal(req.MongoDB.Command, &command); err != nil {
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
	case "count", "distinct", "collStats", "dbStats", "ping", "createIndexes", "dropIndexes":
		return command, nil
	case "explain":
		inner, ok := command[0].Value.(bson.D)
		if !ok || len(inner) == 0 {
			return command, errors.New("explain requires a read command")
		}
		switch inner[0].Key {
		case "find", "aggregate", "count", "distinct":
			if !forbiddenNativeValue(inner) {
				return command, nil
			}
		}
	}
	return command, fmt.Errorf("command %q is unsupported; use Scan for cursors and Write/Delete for data mutations", name)
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
	raw, err := s.client.Database(req.MongoDB.Database).RunCommand(ctx, bson.Raw(req.MongoDB.Command)).Raw()
	if len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
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
	cursor, err := s.client.Database(req.Request.MongoDB.Database).RunCommandCursor(ctx, command)
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
