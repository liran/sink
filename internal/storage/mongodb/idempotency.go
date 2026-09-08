package mongodb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const receiptCollection = "__sink_receipts_v1"

const receiptOwner = "sink:idempotency:v1"

// Never put a TTL index on a pre-existing application collection. Ownership
// is established by the validator at collection creation, not by its name.
func (s *Store) ensureReceiptCollection(ctx context.Context, namespace string) error {
	s.receiptMu.Lock()
	ready := s.receiptDatabases[namespace]
	s.receiptMu.Unlock()
	if ready {
		return nil
	}
	database := s.client.Database(namespace)
	equal := bson.D{{Key: "$eq", Value: receiptOwner}}
	validator := bson.D{{Key: "_sink_receipt_owner", Value: equal}}
	opts := options.CreateCollection().SetValidator(validator).SetValidationAction("error").SetValidationLevel("strict")
	err := database.CreateCollection(ctx, receiptCollection, opts)
	if err != nil {
		var commandError mongo.CommandError
		if !errors.As(err, &commandError) || commandError.Code != 48 {
			return storage.BackendError(err)
		}
		filter := bson.D{{Key: "name", Value: receiptCollection}}
		specifications, err := database.ListCollectionSpecifications(ctx, filter)
		if err != nil {
			return storage.BackendError(err)
		}
		if len(specifications) != 1 {
			return storage.InvalidArgumentError(errors.New("receipt collection ownership is unknown"))
		}
		owner, _ := specifications[0].Options.Lookup("validator", "_sink_receipt_owner", "$eq").StringValueOK()
		if owner != receiptOwner {
			return storage.InvalidArgumentError(errors.New("reserved receipt collection already exists without Sink ownership; it was not modified"))
		}
	}
	keys := bson.D{{Key: "expires_at", Value: 1}}
	index := mongo.IndexModel{Keys: keys, Options: options.Index().SetName("sink_receipt_expiry").SetExpireAfterSeconds(0)}
	if _, err := database.Collection(receiptCollection).Indexes().CreateOne(ctx, index); err != nil {
		return storage.BackendError(err)
	}
	s.receiptMu.Lock()
	if s.receiptDatabases == nil {
		s.receiptDatabases = make(map[string]bool)
	}
	if len(s.receiptDatabases) < 128 {
		s.receiptDatabases[namespace] = true
	}
	s.receiptMu.Unlock()
	return nil
}

func (s *Store) CheckIdempotency(ctx context.Context, address storage.Address) error {
	if _, err := s.resolve(address); err != nil {
		return err
	}
	command := bson.D{{Key: "hello", Value: 1}}
	raw, err := s.client.Database(address.Namespace).RunCommand(ctx, command).Raw()
	if err != nil {
		return storage.BackendError(err)
	}
	setName, _ := raw.Lookup("setName").StringValueOK()
	message, _ := raw.Lookup("msg").StringValueOK()
	if setName == "" && message != "isdbgrid" {
		return storage.ErrIdempotencyUnsupported
	}
	return nil
}

type operationReceipt struct {
	Fingerprint []byte `bson:"fingerprint"`
	Result      []byte `bson:"result"`
}

func (s *Store) ExecuteOnce(ctx context.Context, req storage.IdempotentRequest) ([]byte, error) {
	created, err := storage.OperationCreated(req.OperationID, time.Now())
	if err != nil {
		return nil, err
	}
	if len(req.Fingerprint) != sha256.Size || req.Apply == nil || req.MaxReceiptBytes <= 0 {
		return nil, storage.InvalidArgumentError(errors.New("invalid idempotent write request"))
	}
	if _, err := s.resolve(req.Address); err != nil {
		return nil, err
	}
	// The collection is independent of business documents, so native updates,
	// replacements and hard deletes cannot erase a committed receipt.
	collection := s.client.Database(req.Address.Namespace).Collection(receiptCollection)
	if err := s.ensureReceiptCollection(ctx, req.Address.Namespace); err != nil {
		return nil, err
	}
	session, err := s.client.StartSession()
	if err != nil {
		return nil, storage.BackendError(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		session.EndSession(cleanup)
	}()
	// Scope the ID to the logical store/namespace, not to a record. Reusing an
	// ID for another address must conflict, not silently create another effect.
	digest := sha256.Sum256([]byte(req.Address.Store + "\x00" + req.OperationID))
	id := bson.Binary{Subtype: 0, Data: digest[:]}
	filter := bson.D{{Key: "_id", Value: id}}
	txnOptions := options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary())
	apply := func(tx context.Context) (any, error) {
		if _, err := storage.OperationCreated(req.OperationID, time.Now()); err != nil {
			return nil, err
		}
		var existing operationReceipt
		err := collection.FindOne(tx, filter).Decode(&existing)
		if err == nil {
			if !bytes.Equal(existing.Fingerprint, req.Fingerprint) {
				return nil, storage.NewOperationError(storage.ErrorCodeConflict, false, errors.New("operation_id was already used for a different request"))
			}
			if len(existing.Result) == 0 || len(existing.Result) > req.MaxReceiptBytes {
				return nil, storage.ResourceExhaustedError(errors.New("stored receipt exceeds response limit or is invalid"))
			}
			return existing.Result, nil
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		receipt := bson.D{{Key: "_id", Value: id}, {Key: "_sink_receipt_owner", Value: receiptOwner}, {Key: "fingerprint", Value: req.Fingerprint}, {Key: "expires_at", Value: created.Add(storage.OperationRetention)}}
		if _, err := collection.InsertOne(tx, receipt); err != nil {
			return nil, err
		}
		result, err := req.Apply(tx)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || len(result) > req.MaxReceiptBytes {
			return nil, storage.ResourceExhaustedError(errors.New("idempotent receipt exceeds byte limit"))
		}
		fields := bson.D{{Key: "result", Value: result}}
		update := bson.D{{Key: "$set", Value: fields}}
		if _, err := collection.UpdateOne(tx, filter, update); err != nil {
			return nil, err
		}
		return result, nil
	}
	for range 3 {
		result, err := session.WithTransaction(ctx, apply, txnOptions)
		if mongo.IsDuplicateKeyError(err) {
			// A concurrent transaction committed this ID. Start a fresh snapshot
			// to read its receipt; never rerun the effect outside a transaction.
			continue
		}
		if err != nil {
			return nil, storage.BackendError(err)
		}
		encoded, ok := result.([]byte)
		if !ok {
			return nil, errors.New("invalid committed receipt type")
		}
		return encoded, nil
	}
	return nil, storage.BackendError(errors.New("concurrent operation receipt did not settle"))
}
