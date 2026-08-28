// Package mongodb implements document storage backed by MongoDB.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	ContentTypeBSON         = "application/bson"
	defaultMetadataField    = "__sink"
	defaultConcurrentWrites = 64
	defaultConcurrentGroups = 16
)

type Options struct {
	Store               string
	MetadataField       string
	MaxConcurrentWrites int
	MaxConcurrentGroups int
}

type Store struct {
	client              *mongo.Client
	store               string
	metadataField       string
	maxConcurrentWrites int
	maxConcurrentGroups int
}

func New(client *mongo.Client, opts Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("create MongoDB storage: client is required")
	}
	if opts.Store == "" {
		return nil, errors.New("create MongoDB storage: logical store is required")
	}
	if opts.MaxConcurrentWrites < 0 || opts.MaxConcurrentGroups < 0 {
		return nil, errors.New("create MongoDB storage: concurrency limits cannot be negative")
	}

	metadataField := opts.MetadataField
	if metadataField == "" {
		metadataField = defaultMetadataField
	}
	if metadataField == "_id" || strings.ContainsAny(metadataField, ".$\x00") {
		return nil, errors.New("create MongoDB storage: metadata field is invalid")
	}

	maxConcurrentWrites := opts.MaxConcurrentWrites
	if maxConcurrentWrites == 0 {
		maxConcurrentWrites = defaultConcurrentWrites
	}
	maxConcurrentGroups := opts.MaxConcurrentGroups
	if maxConcurrentGroups == 0 {
		maxConcurrentGroups = defaultConcurrentGroups
	}
	store := &Store{
		client:              client,
		store:               opts.Store,
		metadataField:       metadataField,
		maxConcurrentWrites: maxConcurrentWrites,
		maxConcurrentGroups: maxConcurrentGroups,
	}
	return store, nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx, nil); err != nil {
		return storage.BackendError(err)
	}
	return nil
}

type resolvedCollection struct {
	database   string
	collection string
	value      *mongo.Collection
}

func (s *Store) resolve(address storage.Address) (resolvedCollection, error) {
	var resolved resolvedCollection
	if address.Store != s.store {
		err := fmt.Errorf("logical store %q is not configured", address.Store)
		return resolved, storage.InvalidArgumentError(err)
	}
	if address.Namespace == "" || address.Dataset == "" {
		err := errors.New("logical namespace and dataset are required")
		return resolved, storage.InvalidArgumentError(err)
	}

	resolved.database = address.Namespace
	resolved.collection = address.Dataset
	resolved.value = s.client.Database(address.Namespace).Collection(address.Dataset)
	return resolved, nil
}

func (r resolvedCollection) key() string {
	return r.database + "\x00" + r.collection
}
