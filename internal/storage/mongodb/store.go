// Package mongodb implements document storage backed by MongoDB.
package mongodb

import (
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
)

type Options struct {
	Store               string
	MetadataField       string
	MaxConcurrentWrites int
}

type Store struct {
	client              *mongo.Client
	store               string
	metadataField       string
	maxConcurrentWrites int
}

func New(client *mongo.Client, opts Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("create MongoDB storage: client is required")
	}
	if opts.Store == "" {
		return nil, errors.New("create MongoDB storage: logical store is required")
	}
	if opts.MaxConcurrentWrites < 0 {
		return nil, errors.New("create MongoDB storage: max concurrent writes cannot be negative")
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
	store := &Store{
		client:              client,
		store:               opts.Store,
		metadataField:       metadataField,
		maxConcurrentWrites: maxConcurrentWrites,
	}
	return store, nil
}

type resolvedCollection struct {
	database   string
	collection string
	value      *mongo.Collection
}

func (s *Store) resolve(address storage.Address) (resolvedCollection, error) {
	var resolved resolvedCollection
	if address.Store != s.store {
		return resolved, fmt.Errorf("logical store %q is not configured", address.Store)
	}
	if address.Namespace == "" || address.Dataset == "" {
		return resolved, errors.New("logical namespace and dataset are required")
	}

	resolved.database = address.Namespace
	resolved.collection = address.Dataset
	resolved.value = s.client.Database(address.Namespace).Collection(address.Dataset)
	return resolved, nil
}

func (r resolvedCollection) key() string {
	return r.database + "\x00" + r.collection
}
