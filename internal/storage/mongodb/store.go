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
	defaultHiddenField      = "__sink"
	defaultConcurrentWrites = 64
)

type Dataset struct {
	Namespace string
	Dataset   string
}

type Binding struct {
	Database   string
	Collection string
}

type Options struct {
	Store               string
	HiddenField         string
	Bindings            map[Dataset]Binding
	MaxConcurrentWrites int
}

type Store struct {
	client              *mongo.Client
	store               string
	hiddenField         string
	bindings            map[Dataset]Binding
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

	hiddenField := opts.HiddenField
	if hiddenField == "" {
		hiddenField = defaultHiddenField
	}
	if hiddenField == "_id" || strings.ContainsAny(hiddenField, ".$\x00") {
		return nil, errors.New("create MongoDB storage: hidden field is invalid")
	}

	maxConcurrentWrites := opts.MaxConcurrentWrites
	if maxConcurrentWrites == 0 {
		maxConcurrentWrites = defaultConcurrentWrites
	}
	bindings := make(map[Dataset]Binding, len(opts.Bindings))
	for dataset, binding := range opts.Bindings {
		if dataset.Namespace == "" || dataset.Dataset == "" || binding.Database == "" || binding.Collection == "" {
			return nil, errors.New("create MongoDB storage: dataset binding is incomplete")
		}
		bindings[dataset] = binding
	}

	store := &Store{
		client:              client,
		store:               opts.Store,
		hiddenField:         hiddenField,
		bindings:            bindings,
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

	binding := Binding{
		Database:   address.Namespace,
		Collection: address.Dataset,
	}
	if len(s.bindings) > 0 {
		dataset := Dataset{
			Namespace: address.Namespace,
			Dataset:   address.Dataset,
		}
		configured, exists := s.bindings[dataset]
		if !exists {
			return resolved, fmt.Errorf("logical dataset %q/%q is not configured", address.Namespace, address.Dataset)
		}
		binding = configured
	}

	resolved.database = binding.Database
	resolved.collection = binding.Collection
	resolved.value = s.client.Database(binding.Database).Collection(binding.Collection)
	return resolved, nil
}

func (r resolvedCollection) key() string {
	return r.database + "\x00" + r.collection
}
