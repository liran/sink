// Package search implements document storage compatible with Elasticsearch and OpenSearch.
package search

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/liran/sink/internal/storage"
)

const (
	ContentTypeJSON        = "application/json"
	defaultRequestTimeout  = 30 * time.Second
	defaultMaxResponseSize = 64 << 20
)

type Driver string

const (
	DriverElasticsearch Driver = "elasticsearch"
	DriverOpenSearch    Driver = "opensearch"
)

type Dataset struct {
	Namespace string
	Dataset   string
}

type Binding struct {
	Index string
}

type Options struct {
	Driver          Driver
	Endpoints       []string
	Store           string
	Bindings        map[Dataset]Binding
	Username        string
	Password        string
	APIKey          string
	HTTPClient      *http.Client
	MaxResponseSize int64
}

type Store struct {
	driver          Driver
	endpoints       []*url.URL
	logicalStore    string
	bindings        map[Dataset]Binding
	username        string
	password        string
	apiKey          string
	client          *http.Client
	maxResponseSize int64
	nextEndpoint    atomic.Uint64
}

func New(opts Options) (*Store, error) {
	if opts.Driver != DriverElasticsearch && opts.Driver != DriverOpenSearch {
		return nil, fmt.Errorf("create search storage: unsupported driver %q", opts.Driver)
	}
	if opts.Store == "" {
		return nil, errors.New("create search storage: logical store is required")
	}
	if len(opts.Endpoints) == 0 {
		return nil, errors.New("create search storage: at least one endpoint is required")
	}
	if (opts.Username == "") != (opts.Password == "") {
		return nil, errors.New("create search storage: username and password must be configured together")
	}
	if opts.APIKey != "" && opts.Username != "" {
		return nil, errors.New("create search storage: API key and basic authentication are mutually exclusive")
	}
	if opts.MaxResponseSize < 0 {
		return nil, errors.New("create search storage: max response size cannot be negative")
	}

	endpoints := make([]*url.URL, 0, len(opts.Endpoints))
	for _, rawEndpoint := range opts.Endpoints {
		endpoint, err := parseEndpoint(rawEndpoint)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	bindings := make(map[Dataset]Binding, len(opts.Bindings))
	for dataset, binding := range opts.Bindings {
		if dataset.Namespace == "" || dataset.Dataset == "" || binding.Index == "" {
			return nil, errors.New("create search storage: dataset binding is incomplete")
		}
		bindings[dataset] = binding
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}
	maxResponseSize := opts.MaxResponseSize
	if maxResponseSize == 0 {
		maxResponseSize = defaultMaxResponseSize
	}
	store := &Store{
		driver:          opts.Driver,
		endpoints:       endpoints,
		logicalStore:    opts.Store,
		bindings:        bindings,
		username:        opts.Username,
		password:        opts.Password,
		apiKey:          opts.APIKey,
		client:          client,
		maxResponseSize: maxResponseSize,
	}
	return store, nil
}

func parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("create search storage: parse endpoint: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("create search storage: endpoint scheme must be http or https")
	}
	if endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("create search storage: endpoint must contain only scheme, host, and an optional path")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	return endpoint, nil
}

type resolvedDocument struct {
	index string
	id    string
}

func (s *Store) resolve(address storage.Address) (resolvedDocument, error) {
	var resolved resolvedDocument
	if address.Store != s.logicalStore {
		return resolved, fmt.Errorf("logical store %q is not configured", address.Store)
	}
	if address.Namespace == "" || address.Dataset == "" {
		return resolved, errors.New("logical namespace and dataset are required")
	}

	index := address.Namespace + "-" + address.Dataset
	if len(s.bindings) > 0 {
		dataset := Dataset{Namespace: address.Namespace, Dataset: address.Dataset}
		binding, exists := s.bindings[dataset]
		if !exists {
			return resolved, fmt.Errorf("logical dataset %q/%q is not configured", address.Namespace, address.Dataset)
		}
		index = binding.Index
	}
	id, err := documentID(address.Key)
	if err != nil {
		return resolved, err
	}
	resolved.index = index
	resolved.id = id
	return resolved, nil
}
