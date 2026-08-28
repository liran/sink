# Server configuration

Sink reads all server runtime parameters from one YAML file. Pass its path when
starting the binary:

```shell
sink --config /etc/sink/config.yaml
```

The `--config` option is required for every runtime mode. `sink version` is the
only command that does not load a configuration file. Environment variables
such as `SINK_MODE` and `SINK_MONGODB_URI` are not read by the server.

Configuration is loaded once during startup. Unknown fields, malformed YAML,
multiple YAML documents, duplicate storage names, invalid positive-integer
values, and incompatible option combinations prevent the process from
starting. Sink connects to and pings every configured storage before becoming
ready; failure of any storage prevents startup. Restart Sink after changing the
file.

## Multiple storage instances

The required `storages` list can contain any combination of MongoDB,
Elasticsearch, and OpenSearch instances. Every entry has a unique `name`. A
request's `address.store` must exactly match that name:

```yaml
mode: server

grpc:
  address: ":8080"

prometheus:
  address: ":9090"

storages:
  - name: mongo-main
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-main:27017
      metadata_field: __sink

  - name: mongo-archive
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-archive:27017

  - name: search-main
    driver: elasticsearch
    search:
      endpoints:
        - https://search-1:9200
        - https://search-2:9200
      api_key: replace-with-api-key

service:
  max_operations: 1000
  max_merge_attempts: 3

kafka:
  max_poll_records: 500

shutdown_timeout_seconds: 15
```

The repository's [`config.example.yaml`](../config.example.yaml) is a smaller,
ready-to-edit configuration containing one MongoDB instance.

## Address routing

Sink does not use a separate bindings configuration. The client-provided record
address selects both the configured storage instance and the location inside
that instance:

| Address field | MongoDB | Elasticsearch and OpenSearch |
| --- | --- | --- |
| `store` | Exact `storages[].name` to use | Exact `storages[].name` to use |
| `namespace` | Database name | Logical business namespace; not used to construct the index name |
| `dataset` | Collection name | Complete existing index or alias name |
| `key` | MongoDB `_id` | Document `_id` |

For example, this address selects the `mongo-main` configuration and stores the
document in MongoDB database `catalog`, collection `products`:

```text
store = mongo-main
namespace = catalog
dataset = products
key = product-123
```

With a search driver, set `dataset` to the full name already used by the service.
For example, `namespace = catalog` and `dataset = legacy-products-v2` access the
index or alias `legacy-products-v2`; Sink does not prepend the namespace. Sink
can route operations in one batch to different storage instances and returns
results in the original operation order. An address whose `store` is not
configured receives a per-operation failure.

## Prometheus metrics

Set `prometheus.address` to open a separate HTTP listener. Prometheus metrics
are served at the fixed `/metrics` path in `server`, `worker`, and `all` modes.
Omit the address or set it to an empty string to disable the listener.

```yaml
prometheus:
  address: ":9090"
```

The endpoint includes the standard Go runtime and process collectors plus these
Sink metrics:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `sink_build_info` | gauge | `version` | Build identity for the running Sink binary. |
| `sink_grpc_server_requests_total` | counter | `method`, `code` | Completed Sink gRPC requests by method and canonical gRPC status code. |
| `sink_grpc_server_request_duration_seconds` | histogram | `method` | End-to-end Sink gRPC request latency. |
| `sink_grpc_server_operation_results_total` | counter | `method`, `status` | Per-operation results returned inside batch responses. |

Labels intentionally exclude storage names, namespaces, datasets, record keys,
and error messages to keep metric cardinality bounded. The endpoint has no
application-level authentication; bind it to a private interface or protect it
with the deployment network policy.

### Migrating a single-storage configuration

The former top-level `storage` object is replaced by the `storages` list. Move
the former `storage.mongodb.store` or `storage.search.store` value to the
entry's required `name`, keep the driver-specific connection fields under that
entry, rename `storage.mongodb.hidden_field` to
`storages[].mongodb.metadata_field`, and remove `bindings`. Update client
addresses so their namespace and dataset contain the direct MongoDB
database/collection names. For search drivers, keep the logical namespace and
put the complete existing index or alias name in `dataset`.

## Configuration reference

“Conditionally required” means a field is mandatory only in the modes or with
the drivers stated in its description. Enum values are case-sensitive and must
use the lowercase spelling shown below. Storage names are also case-sensitive.

| Field | Type | Required | Default | Allowed values | Function |
| --- | --- | --- | --- | --- | --- |
| `mode` | enum string | No | `server` | `server`, `worker`, `all` | Process role. See [Mode values](#mode-values). |
| `grpc.address` | string | No | `:8080` | Any valid TCP listen address | TCP listen address for the gRPC and gRPC health services. Used in `server` and `all` modes. |
| `prometheus.address` | string | No | empty (disabled) | Empty or any valid TCP listen address | HTTP listen address for Prometheus `/metrics`. Available in every runtime mode. |
| `storages` | list | Yes | none | One or more storage objects | Storage instances available for address routing. |
| `storages[].name` | string | Yes | none | Any unique, non-empty name | Exact value selected by `address.store`. |
| `storages[].driver` | enum string | Yes | none | `mongodb`, `elasticsearch`, `opensearch` | Adapter used by this storage instance. See [Storage driver values](#storage-driver-values). |
| `storages[].mongodb.uri` | string | Conditionally | none | Valid MongoDB connection string | Required when the entry's driver is `mongodb`. |
| `storages[].mongodb.metadata_field` | string | No | `__sink` | Any valid MongoDB field except `_id`; cannot contain `.`, `$`, or a null byte | Reserved top-level field where Sink stores internal metadata such as the record revision; removed from documents returned to clients. |
| `storages[].search.endpoints` | list of strings | Conditionally | none | One or more HTTP(S) endpoints | Required for `elasticsearch` and `opensearch`. |
| `storages[].search.username` | string | Conditionally | empty | Any username accepted by the search service | Basic-auth username. Must be configured together with `password`. |
| `storages[].search.password` | string | Conditionally | empty | Any password accepted by the search service | Basic-auth password. Must be configured together with `username`. |
| `storages[].search.api_key` | string | No | empty | Any API key accepted by the search service | API key used instead of basic authentication. |
| `service.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `service.max_merge_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum attempts for a merge after revision conflicts. |
| `kafka.brokers` | list of strings | Conditionally | `[]` | Zero or more Kafka bootstrap addresses | Configure together with `kafka.topic` to enable durable asynchronous acceptance. Required in `worker` and `all` modes. |
| `kafka.topic` | string | Conditionally | empty | Any valid Kafka topic name | Topic used for durable mutation publication and consumption. Required with brokers. |
| `kafka.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Required in `worker` and `all` modes. |
| `kafka.max_poll_records` | positive integer | No | `500` | Integer greater than `0` | Maximum number of mutations handled in one consumer fetch batch. |
| `shutdown_timeout_seconds` | positive integer | No | `15` | Integer greater than `0` | Maximum graceful-shutdown time for gRPC and MongoDB disconnect operations. |

### Mode values

| Value | Behavior |
| --- | --- |
| `server` | Opens the gRPC listener and processes API requests. It publishes asynchronous mutations when Kafka brokers and a topic are configured. |
| `worker` | Consumes and applies Kafka mutations without opening the gRPC listener. Kafka brokers, topic, and group ID are required. |
| `all` | Runs the `server` and `worker` roles in one process. Kafka brokers, topic, and group ID are required. |

### Storage driver values

| Value | Behavior | Required driver-specific configuration |
| --- | --- | --- |
| `mongodb` | Stores BSON documents in MongoDB. | `storages[].mongodb.uri` |
| `elasticsearch` | Stores JSON documents in Elasticsearch. | At least one `storages[].search.endpoints` entry |
| `opensearch` | Stores JSON documents in OpenSearch. | At least one `storages[].search.endpoints` entry |

## Kafka mode combinations

- A `server` without Kafka handles synchronous operations only.
- A `server` with `kafka.brokers` and `kafka.topic` can publish durable
  asynchronous mutations; it does not require `kafka.group_id`.
- A `worker` requires brokers, topic, and group ID and does not open the gRPC
  listen address.
- `all` requires brokers, topic, and group ID and runs both publishing and
  consuming in one process.

## Handling credentials

The configuration can contain database URIs, passwords, or API keys. Do not
commit a production configuration file. Limit its filesystem permissions and
mount it read-only in the container. The example files contain placeholders or
local-development values only.
