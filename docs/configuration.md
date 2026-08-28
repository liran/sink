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
multiple YAML documents, invalid positive-integer values, and incompatible
option combinations prevent the process from starting. Restart Sink after
changing the file.

## Complete example

The repository's [`config.example.yaml`](../config.example.yaml) is a ready-to-
edit synchronous MongoDB configuration:

```yaml
mode: server

grpc:
  address: ":8080"

storage:
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
    store: primary
    hidden_field: __sink
    bindings: []

service:
  max_operations: 1000
  max_merge_attempts: 3

kafka:
  max_poll_records: 500

shutdown_timeout_seconds: 15
```

## Configuration reference

“Conditionally required” means a field is mandatory only in the modes or with
the drivers stated in its description. Enum values are case-sensitive and must
use the lowercase spelling shown below.

| Field | Type | Required | Default | Allowed values | Function |
| --- | --- | --- | --- | --- | --- |
| `mode` | enum string | No | `server` | `server`, `worker`, `all` | Process role. See [Mode values](#mode-values). |
| `grpc.address` | string | No | `:8080` | Any valid TCP listen address | TCP listen address for the gRPC and gRPC health services. Used in `server` and `all` modes. |
| `storage.driver` | enum string | No | `mongodb` | `mongodb`, `elasticsearch`, `opensearch` | Storage adapter. See [Storage driver values](#storage-driver-values). |
| `storage.mongodb.uri` | string | Conditionally | none | Valid MongoDB connection string | MongoDB connection string. Required when `storage.driver` is `mongodb`. |
| `storage.mongodb.store` | string | No | `primary` | Any non-empty logical store name | Logical store name that requests must use with the MongoDB adapter. |
| `storage.mongodb.hidden_field` | string | No | `__sink` | Any valid MongoDB field except `_id`; cannot contain `.`, `$`, or a null byte | Top-level field used for Sink's hidden revision metadata. |
| `storage.mongodb.bindings` | list | No | `[]` | Zero or more valid MongoDB binding objects | Maps logical namespace/dataset pairs to physical databases/collections. With an empty list, names map directly. When the list is non-empty, only listed datasets are accepted. |
| `storage.search.endpoints` | list of strings | Conditionally | none | One or more HTTP(S) endpoints | Elasticsearch or OpenSearch endpoints. At least one is required for either search driver. |
| `storage.search.store` | string | No | `primary` | Any non-empty logical store name | Logical store name that requests must use with a search adapter. |
| `storage.search.bindings` | list | No | `[]` | Zero or more valid search binding objects | Maps logical namespace/dataset pairs to existing indexes or aliases. With an empty list, the physical name is `<namespace>-<dataset>`. When the list is non-empty, only listed datasets are accepted. |
| `storage.search.username` | string | Conditionally | empty | Any username accepted by the search service | HTTP basic-auth username. Must be configured together with `storage.search.password`. |
| `storage.search.password` | string | Conditionally | empty | Any password accepted by the search service | HTTP basic-auth password. Must be configured together with `storage.search.username`. |
| `storage.search.api_key` | string | No | empty | Any API key accepted by the search service | Search API key. It is mutually exclusive with username/password authentication. |
| `service.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `service.max_merge_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum attempts for a merge after revision conflicts. |
| `kafka.brokers` | list of strings | Conditionally | `[]` | Zero or more Kafka bootstrap addresses | Configure it together with `kafka.topic` to enable durable asynchronous acceptance. Required in `worker` and `all` modes. |
| `kafka.topic` | string | Conditionally | empty | Any valid Kafka topic name | Topic used for durable mutation publication and consumption. Configure it together with `kafka.brokers`; required in `worker` and `all` modes. |
| `kafka.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Kafka consumer group. Required in `worker` and `all` modes. |
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
| `mongodb` | Stores BSON documents in MongoDB. | `storage.mongodb.uri` |
| `elasticsearch` | Stores JSON documents in Elasticsearch. | At least one `storage.search.endpoints` entry |
| `opensearch` | Stores JSON documents in OpenSearch. | At least one `storage.search.endpoints` entry |

### MongoDB bindings

Each `storage.mongodb.bindings` entry has four required string fields:

| Field | Function |
| --- | --- |
| `namespace` | Logical namespace received through the Sink API. |
| `dataset` | Logical dataset received through the Sink API. |
| `database` | Existing MongoDB database to use. |
| `collection` | Existing MongoDB collection to use. |

Logical namespace/dataset pairs must be unique:

```yaml
storage:
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
    bindings:
      - namespace: logical
        dataset: records
        database: legacy
        collection: documents
```

### Elasticsearch and OpenSearch bindings

Each `storage.search.bindings` entry has three required string fields:

| Field | Function |
| --- | --- |
| `namespace` | Logical namespace received through the Sink API. |
| `dataset` | Logical dataset received through the Sink API. |
| `index` | Existing index or alias to use. |

Logical namespace/dataset pairs must be unique. For example:

```yaml
mode: server

storage:
  driver: elasticsearch
  search:
    endpoints:
      - https://search-1:9200
      - https://search-2:9200
    api_key: replace-with-api-key
    bindings:
      - namespace: logical
        dataset: records
        index: legacy-records
```

Use `opensearch` as the driver for OpenSearch; the remaining search fields are
shared.

## Kafka mode combinations

- A `server` without Kafka handles synchronous operations only.
- A `server` with `kafka.brokers` and `kafka.topic` can publish durable
  asynchronous mutations; it does not require `kafka.group_id`.
- A `worker` requires brokers, topic, and group ID and does not open the gRPC
  listen address.
- `all` requires brokers, topic, and group ID and runs both publishing and
  consuming in one process.

## Handling credentials

The configuration can contain a database URI, password, or API key. Do not
commit a production configuration file. Limit its filesystem permissions and
mount it read-only in the container. The example files contain placeholders or
local-development values only.
