.PHONY: proto build test test-unit test-integration test-search-integration fmt check-format lint lint-workflows lint-docs quickstart quickstart-down

PROTO_DIR := proto
GEN_DIR := gen
STATICCHECK_VERSION := v0.8.1
ACTIONLINT_VERSION := v1.7.12
LYCHEE ?= lychee

build:
	go build -o bin/sink ./cmd/sink

proto:
	@mkdir -p $(GEN_DIR)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--go-vtproto_out=$(GEN_DIR) --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size+pool \
		$(PROTO_DIR)/sink/sink.proto
	@printf '%s\n%s\n' '// Package sink contains generated protobuf definitions for the Sink gRPC service.' 'package sink' > $(GEN_DIR)/sink/doc.go

test:
	go test ./... -v -count=1

test-unit:
	go test ./internal/... -v -count=1

test-integration:
	bash scripts/test-mongodb-integration.sh
	bash scripts/test-search-integration.sh elasticsearch
	bash scripts/test-search-integration.sh opensearch

test-search-integration:
	bash scripts/test-search-integration.sh elasticsearch
	bash scripts/test-search-integration.sh opensearch

quickstart:
	bash examples/quickstart/run.sh

quickstart-down:
	docker compose --file examples/quickstart/compose.yaml down

fmt:
	gofmt -s -w .

check-format:
	@set -e; files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' "$$files"; \
		printf '%s\n' 'Run make fmt to format these files.'; \
		exit 1; \
	fi

lint: check-format
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=all ./...

lint-workflows:
	go run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) -shellcheck=''

lint-docs:
	$(LYCHEE) --offline --include-fragments --no-progress '*.md' 'docs/**/*.md' 'examples/**/*.md' 'benchmarks/**/*.md' '.github/*.md'
