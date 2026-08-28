.PHONY: proto build test test-unit test-integration lint clean

PROTO_DIR := proto
GEN_DIR := gen
STATICCHECK_VERSION := v0.8.1

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

lint:
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=all ./...
	gofmt -s -w .
