package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/twmb/franz-go/pkg/kfake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestApplicationAlwaysBatchesGRPCRequests(t *testing.T) {
	for _, mode := range []runMode{modeServer, modeAll} {
		t.Run(string(mode), func(t *testing.T) {
			kafkaConfig := ""
			if mode == modeAll {
				broker, err := kfake.NewCluster(kfake.NumBrokers(1))
				if err != nil {
					t.Fatal(err)
				}
				defer broker.Close()
				kafkaConfig = fmt.Sprintf(`    kafka:
      enabled: true
      brokers: [%q]
      topic: batching-test
      group_id: batching-test
      topic_replication_factor: 1
      min_insync_replicas: 1
`, broker.ListenAddrs()[0])
			}
			var calls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/_mget" {
					http.NotFound(w, r)
					return
				}
				calls.Add(1)
				var body struct {
					Documents []map[string]any `json:"docs"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode backend request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if len(body.Documents) != 2 {
					t.Errorf("backend received %d operations, want two coalesced RPCs", len(body.Documents))
				}
				for _, document := range body.Documents {
					document["found"] = false
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(body); err != nil {
					t.Errorf("encode backend response: %v", err)
				}
			}))
			defer backend.Close()

			contents := fmt.Sprintf(`mode: %s
grpc:
  address: "127.0.0.1:0"
storages:
  - name: primary
    driver: opensearch
    search:
      endpoints: [%q]
%s
service:
  batching:
    max_operations: 2
    max_wait_milliseconds: 1000
`, mode, backend.URL, kafkaConfig)
			path := writeConfig(t, contents)
			loaded, err := loadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			app, err := newApplication(t.Context(), loaded)
			if err != nil {
				t.Fatal(err)
			}
			serveErrors := make(chan error, 1)
			go func() { serveErrors <- app.grpcServer.Serve(app.listener) }()
			defer func() {
				app.close()
				if err := <-serveErrors; err != nil {
					t.Errorf("serve gRPC: %v", err)
				}
			}()
			connection, err := grpc.NewClient(app.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := sink.NewSinkClient(connection)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var callers sync.WaitGroup
			for index := range 2 {
				callers.Go(func() {
					keyValue := &sink.RecordKey_StringValue{StringValue: fmt.Sprintf("key-%d", index)}
					key := &sink.RecordKey{Kind: keyValue}
					address := &sink.RecordAddress{Store: "primary", Namespace: "app", Dataset: "items", Key: key}
					operation := &sink.ReadOperation{Address: address}
					request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
					response, err := client.Read(ctx, request)
					if err != nil {
						t.Errorf("read: %v", err)
						return
					}
					if len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
						t.Errorf("read response lost its RPC boundary: %v", response)
					}
				})
			}
			callers.Wait()
			if count := calls.Load(); count != 1 {
				t.Fatalf("backend read calls = %d, want one batch", count)
			}
		})
	}
}
