package service_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type nativeFixtureStorage struct {
	*memory.Store
	stopped chan struct{}
	silent  bool
	started chan struct{}
	queries chan storage.QueryRequest
	counts  chan storage.CountRequest
}

func (s *nativeFixtureStorage) Query(_ context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	if s.queries != nil {
		s.queries <- req
	}
	result := storage.QueryResponse{}
	return result, nil
}

func (s *nativeFixtureStorage) Count(_ context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	if s.counts != nil {
		s.counts <- req
	}
	result := storage.CountResponse{Count: 123, Estimated: true}
	return result, nil
}

func (s *nativeFixtureStorage) Execute(_ context.Context, _ storage.NativeRequest) (storage.NativeResponse, error) {
	response := storage.NativeResponse{ContentType: "application/json", Payload: []byte("{\"error\":\"native\"}\n"), StatusCode: 400,
		Headers: http.Header{"Warning": {"first", "second"}}}
	return response, nil
}

func (s *nativeFixtureStorage) Scan(ctx context.Context, _ storage.ScanRequest) (storage.ScanResponse, error) {
	var response storage.ScanResponse
	if s.silent {
		close(s.started)
		defer close(s.stopped)
		<-ctx.Done()
		return response, ctx.Err()
	}
	document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"value":1}`)}
	response.Documents = []storage.Document{document}
	return response, nil
}

func nativeRPCFixture(t *testing.T, silent bool) (sink.SinkClient, *nativeFixtureStorage) {
	t.Helper()
	backend := &nativeFixtureStorage{Store: memory.New(), stopped: make(chan struct{}), started: make(chan struct{}), silent: silent}
	backends := map[string]storage.Storage{"primary": backend}
	router, err := storage.NewRouter(backends)
	if err != nil {
		t.Fatal(err)
	}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	idle := time.Second
	if silent {
		idle = 200 * time.Millisecond
	}
	options := service.Options{Storage: router, Lua: lua, MaxInFlightRequests: 1, MaxReadBytes: 4096,
		RequestTimeout: idle, StoreNames: []string{"primary"}}
	core, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := service.BatchingOptions{StoreNames: []string{"primary"}}
	server, err := service.NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	codec := protocol.NewVTProtoCodec()
	grpcServer := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	sink.RegisterSinkServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }
	connection, err := grpc.NewClient("passthrough:///native", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); grpcServer.Stop(); server.Close(); _ = listener.Close() })
	return sink.NewSinkClient(connection), backend
}

func nativeSearchRequest() *sink.ExecuteRequest {
	command := &sink.Command{Store: "primary", Method: "POST", Path: "/products/_search", ContentType: "application/json", Payload: []byte(`{}`)}
	request := &sink.ExecuteRequest{Command: command}
	return request
}

func TestNativeRPCSharesAdmissionAndReleasesCanceledScan(t *testing.T) {
	client, backend := nativeRPCFixture(t, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := nativeSearchRequest()
	scan := &sink.ScanRequest{Command: request.Command, BatchSize: 1}
	finished := make(chan error, 1)
	go func() { _, err := client.Scan(ctx, scan); finished <- err }()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("scan did not start")
	}
	_, err := client.Execute(t.Context(), request)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("scan bypassed shared admission: %v", err)
	}
	cancel()
	if err := <-finished; status.Code(err) != codes.Canceled {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-backend.stopped:
	case <-time.After(time.Second):
		t.Fatal("canceled scan retained backend resources")
	}
	var response *sink.ExecuteResponse
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err = client.Execute(t.Context(), request)
		if status.Code(err) != codes.ResourceExhausted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || response.GetStatusCode() != 400 || response.GetSuccess() || len(response.GetHeaders()[0].GetValues()) != 2 {
		t.Fatalf("native result=%v err=%v", response, err)
	}
}

func TestNativeScanUsesOrdinaryRequestTimeout(t *testing.T) {
	client, backend := nativeRPCFixture(t, true)
	request := &sink.ScanRequest{Command: nativeSearchRequest().Command}
	_, err := client.Scan(t.Context(), request)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("scan error=%v", err)
	}
	select {
	case <-backend.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed-out scan retained resources")
	}
}

func TestNativeScanReturnsOneUnaryPageAndReleasesAdmission(t *testing.T) {
	client, _ := nativeRPCFixture(t, false)
	request := &sink.ScanRequest{Command: nativeSearchRequest().Command}
	for range 3 {
		page, err := client.Scan(t.Context(), request)
		if err != nil || len(page.GetDocuments()) != 1 || len(page.GetNextCursor()) != 0 {
			t.Fatalf("page=%v err=%v", page, err)
		}
	}
	request.Cursor = []byte("invalid")
	_, err := client.Scan(t.Context(), request)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor=%v", err)
	}
}
