package service_test

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryAndCountRouteCommonCommandAndPageWithoutOverflow(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.queries = make(chan storage.QueryRequest, 1)
	backend.counts = make(chan storage.CountRequest, 1)
	command := nativeSearchRequest().Command
	header := &sink.Header{Name: "X-Option", Values: []string{"first", "second"}}
	command.Headers = []*sink.Header{header}
	command.Query = "q=a&q=b"
	request := &sink.QueryRequest{Command: command, Page: ^uint32(0), PageSize: 1000}
	if _, err := client.Query(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	captured := <-backend.queries
	if captured.Offset != 4294967294000 || captured.PageSize != 1000 || captured.Request.Query != command.Query || len(captured.Request.Headers.Values("X-Option")) != 2 || string(captured.Request.Payload) != "{}" {
		t.Fatalf("query lost pagination or command: %+v", captured)
	}
	request.Page, request.PageSize = 0, 0
	if _, err := client.Query(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	captured = <-backend.queries
	if captured.Offset != 0 || captured.PageSize != 100 {
		t.Fatalf("incorrect defaults: %+v", captured)
	}
	request.PageSize = 1001
	if _, err := client.Query(t.Context(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid page size reached backend: %v", err)
	}
	countRequest := &sink.CountRequest{Command: command, Estimate: true}
	response, err := client.Count(t.Context(), countRequest)
	if err != nil || response.GetCount() != 123 || !response.GetEstimated() {
		t.Fatalf("count=%v err=%v", response, err)
	}
	countCommand := (<-backend.counts).Request
	if countCommand.Store != command.Store || countCommand.ContentType != command.ContentType || string(countCommand.Payload) != string(command.Payload) {
		t.Fatalf("count command changed: %+v", countCommand)
	}
	countRequest.Estimate = false
	response, err = client.Count(t.Context(), countRequest)
	if err != nil || response.GetEstimated() || (<-backend.counts).Estimate {
		t.Fatalf("exact count lost: %v err=%v", response, err)
	}
	command.Store = "missing"
	if _, err := client.Count(t.Context(), countRequest); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown count store: %v", err)
	}
}

func TestNativeCommandRejectsMissingEncodingBeforeDispatch(t *testing.T) {
	client, _ := nativeRPCFixture(t, false)
	request := nativeSearchRequest()
	request.Command.ContentType = ""
	if _, err := client.Execute(t.Context(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("untyped payload accepted: %v", err)
	}
	request.Command = nil
	if _, err := client.Execute(t.Context(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing command accepted: %v", err)
	}
}
