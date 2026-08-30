package protocol_test

import (
	"bytes"
	"testing"

	"github.com/liran/sink/internal/protocol"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

type trackedVTMessage struct {
	data        []byte
	marshaled   bool
	unmarshaled bool
}

func (m *trackedVTMessage) MarshalToSizedBufferVT(buffer []byte) (int, error) {
	m.marshaled = true
	return copy(buffer, m.data), nil
}

func (m *trackedVTMessage) UnmarshalVT(data []byte) error {
	m.unmarshaled = true
	m.data = append(m.data[:0], data...)
	return nil
}

func (m *trackedVTMessage) SizeVT() int {
	return len(m.data)
}

func TestVTProtoCodecUsesGeneratedHelpers(t *testing.T) {
	codec := protocol.NewVTProtoCodec()
	message := &trackedVTMessage{data: []byte("sink")}
	encoded, err := codec.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	defer encoded.Free()
	if !message.marshaled {
		t.Fatal("Marshal() did not use the VT helper")
	}
	decoded := &trackedVTMessage{}
	if err := codec.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !decoded.unmarshaled || !bytes.Equal(decoded.data, message.data) {
		t.Fatalf("Unmarshal() message = %+v", decoded)
	}
}

func TestVTProtoCodecFallsBackForGRPCHealth(t *testing.T) {
	codec := protocol.NewVTProtoCodec()
	request := &healthpb.HealthCheckRequest{Service: "sink"}
	encoded, err := codec.Marshal(request)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	defer encoded.Free()
	decoded := &healthpb.HealthCheckRequest{}
	if err := codec.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !proto.Equal(decoded, request) {
		t.Fatalf("round-trip request = %v, want %v", decoded, request)
	}
}
