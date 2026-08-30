package protocol

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

const vtProtoCodecName = "proto"

type vtProtoCodecMessage interface {
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

// VTProtoCodec uses generated VT helpers and delegates messages without them
// to the standard gRPC protobuf codec.
type VTProtoCodec struct {
	fallback encoding.CodecV2
	pool     mem.BufferPool
}

// NewVTProtoCodec creates a scoped CodecV2 with standard protobuf fallback.
func NewVTProtoCodec() *VTProtoCodec {
	codec := &VTProtoCodec{
		fallback: encoding.GetCodecV2(vtProtoCodecName),
		pool:     mem.DefaultBufferPool(),
	}
	return codec
}

func (*VTProtoCodec) Name() string {
	return vtProtoCodecName
}

func (c *VTProtoCodec) Marshal(value any) (mem.BufferSlice, error) {
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		if c.fallback == nil {
			return nil, fmt.Errorf("vtproto: no protobuf fallback for message %T", value)
		}
		return c.fallback.Marshal(value)
	}

	size := message.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buffer := make([]byte, size)
		written, err := message.MarshalToSizedBufferVT(buffer)
		if err != nil {
			return nil, err
		}
		if written != size {
			return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
		}
		encoded := mem.BufferSlice{mem.SliceBuffer(buffer)}
		return encoded, nil
	}

	pooled := c.pool.Get(size)
	buffer := (*pooled)[:size]
	written, err := message.MarshalToSizedBufferVT(buffer)
	if err != nil {
		c.pool.Put(pooled)
		return nil, err
	}
	if written != size {
		c.pool.Put(pooled)
		return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
	}
	encoded := mem.BufferSlice{mem.NewBuffer(pooled, c.pool)}
	return encoded, nil
}

func (c *VTProtoCodec) Unmarshal(data mem.BufferSlice, value any) error {
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		if c.fallback == nil {
			return fmt.Errorf("vtproto: no protobuf fallback for message %T", value)
		}
		return c.fallback.Unmarshal(data, value)
	}
	buffer := data.MaterializeToBuffer(c.pool)
	defer buffer.Free()
	return message.UnmarshalVT(buffer.ReadOnlyData())
}
