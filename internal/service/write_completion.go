package service

import (
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type writeResultOwner struct {
	caller int
	index  int
	key    recordIdentity
	valid  bool
	done   bool
}

// A core execution publishes only final results. Speculative conditional/Lua
// failures stay private until their document chain commits or fails definitively.
type writeCompletion struct {
	calls     []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
	owners    []writeResultOwner
	responses []*sink.WriteResponse
	remaining []int
	records   []map[recordIdentity]int
}

func newWriteCompletion(
	calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse],
	identity func(storage.Address) recordIdentity,
) *writeCompletion {
	completion := &writeCompletion{calls: calls}
	for caller, call := range calls {
		operations := call.request.GetOperations()
		response := &sink.WriteResponse{Results: make([]*sink.WriteResult, len(operations))}
		completion.responses = append(completion.responses, response)
		completion.remaining = append(completion.remaining, len(operations))
		records := make(map[recordIdentity]int)
		for index, operation := range operations {
			owner := writeResultOwner{caller: caller, index: index}
			address, err := convertAddress(operation.GetAddress())
			if err == nil {
				owner.valid = true
				owner.key = identity(address)
				records[owner.key]++
			}
			completion.owners = append(completion.owners, owner)
		}
		completion.records = append(completion.records, records)
	}
	return completion
}

func (c *writeCompletion) operation(index int, result *sink.WriteResult) {
	if c == nil || c.owners[index].done {
		return
	}
	owner := &c.owners[index]
	owner.done = true
	// Returned protobufs must not share mutable result state with the executor.
	cloned := proto.Clone(result).(*sink.WriteResult)
	cloned.OperationIndex = uint32(owner.index)
	c.responses[owner.caller].Results[owner.index] = cloned
	c.remaining[owner.caller]--
	if owner.valid {
		c.records[owner.caller][owner.key]--
		if c.records[owner.caller][owner.key] == 0 {
			c.calls[owner.caller].finishRecords([]recordIdentity{owner.key})
		}
	}
	if c.remaining[owner.caller] == 0 {
		completeCall(c.calls[owner.caller], c.responses[owner.caller], nil)
	}
}

func (c *writeCompletion) group(group writeGroup, results []*sink.WriteResult) {
	if c == nil {
		return
	}
	for _, operation := range group.operations {
		c.operation(operation.index, results[operation.index])
	}
}

func (c *writeCompletion) finish(response *sink.WriteResponse, err error) {
	if err == nil && (response == nil || len(response.Results) != len(c.owners)) {
		err = status.Error(codes.Internal, "batched write returned an invalid result count")
	}
	if err != nil {
		for caller, call := range c.calls {
			if c.remaining[caller] > 0 {
				completeCall(call, (*sink.WriteResponse)(nil), err)
			}
		}
		return
	}
	for index, result := range response.Results {
		c.operation(index, result)
	}
}
