package service

import (
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
)

func TestReturnedChainDoesNotBlockUnrelatedPut(t *testing.T) {
	for _, returned := range []bool{false, true} {
		for _, slowFirst := range []bool{false, true} {
			backend := newHeldReadStorage(t)
			server := completionServer(t, backend)
			mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
			fast := completionWriteCall(t.Context(), mode, completionPut("fast", 1))
			first := completionMerge("slow", 1)
			first.ReturnDocument = returned
			second := completionMerge("slow", 1)
			second.ReturnDocument = returned
			slow := completionWriteCall(t.Context(), mode, first, second)
			calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{fast, slow}
			if slowFirst {
				calls[0], calls[1] = calls[1], calls[0]
			}
			done := make(chan struct{})
			go func() {
				server.executeWrites(t.Context(), calls)
				close(done)
			}()
			awaitCompletion(t, backend.entered)
			select {
			case result := <-fast.result:
				if result.err != nil {
					t.Errorf("unrelated put failed: %v", result.err)
				}
			case <-time.After(time.Second):
				t.Errorf("unrelated put blocked by returned=%v slowFirst=%v", returned, slowFirst)
			}
			backend.unblock()
			awaitCompletion(t, done)
			requireWriteResult(t, slow)
		}
	}
}
