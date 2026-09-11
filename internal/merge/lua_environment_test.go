package merge_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/liran/sink/internal/merge"
)

func TestLuaEnvironmentIsFreshAfterMutationAndFailure(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    assert(marker == nil)
    assert(string.upper("hello") == "HELLO")
    assert(("hello"):upper() == "HELLO")
    assert(math.max(1, 2) == 2)
    assert(table.concat({"a", "b"}) == "ab")
    assert(utf8.upper("café") == "CAFÉ")
    assert(_G == nil and io == nil and os == nil and package == nil)
    assert(load == nil and require == nil and debug == nil)
    assert(math.random == nil and string.rep == nil)
    local observed = sink.v1.time.now()
    marker = true
    string.upper = function() return "leaked" end
    math.max = nil
    table.concat = nil
    utf8.upper = nil
    sink.v1.time.now = function() return "leaked" end
    if incoming.fail then error("deliberate failure") end
    return {observed=observed}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	base := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	failed := merge.Request{Incoming: jsonDocument(`{"fail":true}`), ObservedAt: base}
	if _, err := merger.Merge(t.Context(), failed); err == nil {
		t.Fatal("deliberate script failure succeeded")
	}
	var executions sync.WaitGroup
	for index := range 64 {
		executions.Go(func() {
			observed := base.Add(time.Duration(index) * time.Second)
			request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: observed}
			result, err := merger.Merge(t.Context(), request)
			if err != nil {
				t.Error(err)
				return
			}
			var document map[string]string
			if err := json.Unmarshal(result.Document.Payload, &document); err != nil {
				t.Error(err)
			}
			if document["observed"] != observed.Format(time.RFC3339Nano) {
				t.Errorf("request-bound time leaked: %s", result.Document.Payload)
			}
		})
	}
	executions.Wait()
}
