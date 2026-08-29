package merge_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
)

func TestLuaMergePreservesJSONTypesAndInt64(t *testing.T) {
	source := []byte(`
return function(current, incoming, context)
    current = current or json.object()
    current.id = incoming.id
    current.null_value = incoming.null_value
    current.empty_array = incoming.empty_array
    current.empty_object = incoming.empty_object
    current.created_array = json.array()
    current.created_object = json.object()
    current.observed_at = context.observed_at
    return current
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	current := jsonDocument(`{"keep":true}`)
	incoming := jsonDocument(`{"id":9223372036854775807,"null_value":null,"empty_array":[],"empty_object":{}}`)
	observedAt := time.Date(2026, time.August, 29, 10, 11, 12, 123, time.UTC)
	request := merge.Request{Current: &current, Incoming: incoming, ObservedAt: observedAt}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"created_array":[],"created_object":{},"empty_array":[],"empty_object":{},"id":9223372036854775807,"keep":true,"null_value":null,"observed_at":"2026-08-29T10:11:12.000000123Z"}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
}

func TestLuaMergeRunsProductRule(t *testing.T) {
	source := productMergeSource(t)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	current := jsonDocument(`{"uid":"old","uids":["legacy"],"brand":"OLD","allowed_countries":["US"],"solds":[{"sold":1,"period_hours":24,"record_at":"2026-08-01T00:00:00Z"}]}`)
	incoming := jsonDocument(`{"uid":"new","uids":["legacy","new"],"brand":"new brand","allowed_countries":["US","JP"],"solds":[{"sold":2,"period_hours":24,"record_at":"2026-08-02T00:00:00Z"}],"available":true}`)
	observedAt := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	request := merge.Request{Current: &current, Incoming: incoming, ObservedAt: observedAt}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	var product struct {
		UID              string   `json:"uid"`
		UIDs             []string `json:"uids"`
		Brand            string   `json:"brand"`
		AllowedCountries []string `json:"allowed_countries"`
		Solds            []any    `json:"solds"`
		Available        bool     `json:"available"`
		LastFoundAt      string   `json:"last_found_at"`
	}
	if err := json.Unmarshal(result.Document.JSON, &product); err != nil {
		t.Fatalf("decode merged product: %v", err)
	}
	if product.UID != "new" || product.Brand != "NEW BRAND" || !product.Available || product.LastFoundAt != "2026-08-29T12:00:00Z" {
		t.Fatalf("merged product = %+v", product)
	}
	if len(product.UIDs) != 3 || len(product.AllowedCountries) != 2 || len(product.Solds) != 2 {
		t.Fatalf("merged product collections = %+v", product)
	}
}

func BenchmarkLuaMergeProduct5KB(b *testing.B) {
	source := productMergeSource(b)
	options := merge.LuaOptions{}
	merger := compileTestProgram(b, source, options)
	currentValue := map[string]any{
		"uid":         "product-1",
		"title":       "current",
		"description": strings.Repeat("current product data ", 250),
		"uids":        []string{"legacy-product-1"},
	}
	incomingValue := map[string]any{
		"uid":         "product-1",
		"title":       "incoming",
		"description": strings.Repeat("incoming product data ", 240),
		"available":   true,
	}
	currentJSON, err := json.Marshal(currentValue)
	if err != nil {
		b.Fatalf("encode current product: %v", err)
	}
	incomingJSON, err := json.Marshal(incomingValue)
	if err != nil {
		b.Fatalf("encode incoming product: %v", err)
	}
	current := jsonDocument(string(currentJSON))
	incoming := jsonDocument(string(incomingJSON))
	request := merge.Request{Current: &current, Incoming: incoming, ObservedAt: time.Now()}
	b.SetBytes(int64(len(currentJSON) + len(incomingJSON)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := merger.Merge(context.Background(), request); err != nil {
			b.Fatalf("Merge() error = %v", err)
		}
	}
}

func TestLuaMergeUsesFreshVMForEveryCall(t *testing.T) {
	source := []byte(`
counter = 0
return function(current, incoming, context)
    counter = counter + 1
    return {counter = counter}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: time.Now()}
	for range 2 {
		result, err := merger.Merge(context.Background(), request)
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if string(result.Document.JSON) != `{"counter":1}` {
			t.Fatalf("Merge() document = %s", result.Document.JSON)
		}
	}
}

func TestLuaMergePresentsObjectKeysDeterministically(t *testing.T) {
	source := []byte(`
return function(current, incoming, context)
    local keys = {}
    for key in pairs(incoming) do
        keys[#keys + 1] = key
    end
    return {keys = keys}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{"z":1,"a":2,"m":3}`), ObservedAt: time.Now()}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if string(result.Document.JSON) != `{"keys":["a","m","z"]}` {
		t.Fatalf("Merge() document = %s", result.Document.JSON)
	}
}

func TestLuaMergeSupportsConcurrentCalls(t *testing.T) {
	source := []byte(`return function(current, incoming, context) return incoming end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{"value":1}`), ObservedAt: time.Now()}

	const calls = 32
	var waitGroup sync.WaitGroup
	waitGroup.Add(calls)
	for range calls {
		go func() {
			defer waitGroup.Done()
			result, err := merger.Merge(context.Background(), request)
			if err != nil {
				t.Errorf("Merge() error = %v", err)
				return
			}
			if string(result.Document.JSON) != `{"value":1}` {
				t.Errorf("Merge() document = %s", result.Document.JSON)
			}
		}()
	}
	waitGroup.Wait()
}

func TestLuaMergeRestrictsNondeterministicAndHostAPIs(t *testing.T) {
	source := []byte(`
return function(current, incoming, context)
    return {
        ["global"] = _G ~= nil,
        io = io ~= nil,
        load = load ~= nil,
        os = os ~= nil,
        package = package ~= nil,
        random = math.random ~= nil,
        require = require ~= nil,
        time = time ~= nil
    }
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: time.Now()}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"global":false,"io":false,"load":false,"os":false,"package":false,"random":false,"require":false,"time":false}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
}

func TestLuaEngineValidatesProgram(t *testing.T) {
	options := merge.LuaOptions{MaxSourceBytes: 8}
	engine, err := merge.NewLuaEngine(options)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}

	tests := []struct {
		name    string
		program merge.Program
	}{
		{name: "empty", program: merge.Program{}},
		{name: "too large", program: merge.Program{Source: []byte("123456789")}},
		{name: "invalid digest length", program: merge.Program{Source: []byte("return 1"), SHA256: []byte{1}}},
		{name: "digest mismatch", program: merge.Program{Source: []byte("return 1"), SHA256: make([]byte, sha256.Size)}},
		{name: "not a function", program: merge.Program{Source: []byte("return 1")}},
		{name: "syntax", program: merge.Program{Source: []byte("return @")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := engine.Compile(test.program)
			if !errors.Is(err, merge.ErrInvalidProgram) {
				t.Fatalf("Compile() error = %v", err)
			}
		})
	}
}

func TestLuaMergeEnforcesExecutionLimits(t *testing.T) {
	t.Run("instructions", func(t *testing.T) {
		options := merge.LuaOptions{MaxInstructions: 100}
		source := []byte(`return function(current, incoming, context) while true do end end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: time.Now()}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		options := merge.LuaOptions{Timeout: time.Nanosecond, MaxInstructions: 10_000_000}
		source := []byte(`return function(current, incoming, context) while true do end end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: time.Now()}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionDeadline) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("result bytes", func(t *testing.T) {
		options := merge.LuaOptions{MaxResultBytes: 8}
		source := []byte(`return function(current, incoming, context) return {value = "too large"} end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: time.Now()}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) {
			t.Fatalf("Merge() error = %v", err)
		}
	})
}

func TestLuaMergeClassifiesDocumentAndScriptErrors(t *testing.T) {
	t.Run("incoming", func(t *testing.T) {
		source := []byte(`return function(current, incoming, context) return incoming end`)
		options := merge.LuaOptions{}
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: storage.Document{JSON: []byte("bad")}}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrInvalidIncoming) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("current", func(t *testing.T) {
		source := []byte(`return function(current, incoming, context) return incoming end`)
		options := merge.LuaOptions{}
		merger := compileTestProgram(t, source, options)
		current := jsonDocument(`not-json`)
		request := merge.Request{Current: &current, Incoming: jsonDocument(`{}`)}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrInvalidCurrent) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("runtime", func(t *testing.T) {
		source := []byte(`return function(current, incoming, context) error("bad rule") end`)
		options := merge.LuaOptions{}
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`)}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecution) {
			t.Fatalf("Merge() error = %v", err)
		}
	})
}

func compileTestProgram(t testing.TB, source []byte, options merge.LuaOptions) merge.Merger {
	t.Helper()
	engine, err := merge.NewLuaEngine(options)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	digest := sha256.Sum256(source)
	program := merge.Program{Source: source, SHA256: digest[:]}
	merger, err := engine.Compile(program)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return merger
}

func productMergeSource(t testing.TB) []byte {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	sourcePath := filepath.Join(filepath.Dir(filename), "..", "..", "benchmarks", "lua", "product_merge.lua")
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read product merge program: %v", err)
	}
	return source
}

func jsonDocument(value string) storage.Document {
	document := storage.Document{JSON: []byte(value)}
	return document
}
