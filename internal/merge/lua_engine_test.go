package merge_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
)

func TestLuaMergePreservesDateTimeMetadata(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    return {
        created_at = incoming.created_at,
        literal = incoming.literal,
    }
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	incoming := storage.Document{
		JSON:          []byte(`{"created_at":"2026-08-29T04:34:56.789Z","literal":"2026-08-29T04:34:56.789Z"}`),
		DateTimePaths: []string{"/created_at"},
	}
	request := merge.Request{Incoming: incoming}
	result, err := merger.Merge(t.Context(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	wantPaths := []string{"/created_at"}
	if !slices.Equal(result.Document.DateTimePaths, wantPaths) {
		t.Fatalf("merged date-time paths = %v, want %v", result.Document.DateTimePaths, wantPaths)
	}
	var decoded map[string]string
	if err := json.Unmarshal(result.Document.JSON, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded["created_at"] != "2026-08-29T04:34:56.789Z" || decoded["literal"] != "2026-08-29T04:34:56.789Z" {
		t.Fatalf("merged document = %v", decoded)
	}
}

func TestLuaMergePreservesJSONTypesAndInt64(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    current = current or json.object()
    current.id = incoming.id
    current.null_value = incoming.null_value
    current.empty_array = incoming.empty_array
    current.empty_object = incoming.empty_object
    current.created_array = json.array()
    current.created_object = json.object()
    return current
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	current := jsonDocument(`{"keep":true}`)
	incoming := jsonDocument(`{"id":9223372036854775807,"null_value":null,"empty_array":[],"empty_object":{}}`)
	request := merge.Request{Current: &current, Incoming: incoming}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"created_array":[],"created_object":{},"empty_array":[],"empty_object":{},"id":9223372036854775807,"keep":true,"null_value":null}`
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
	request := merge.Request{Current: &current, Incoming: incoming}
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
	}
	if err := json.Unmarshal(result.Document.JSON, &product); err != nil {
		t.Fatalf("decode merged product: %v", err)
	}
	if product.UID != "new" || product.Brand != "NEW BRAND" || !product.Available {
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
	request := merge.Request{Current: &current, Incoming: incoming}
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
return function(current, incoming)
    counter = counter + 1
    return {counter = counter}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{}`)}
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
return function(current, incoming)
    local keys = {}
    for key in pairs(incoming) do
        keys[#keys + 1] = key
    end
    return {keys = keys}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{"z":1,"a":2,"m":3}`)}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if string(result.Document.JSON) != `{"keys":["a","m","z"]}` {
		t.Fatalf("Merge() document = %s", result.Document.JSON)
	}
}

func TestLuaMergeSupportsConcurrentCalls(t *testing.T) {
	source := []byte(`return function(current, incoming) return incoming end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{"value":1}`)}

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
return function(current, incoming)
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
	request := merge.Request{Incoming: jsonDocument(`{}`)}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"global":false,"io":false,"load":false,"os":false,"package":false,"random":false,"require":false,"time":false}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
}

func TestLuaMergeProvidesUnicodeUpper(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    return {brand = utf8.upper(incoming.brand)}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{"brand":"café Straße 品牌"}`)}
	result, err := merger.Merge(context.Background(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"brand":"CAFÉ STRAßE 品牌"}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
}

func TestLuaMergeProvidesSinkV1Utilities(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    local array = sink.v1.array
    local object = sink.v1.object
    local appended = json.array()
    local returned = array.append_all(appended, incoming.left)
    array.append_all(appended, incoming.right)
    array.append_all(appended, nil)

    local target = json.object()
    target.title = "old"
    target.gallery = incoming.previous_gallery
    object.replace_nonempty_string(target, incoming, "title")
    object.replace_nonempty_string(target, incoming, "empty_title")
    object.replace_nonempty_array(target, incoming, "gallery")
    object.replace_nonempty_array(target, incoming, "empty_gallery")

    local result = {
        append_returns_target = returned == appended,
        appended = appended,
        deduplicated = array.deduplicate(incoming.records, function(item) return item.id end),
        source_record_count = #incoming.records,
        tail = array.keep_tail(appended, 2),
        target = target,
        union = array.union_strings(incoming.current_tags, incoming.new_tags),
        v2_missing = sink.v2 == nil,
    }
    return result
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	incoming := jsonDocument(`{"left":[1,2],"right":[3,4],"records":[{"id":"a","value":1},{"id":"a","value":2},{"id":"b","value":3}],"previous_gallery":["old"],"title":"new","empty_title":"","gallery":["new"],"empty_gallery":[],"current_tags":["a","b"],"new_tags":["b","c"]}`)
	request := merge.Request{Incoming: incoming}
	result, err := merger.Merge(t.Context(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"append_returns_target":true,"appended":[1,2,3,4],"deduplicated":[{"id":"a","value":1},{"id":"b","value":3}],"source_record_count":3,"tail":[3,4],"target":{"gallery":["new"],"title":"new"},"union":["a","b","c"],"v2_missing":true}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
}

func TestLuaMergeProvidesStableSinkV1Time(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    local first = sink.v1.time.now()
    local second = sink.v1.time.now()
    return {first = first, same = first == second, second = second}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	observedAt := time.Date(2026, time.August, 30, 9, 8, 7, 654321000, time.UTC)
	request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: observedAt}
	result, err := merger.Merge(t.Context(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	want := `{"first":"2026-08-30T09:08:07.654321Z","same":true,"second":"2026-08-30T09:08:07.654321Z"}`
	if string(result.Document.JSON) != want {
		t.Fatalf("Merge() document = %s, want %s", result.Document.JSON, want)
	}
	wantPaths := []string{"/first", "/second"}
	if !slices.Equal(result.Document.DateTimePaths, wantPaths) {
		t.Fatalf("Merge() date-time paths = %v, want %v", result.Document.DateTimePaths, wantPaths)
	}
}

func TestLuaMergeSinkV1TimeMetadataWinsOverMatchingUntypedInput(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    return {copied = incoming.literal, generated = sink.v1.time.now()}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	observedAt := time.Date(2026, time.August, 30, 9, 8, 7, 0, time.UTC)
	request := merge.Request{
		Incoming:   jsonDocument(`{"literal":"2026-08-30T09:08:07Z"}`),
		ObservedAt: observedAt,
	}
	result, err := merger.Merge(t.Context(), request)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	wantPaths := []string{"/copied", "/generated"}
	if !slices.Equal(result.Document.DateTimePaths, wantPaths) {
		t.Fatalf("Merge() date-time paths = %v, want %v", result.Document.DateTimePaths, wantPaths)
	}
}

func TestLuaMergeSinkV1UtilitiesRejectInvalidInputs(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		incoming string
		want     string
	}{
		{name: "argument count", body: `sink.v1.array.keep_tail(incoming.items)`, incoming: `{"items":[]}`, want: "bad argument count"},
		{name: "array object", body: `sink.v1.array.append_all(incoming.object, incoming.items)`, incoming: `{"object":{},"items":[]}`, want: "must be a JSON array"},
		{name: "array hole", body: `incoming.items[2] = nil; sink.v1.array.keep_tail(incoming.items, 2)`, incoming: `{"items":[1,2,3]}`, want: "contiguous integer keys"},
		{name: "fractional limit", body: `sink.v1.array.keep_tail(incoming.items, 1.5)`, incoming: `{"items":[]}`, want: "non-negative integer"},
		{name: "negative limit", body: `sink.v1.array.keep_tail(incoming.items, -1)`, incoming: `{"items":[]}`, want: "non-negative integer"},
		{name: "union item type", body: `sink.v1.array.union_strings(incoming.items, nil)`, incoming: `{"items":["ok",1]}`, want: "array item 2 must be a string"},
		{name: "deduplicate callback error", body: `sink.v1.array.deduplicate(incoming.items, function() error("key failed") end)`, incoming: `{"items":[1]}`, want: "key function failed"},
		{name: "deduplicate nil key", body: `sink.v1.array.deduplicate(incoming.items, function() return nil end)`, incoming: `{"items":[1]}`, want: "must return a string, number, or boolean"},
		{name: "deduplicate callback type", body: `sink.v1.array.deduplicate(incoming.items, "key")`, incoming: `{"items":[1]}`, want: "function expected"},
		{name: "string field type", body: `sink.v1.object.replace_nonempty_string(incoming.target, incoming.source, "value")`, incoming: `{"target":{},"source":{"value":1}}`, want: "must be a string or nil"},
		{name: "array field type", body: `sink.v1.object.replace_nonempty_array(incoming.target, incoming.source, "value")`, incoming: `{"target":{},"source":{"value":{}}}`, want: "must be a JSON array"},
		{name: "time argument", body: `sink.v1.time.now(1)`, incoming: `{}`, want: "bad argument count"},
		{name: "missing observation time", body: `sink.v1.time.now()`, incoming: `{}`, want: "observation time is missing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := []byte(`return function(current, incoming) ` + test.body + `; return incoming end`)
			options := merge.LuaOptions{}
			merger := compileTestProgram(t, source, options)
			request := merge.Request{Incoming: jsonDocument(test.incoming)}
			_, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, merge.ErrExecution) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Merge() error = %v, want execution error containing %q", err, test.want)
			}
		})
	}
}

func TestLuaMergeUnicodeUpperRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		incoming string
	}{
		{name: "missing", source: `utf8.upper()`, incoming: `{}`},
		{name: "number", source: `utf8.upper(incoming.brand)`, incoming: `{"brand":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := []byte(`
return function(current, incoming)
    return {brand = ` + test.source + `}
end`)
			options := merge.LuaOptions{}
			merger := compileTestProgram(t, source, options)
			request := merge.Request{Incoming: jsonDocument(test.incoming)}
			_, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, merge.ErrExecution) {
				t.Fatalf("Merge() error = %v", err)
			}
		})
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
		source := []byte(`return function(current, incoming) while true do end end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`)}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		options := merge.LuaOptions{Timeout: time.Nanosecond, MaxInstructions: 10_000_000}
		source := []byte(`return function(current, incoming) while true do end end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`)}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionDeadline) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("result bytes", func(t *testing.T) {
		options := merge.LuaOptions{MaxResultBytes: 8}
		source := []byte(`return function(current, incoming) return {value = "too large"} end`)
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: jsonDocument(`{}`)}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("native utility work", func(t *testing.T) {
		options := merge.LuaOptions{MaxInstructions: 100}
		source := []byte(`return function(current, incoming) return {items = sink.v1.array.keep_tail(incoming.items, 101)} end`)
		merger := compileTestProgram(t, source, options)
		items := make([]int, 101)
		for index := range items {
			items[index] = index
		}
		incomingJSON, err := json.Marshal(map[string]any{"items": items})
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		request := merge.Request{Incoming: jsonDocument(string(incomingJSON))}
		_, err = merger.Merge(t.Context(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) {
			t.Fatalf("Merge() error = %v", err)
		}
	})
}

func TestLuaMergeClassifiesDocumentAndScriptErrors(t *testing.T) {
	t.Run("incoming", func(t *testing.T) {
		source := []byte(`return function(current, incoming) return incoming end`)
		options := merge.LuaOptions{}
		merger := compileTestProgram(t, source, options)
		request := merge.Request{Incoming: storage.Document{JSON: []byte("bad")}}
		_, err := merger.Merge(context.Background(), request)
		if !errors.Is(err, merge.ErrInvalidIncoming) {
			t.Fatalf("Merge() error = %v", err)
		}
	})

	t.Run("current", func(t *testing.T) {
		source := []byte(`return function(current, incoming) return incoming end`)
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
		source := []byte(`return function(current, incoming) error("bad rule") end`)
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
