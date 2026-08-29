package merge

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

const (
	defaultLuaTimeout        = 100 * time.Millisecond
	defaultMaxSourceBytes    = 64 << 10
	defaultMaxResultBytes    = 16 << 20
	defaultMaxCachedPrograms = 256
	defaultMaxInstructions   = 1_000_000
	defaultMaxCallDepth      = 256
	defaultMaxStackSlots     = 65_536
	luaProgramFilename       = "merge.lua"
)

type LuaOptions struct {
	Timeout           time.Duration
	MaxSourceBytes    int
	MaxResultBytes    int
	MaxCachedPrograms int
	MaxInstructions   int64
	MaxCallDepth      int
	MaxStackSlots     int
}

type LuaEngine struct {
	options LuaOptions
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	recent  list.List
}

type cachedProgram struct {
	digest [sha256.Size]byte
	proto  *compiler.Proto
}

type luaMerger struct {
	engine  *LuaEngine
	program *compiler.Proto
}

func NewLuaEngine(options LuaOptions) (*LuaEngine, error) {
	if options.Timeout < 0 || options.MaxSourceBytes < 0 || options.MaxResultBytes < 0 ||
		options.MaxCachedPrograms < 0 || options.MaxInstructions < 0 || options.MaxCallDepth < 0 ||
		options.MaxStackSlots < 0 {
		return nil, errors.New("create Lua merge engine: limits cannot be negative")
	}
	if options.Timeout == 0 {
		options.Timeout = defaultLuaTimeout
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultMaxSourceBytes
	}
	if options.MaxResultBytes == 0 {
		options.MaxResultBytes = defaultMaxResultBytes
	}
	if options.MaxCachedPrograms == 0 {
		options.MaxCachedPrograms = defaultMaxCachedPrograms
	}
	if options.MaxInstructions == 0 {
		options.MaxInstructions = defaultMaxInstructions
	}
	if options.MaxCallDepth == 0 {
		options.MaxCallDepth = defaultMaxCallDepth
	}
	if options.MaxStackSlots == 0 {
		options.MaxStackSlots = defaultMaxStackSlots
	}
	engine := &LuaEngine{
		options: options,
		entries: make(map[[sha256.Size]byte]*list.Element),
	}
	return engine, nil
}

func (e *LuaEngine) Compile(program Program) (Merger, error) {
	if len(program.Source) == 0 {
		return nil, fmt.Errorf("%w: source is required", ErrInvalidProgram)
	}
	if len(program.Source) > e.options.MaxSourceBytes {
		return nil, fmt.Errorf("%w: source is %d bytes; maximum is %d", ErrInvalidProgram, len(program.Source), e.options.MaxSourceBytes)
	}
	digest := sha256.Sum256(program.Source)
	if len(program.SHA256) != 0 {
		if len(program.SHA256) != sha256.Size {
			return nil, fmt.Errorf("%w: SHA-256 digest must be %d bytes", ErrInvalidProgram, sha256.Size)
		}
		provided := [sha256.Size]byte(program.SHA256)
		if provided != digest {
			return nil, fmt.Errorf("%w: SHA-256 digest does not match source", ErrInvalidProgram)
		}
	}

	if compiled := e.cached(digest); compiled != nil {
		merger := &luaMerger{engine: e, program: compiled}
		return merger, nil
	}
	block, err := parser.Parse(luaProgramFilename, string(program.Source))
	if err != nil {
		return nil, fmt.Errorf("%w: parse source: %v", ErrInvalidProgram, err)
	}
	compiled, err := compiler.Compile(luaProgramFilename, block)
	if err != nil {
		return nil, fmt.Errorf("%w: compile source: %v", ErrInvalidProgram, err)
	}
	if err := e.validate(compiled); err != nil {
		return nil, err
	}
	compiled = e.store(digest, compiled)
	merger := &luaMerger{engine: e, program: compiled}
	return merger, nil
}

func (e *LuaEngine) validate(compiled *compiler.Proto) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
	defer cancel()
	luaVM, _ := e.newVM(ctx)
	defer luaVM.Close(context.Background())
	results, err := luaVM.Run(compiled)
	if err != nil {
		return fmt.Errorf("%w: initialize chunk: %v", ErrInvalidProgram, classifyExecutionError(ctx, err))
	}
	if len(results) != 1 || !results[0].IsFunction() {
		return fmt.Errorf("%w: chunk must return exactly one function", ErrInvalidProgram)
	}
	return nil
}

func (e *LuaEngine) cached(digest [sha256.Size]byte) *compiler.Proto {
	e.mu.Lock()
	defer e.mu.Unlock()
	element := e.entries[digest]
	if element == nil {
		return nil
	}
	e.recent.MoveToFront(element)
	entry := element.Value.(*cachedProgram)
	return entry.proto
}

func (e *LuaEngine) store(digest [sha256.Size]byte, compiled *compiler.Proto) *compiler.Proto {
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing := e.entries[digest]; existing != nil {
		e.recent.MoveToFront(existing)
		entry := existing.Value.(*cachedProgram)
		return entry.proto
	}
	entry := &cachedProgram{digest: digest, proto: compiled}
	element := e.recent.PushFront(entry)
	e.entries[digest] = element
	if e.recent.Len() > e.options.MaxCachedPrograms {
		oldest := e.recent.Back()
		oldEntry := oldest.Value.(*cachedProgram)
		delete(e.entries, oldEntry.digest)
		e.recent.Remove(oldest)
	}
	return compiled
}

func (m *luaMerger) Merge(ctx context.Context, req Request) (Result, error) {
	var empty Result
	incoming, err := decodeJSONObject(req.Incoming)
	if err != nil {
		return empty, fmt.Errorf("%w: %v", ErrInvalidIncoming, err)
	}

	var current decodedJSONObject
	if req.Current != nil {
		current, err = decodeJSONObject(*req.Current)
		if err != nil {
			return empty, fmt.Errorf("%w: %v", ErrInvalidCurrent, err)
		}
	}

	executionContext, cancel := context.WithTimeout(ctx, m.engine.options.Timeout)
	defer cancel()
	luaVM, bridge := m.engine.newVM(executionContext)
	defer luaVM.Close(context.Background())
	bridge.addDateTimeDocument(incoming)
	bridge.addDateTimeDocument(current)

	results, err := luaVM.Run(m.program)
	if err != nil {
		return empty, classifyExecutionError(executionContext, err)
	}
	if len(results) != 1 || !results[0].IsFunction() {
		return empty, fmt.Errorf("%w: chunk must return exactly one function", ErrInvalidProgram)
	}

	currentValue := vm.Nil
	if current.value != nil {
		currentValue, err = bridge.goToLua(current.value)
		if err != nil {
			return empty, fmt.Errorf("%w: convert current document: %v", ErrInvalidCurrent, err)
		}
	}
	incomingValue, err := bridge.goToLua(incoming.value)
	if err != nil {
		return empty, fmt.Errorf("%w: convert incoming document: %v", ErrInvalidIncoming, err)
	}
	observedAt := req.ObservedAt.UTC().Format(time.RFC3339Nano)
	bridge.dateTimeValues[observedAt] = struct{}{}
	contextTable := vm.NewEmptyTable()
	contextTable.SetString("observed_at", vm.NewString(observedAt))
	arguments := []vm.Value{currentValue, incomingValue, vm.NewTable(contextTable)}
	merged, err := luaVM.ProtectedCall(results[0], arguments)
	if err != nil {
		return empty, classifyExecutionError(executionContext, err)
	}
	if len(merged) != 1 {
		return empty, fmt.Errorf("%w: function returned %d values; expected one", ErrInvalidResult, len(merged))
	}
	document, err := bridge.encodeJSONObject(merged[0])
	if err != nil {
		return empty, fmt.Errorf("%w: %v", ErrInvalidResult, err)
	}
	if len(document.JSON) > m.engine.options.MaxResultBytes {
		return empty, fmt.Errorf("%w: result is %d bytes; maximum is %d", ErrExecutionExhausted, len(document.JSON), m.engine.options.MaxResultBytes)
	}
	result := Result{Document: document}
	return result, nil
}

func (e *LuaEngine) newVM(ctx context.Context) (*vm.VM, *luaJSONBridge) {
	limits := vm.Limits{
		MaxCallDepth:    e.options.MaxCallDepth,
		MaxStackSlots:   e.options.MaxStackSlots,
		MaxInstructions: e.options.MaxInstructions,
		MinGCInterval:   -1,
	}
	options := []vm.VMOption{vm.WithContext(ctx), vm.WithLimits(limits)}
	luaVM := vm.New(options...)
	stdlib.Open(luaVM)
	addUnicodeTextFunctions(luaVM)
	bridge := newLuaJSONBridge(luaVM)
	restrictLuaEnvironment(luaVM)
	return luaVM, bridge
}

func restrictLuaEnvironment(luaVM *vm.VM) {
	blocked := []string{
		"_G", "_lastoutput", "_outputlines", "bit32", "collectgarbage", "coroutine",
		"debug", "dofile", "exec", "getmetatable", "glob", "io", "load", "loadfile",
		"os", "package", "print", "rawset", "require", "setmetatable", "time", "warn",
	}
	for _, name := range blocked {
		luaVM.SetGlobal(name, vm.Nil)
	}
	removeTableFunctions(luaVM, "math", "random", "randomseed")
	removeTableFunctions(luaVM, "string", "dump", "rep")
}

func removeTableFunctions(luaVM *vm.VM, global string, names ...string) {
	value := luaVM.GetGlobal(global)
	if !value.IsTable() {
		return
	}
	table, ok := value.AsTable().(*vm.Table)
	if !ok {
		return
	}
	for _, name := range names {
		table.SetString(name, vm.Nil)
	}
}

func classifyExecutionError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %v", ErrExecutionDeadline, ctx.Err())
	}
	if strings.Contains(err.Error(), "instruction limit exceeded") ||
		strings.Contains(err.Error(), "not enough memory") || strings.Contains(err.Error(), "stack overflow") {
		return fmt.Errorf("%w: %v", ErrExecutionExhausted, err)
	}
	return fmt.Errorf("%w: %v", ErrExecution, err)
}
