package merge

import (
	"fmt"
	"math"
	"time"

	"github.com/iceisfun/golua/vm"
)

const (
	sinkV1ArrayAppendAll            = "sink.v1.array.append_all"
	sinkV1ArrayDeduplicate          = "sink.v1.array.deduplicate"
	sinkV1ArrayKeepTail             = "sink.v1.array.keep_tail"
	sinkV1ArrayUnionStrings         = "sink.v1.array.union_strings"
	sinkV1ObjectReplaceString       = "sink.v1.object.replace_nonempty_string"
	sinkV1ObjectReplaceArray        = "sink.v1.object.replace_nonempty_array"
	sinkV1TimeNow                   = "sink.v1.time.now"
	sinkV1DeduplicateCallbackResult = "key function"
)

type sinkV1Library struct {
	bridge     *luaJSONBridge
	observedAt string
}

func addSinkV1Functions(luaVM *vm.VM, bridge *luaJSONBridge, observedAt time.Time) {
	observedAtText := ""
	if !observedAt.IsZero() {
		observedAtText = observedAt.UTC().Format(time.RFC3339Nano)
	}
	library := sinkV1Library{bridge: bridge, observedAt: observedAtText}
	arrayLibrary := vm.NewEmptyTable()
	arrayLibrary.SetString("append_all", vm.NewNativeFunc(library.arrayAppendAll))
	arrayLibrary.SetString("deduplicate", vm.NewNativeFunc(library.arrayDeduplicate))
	arrayLibrary.SetString("keep_tail", vm.NewNativeFunc(library.arrayKeepTail))
	arrayLibrary.SetString("union_strings", vm.NewNativeFunc(library.arrayUnionStrings))

	objectLibrary := vm.NewEmptyTable()
	objectLibrary.SetString("replace_nonempty_string", vm.NewNativeFunc(library.objectReplaceNonemptyString))
	objectLibrary.SetString("replace_nonempty_array", vm.NewNativeFunc(library.objectReplaceNonemptyArray))
	timeLibrary := vm.NewEmptyTable()
	timeLibrary.SetString("now", vm.NewNativeFunc(library.timeNow))

	v1Library := vm.NewEmptyTable()
	v1Library.SetString("array", vm.NewTable(arrayLibrary))
	v1Library.SetString("object", vm.NewTable(objectLibrary))
	v1Library.SetString("time", vm.NewTable(timeLibrary))

	sinkLibrary := vm.NewEmptyTable()
	sinkLibrary.SetString("v1", vm.NewTable(v1Library))
	luaVM.SetGlobal("sink", vm.NewTable(sinkLibrary))
}

func (l sinkV1Library) timeNow(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1TimeNow, 0)
	if l.observedAt == "" {
		panic(fmt.Sprintf("%s is unavailable because the merge observation time is missing", sinkV1TimeNow))
	}
	l.bridge.dateTimeValues[l.observedAt] = struct{}{}
	l.bridge.forcedDateTimes[l.observedAt] = struct{}{}
	state.Set(0, vm.NewString(l.observedAt))
	return 1
}

func (l sinkV1Library) arrayAppendAll(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ArrayAppendAll, 2)
	target := l.requireJSONArray(state, sinkV1ArrayAppendAll, 1, false)
	source := l.requireJSONArray(state, sinkV1ArrayAppendAll, 2, true)
	if source == nil {
		state.Set(0, vm.NewTable(target))
		return 1
	}
	targetLength := target.Len()
	sourceLength := source.Len()
	requireSinkV1WorkWithinLimit(state, sinkV1ArrayAppendAll, sourceLength)
	if sourceLength > math.MaxInt-targetLength {
		panic(fmt.Sprintf("%s result is too large", sinkV1ArrayAppendAll))
	}
	for index := 1; index <= sourceLength; index++ {
		checkSinkV1Context(state, index)
		target.SetInt(targetLength+index, source.GetInt(index))
	}
	state.Set(0, vm.NewTable(target))
	return 1
}

func (l sinkV1Library) arrayDeduplicate(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ArrayDeduplicate, 2)
	items := l.requireJSONArray(state, sinkV1ArrayDeduplicate, 1, false)
	keyFunction := state.Get(2)
	if !keyFunction.IsCallable() {
		panicLuaArgumentType(state, sinkV1ArrayDeduplicate, 2, "function")
	}

	result := l.bridge.newArray(items.Len())
	seen := vm.NewEmptyTable()
	requireSinkV1WorkWithinLimit(state, sinkV1ArrayDeduplicate, items.Len())
	for index := 1; index <= items.Len(); index++ {
		checkSinkV1Context(state, index)
		item := items.GetInt(index)
		arguments := []vm.Value{item}
		keys, err := state.ProtectedCall(keyFunction, arguments)
		if err != nil {
			panic(fmt.Sprintf("%s %s failed: %v", sinkV1ArrayDeduplicate, sinkV1DeduplicateCallbackResult, err))
		}
		if len(keys) != 1 {
			panic(fmt.Sprintf("%s %s returned %d values; expected one", sinkV1ArrayDeduplicate, sinkV1DeduplicateCallbackResult, len(keys)))
		}
		key := keys[0]
		if key.IsNil() || (!key.IsBool() && !key.IsNumber() && !key.IsString()) {
			panic(fmt.Sprintf("%s %s must return a string, number, or boolean; got %s", sinkV1ArrayDeduplicate, sinkV1DeduplicateCallbackResult, key.Type()))
		}
		if !seen.Get(key).IsNil() {
			continue
		}
		if err := seen.Set(key, vm.True); err != nil {
			panic(fmt.Sprintf("%s cannot store key: %v", sinkV1ArrayDeduplicate, err))
		}
		result.SetInt(result.Len()+1, item)
	}
	state.Set(0, vm.NewTable(result))
	return 1
}

func (l sinkV1Library) arrayKeepTail(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ArrayKeepTail, 2)
	items := l.requireJSONArray(state, sinkV1ArrayKeepTail, 1, false)
	limitValue := state.Get(2)
	if !limitValue.IsInt() {
		panicLuaArgumentType(state, sinkV1ArrayKeepTail, 2, "non-negative integer")
	}
	limit := limitValue.AsInt()
	if limit < 0 {
		panicLuaArgumentType(state, sinkV1ArrayKeepTail, 2, "non-negative integer")
	}

	length := items.Len()
	start := 1
	if limit < int64(length) {
		start = length - int(limit) + 1
	}
	resultLength := length - start + 1
	if limit == 0 {
		resultLength = 0
	}
	requireSinkV1WorkWithinLimit(state, sinkV1ArrayKeepTail, resultLength)
	result := l.bridge.newArray(resultLength)
	for index := start; index <= length && limit != 0; index++ {
		checkSinkV1Context(state, index-start+1)
		result.SetInt(result.Len()+1, items.GetInt(index))
	}
	state.Set(0, vm.NewTable(result))
	return 1
}

func (l sinkV1Library) arrayUnionStrings(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ArrayUnionStrings, 2)
	left := l.requireJSONArray(state, sinkV1ArrayUnionStrings, 1, true)
	right := l.requireJSONArray(state, sinkV1ArrayUnionStrings, 2, true)
	capacity := 0
	if left != nil {
		capacity += left.Len()
	}
	if right != nil {
		if right.Len() > math.MaxInt-capacity {
			panic(fmt.Sprintf("%s result is too large", sinkV1ArrayUnionStrings))
		}
		capacity += right.Len()
	}
	requireSinkV1WorkWithinLimit(state, sinkV1ArrayUnionStrings, capacity)

	result := l.bridge.newArray(capacity)
	seen := make(map[string]struct{}, capacity)
	leftOptions := appendUniqueStringsOptions{result: result, seen: seen, source: left, argumentIndex: 1}
	l.appendUniqueStrings(state, leftOptions)
	rightOptions := appendUniqueStringsOptions{result: result, seen: seen, source: right, argumentIndex: 2}
	l.appendUniqueStrings(state, rightOptions)
	state.Set(0, vm.NewTable(result))
	return 1
}

type appendUniqueStringsOptions struct {
	result        *vm.Table
	seen          map[string]struct{}
	source        *vm.Table
	argumentIndex int
}

func (l sinkV1Library) appendUniqueStrings(state *vm.VM, options appendUniqueStringsOptions) {
	if options.source == nil {
		return
	}
	for index := 1; index <= options.source.Len(); index++ {
		checkSinkV1Context(state, index)
		item := options.source.GetInt(index)
		if !item.IsString() {
			panic(fmt.Sprintf("bad argument #%d to '%s' (array item %d must be a string, got %s)", options.argumentIndex, sinkV1ArrayUnionStrings, index, item.Type()))
		}
		value := item.AsString()
		if _, exists := options.seen[value]; exists {
			continue
		}
		options.seen[value] = struct{}{}
		options.result.SetInt(options.result.Len()+1, item)
	}
}

func (l sinkV1Library) objectReplaceNonemptyString(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ObjectReplaceString, 3)
	target := l.requireJSONObject(state, sinkV1ObjectReplaceString, 1)
	source := l.requireJSONObject(state, sinkV1ObjectReplaceString, 2)
	field := requireLuaString(state, sinkV1ObjectReplaceString, 3)
	value := source.GetString(field)
	if value.IsNil() || value.IsString() && value.AsString() == "" {
		return 0
	}
	if !value.IsString() {
		panic(fmt.Sprintf("%s source field %q must be a string or nil; got %s", sinkV1ObjectReplaceString, field, value.Type()))
	}
	target.SetString(field, value)
	return 0
}

func (l sinkV1Library) objectReplaceNonemptyArray(state *vm.VM) int {
	requireLuaArgumentCount(state, sinkV1ObjectReplaceArray, 3)
	target := l.requireJSONObject(state, sinkV1ObjectReplaceArray, 1)
	source := l.requireJSONObject(state, sinkV1ObjectReplaceArray, 2)
	field := requireLuaString(state, sinkV1ObjectReplaceArray, 3)
	value := source.GetString(field)
	if value.IsNil() {
		return 0
	}
	arrayOptions := requireJSONArrayValueOptions{
		value:        value,
		functionName: sinkV1ObjectReplaceArray,
		label:        fmt.Sprintf("source field %q", field),
	}
	array := l.requireJSONArrayValue(state, arrayOptions)
	if array.Len() == 0 {
		return 0
	}
	target.SetString(field, value)
	return 0
}

func (l sinkV1Library) requireJSONArray(state *vm.VM, functionName string, index int, allowNil bool) *vm.Table {
	value := state.Get(index)
	label := fmt.Sprintf("argument #%d", index)
	options := requireJSONArrayValueOptions{
		value:        value,
		functionName: functionName,
		label:        label,
		allowNil:     allowNil,
	}
	return l.requireJSONArrayValue(state, options)
}

type requireJSONArrayValueOptions struct {
	value        vm.Value
	functionName string
	label        string
	allowNil     bool
}

func (l sinkV1Library) requireJSONArrayValue(state *vm.VM, options requireJSONArrayValueOptions) *vm.Table {
	if options.value.IsNil() && options.allowNil {
		return nil
	}
	if !options.value.IsTable() {
		panic(fmt.Sprintf("%s %s must be a JSON array%s; got %s", options.functionName, options.label, nilAllowance(options.allowNil), options.value.Type()))
	}
	table, ok := options.value.AsTable().(*vm.Table)
	if !ok || table.Metatable() != l.bridge.arrayMeta {
		panic(fmt.Sprintf("%s %s must be a JSON array%s", options.functionName, options.label, nilAllowance(options.allowNil)))
	}
	count, array := inspectSinkV1JSONArray(state, options.functionName, options.label, table)
	if !array || table.Len() != count {
		panic(fmt.Sprintf("%s %s must have contiguous integer keys starting at one", options.functionName, options.label))
	}
	return table
}

func inspectSinkV1JSONArray(state *vm.VM, functionName string, label string, table *vm.Table) (int, bool) {
	count := 0
	array := true
	key := vm.Nil
	for {
		next, _, err := table.Next(key)
		if err != nil {
			panic(fmt.Sprintf("%s inspect %s: %v", functionName, label, err))
		}
		if next.IsNil() {
			return count, array
		}
		count++
		requireSinkV1WorkWithinLimit(state, functionName, count)
		checkSinkV1Context(state, count)
		if !next.IsInt() || next.AsInt() < 1 {
			array = false
		}
		key = next
	}
}

func (l sinkV1Library) requireJSONObject(state *vm.VM, functionName string, index int) *vm.Table {
	value := state.Get(index)
	if !value.IsTable() {
		panicLuaArgumentType(state, functionName, index, "JSON object")
	}
	table, ok := value.AsTable().(*vm.Table)
	if !ok || table.Metatable() != l.bridge.objectMeta {
		panicLuaArgumentType(state, functionName, index, "JSON object")
	}
	return table
}

func requireLuaArgumentCount(state *vm.VM, functionName string, expected int) {
	actual := state.ArgCount()
	if actual != expected {
		panic(fmt.Sprintf("bad argument count to '%s' (expected %d, got %d)", functionName, expected, actual))
	}
}

func requireLuaString(state *vm.VM, functionName string, index int) string {
	value := state.Get(index)
	if !value.IsString() {
		panicLuaArgumentType(state, functionName, index, "string")
	}
	return value.AsString()
}

func panicLuaArgumentType(state *vm.VM, functionName string, index int, expected string) {
	got := "no value"
	if state.ArgCount() >= index {
		got = state.Get(index).Type()
	}
	panic(fmt.Sprintf("bad argument #%d to '%s' (%s expected, got %s)", index, functionName, expected, got))
}

func nilAllowance(allowNil bool) string {
	if allowNil {
		return " or nil"
	}
	return ""
}

func requireSinkV1WorkWithinLimit(state *vm.VM, functionName string, items int) {
	limit := state.GetLimits().MaxInstructions
	if limit > 0 && int64(items) > limit {
		panic(fmt.Sprintf("%s instruction limit exceeded: %d items exceeds limit %d", functionName, items, limit))
	}
}

func checkSinkV1Context(state *vm.VM, position int) {
	if position%256 != 0 {
		return
	}
	ctx := state.Context()
	if ctx == nil || ctx.Err() == nil {
		return
	}
	panic(ctx.Err())
}
