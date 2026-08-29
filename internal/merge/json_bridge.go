package merge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/iceisfun/golua/vm"
	"github.com/liran/sink/internal/storage"
)

type luaJSONBridge struct {
	objectMeta *vm.Table
	arrayMeta  *vm.Table
	nullMeta   *vm.Table
	nullTable  *vm.Table
}

func newLuaJSONBridge(luaVM *vm.VM) *luaJSONBridge {
	bridge := &luaJSONBridge{
		objectMeta: protectedMetatable("JSON object"),
		arrayMeta:  protectedMetatable("JSON array"),
		nullMeta:   protectedMetatable("JSON null"),
		nullTable:  vm.NewEmptyTable(),
	}
	bridge.nullTable.SetMetatable(bridge.nullMeta)

	jsonLibrary := vm.NewEmptyTable()
	jsonLibrary.SetString("null", vm.NewTable(bridge.nullTable))
	jsonLibrary.SetString("object", vm.NewNativeFunc(func(state *vm.VM) int {
		table := vm.NewEmptyTable()
		table.SetMetatable(bridge.objectMeta)
		state.Set(0, vm.NewTable(table))
		return 1
	}))
	jsonLibrary.SetString("array", vm.NewNativeFunc(func(state *vm.VM) int {
		table := vm.NewEmptyTable()
		table.SetMetatable(bridge.arrayMeta)
		state.Set(0, vm.NewTable(table))
		return 1
	}))
	jsonLibrary.SetString("is_null", vm.NewNativeFunc(func(state *vm.VM) int {
		isNull := false
		if state.ArgCount() >= 1 && state.Get(1).IsTable() {
			table, ok := state.Get(1).AsTable().(*vm.Table)
			isNull = ok && table == bridge.nullTable
		}
		state.Set(0, vm.NewBool(isNull))
		return 1
	}))
	luaVM.SetGlobal("json", vm.NewTable(jsonLibrary))
	return bridge
}

func protectedMetatable(label string) *vm.Table {
	meta := vm.NewEmptyTable()
	meta.SetString(vm.MetaMetatable, vm.NewString(label))
	return meta
}

func decodeJSONObject(document storage.Document) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(document.JSON))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode JSON: unexpected trailing content")
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("JSON document must be an object")
	}
	return object, nil
}

func (b *luaJSONBridge) goToLua(value any) (vm.Value, error) {
	switch typed := value.(type) {
	case nil:
		return vm.NewTable(b.nullTable), nil
	case bool:
		return vm.NewBool(typed), nil
	case string:
		return vm.NewString(typed), nil
	case json.Number:
		integer, err := typed.Int64()
		if err == nil {
			return vm.NewInt(integer), nil
		}
		number, err := typed.Float64()
		if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
			return vm.Nil, fmt.Errorf("invalid JSON number %q", typed)
		}
		return vm.NewFloat(number), nil
	case []any:
		table := vm.NewTableWithSize(len(typed), 0)
		table.SetMetatable(b.arrayMeta)
		for index, item := range typed {
			converted, err := b.goToLua(item)
			if err != nil {
				return vm.Nil, err
			}
			table.SetInt(index+1, converted)
		}
		return vm.NewTable(table), nil
	case map[string]any:
		table := vm.NewTableWithSize(0, len(typed))
		table.SetMetatable(b.objectMeta)
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			item := typed[key]
			converted, err := b.goToLua(item)
			if err != nil {
				return vm.Nil, err
			}
			table.SetString(key, converted)
		}
		return vm.NewTable(table), nil
	default:
		return vm.Nil, fmt.Errorf("unsupported Go value %T", value)
	}
}

func (b *luaJSONBridge) encodeJSONObject(value vm.Value) ([]byte, error) {
	decoded, err := b.luaToGo(value, make(map[*vm.Table]bool))
	if err != nil {
		return nil, err
	}
	if _, ok := decoded.(map[string]any); !ok {
		return nil, errors.New("merge result must be a JSON object")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("encode JSON: %w", err)
	}
	return encoded, nil
}

func (b *luaJSONBridge) luaToGo(value vm.Value, active map[*vm.Table]bool) (any, error) {
	switch {
	case value.IsNil():
		return nil, nil
	case value.IsBool():
		return value.AsBool(), nil
	case value.IsString():
		return value.AsString(), nil
	case value.IsInt():
		return json.Number(strconv.FormatInt(value.AsInt(), 10)), nil
	case value.IsFloat():
		number := value.AsFloat()
		if math.IsInf(number, 0) || math.IsNaN(number) {
			return nil, errors.New("lua result contains a non-finite number")
		}
		return number, nil
	case value.IsTable():
		table, ok := value.AsTable().(*vm.Table)
		if !ok {
			return nil, errors.New("lua result contains a virtual table")
		}
		if table == b.nullTable {
			return nil, nil
		}
		if active[table] {
			return nil, errors.New("lua result contains a table cycle")
		}
		active[table] = true
		defer delete(active, table)
		return b.luaTableToGo(table, active)
	default:
		return nil, fmt.Errorf("lua result contains unsupported type %s", value.Type())
	}
}

func (b *luaJSONBridge) luaTableToGo(table *vm.Table, active map[*vm.Table]bool) (any, error) {
	switch table.Metatable() {
	case b.objectMeta:
		return b.luaObjectToGo(table, active)
	case b.arrayMeta:
		return b.luaArrayToGo(table, active)
	case b.nullMeta:
		return nil, errors.New("lua result contains an invalid JSON null value")
	}

	count, array, err := inspectLuaTable(table)
	if err != nil {
		return nil, err
	}
	if array && count > 0 {
		return b.luaArrayToGo(table, active)
	}
	return b.luaObjectToGo(table, active)
}

func inspectLuaTable(table *vm.Table) (int, bool, error) {
	count := 0
	array := true
	key := vm.Nil
	for {
		next, _, err := table.Next(key)
		if err != nil {
			return 0, false, err
		}
		if next.IsNil() {
			break
		}
		count++
		if !next.IsInt() || next.AsInt() < 1 {
			array = false
		}
		key = next
	}
	return count, array, nil
}

func (b *luaJSONBridge) luaArrayToGo(table *vm.Table, active map[*vm.Table]bool) ([]any, error) {
	count, array, err := inspectLuaTable(table)
	if err != nil {
		return nil, err
	}
	if !array || table.Len() != count {
		return nil, errors.New("lua JSON array must have contiguous integer keys starting at one")
	}
	result := make([]any, count)
	for index := 1; index <= count; index++ {
		item, err := b.luaToGo(table.GetInt(index), active)
		if err != nil {
			return nil, err
		}
		result[index-1] = item
	}
	return result, nil
}

func (b *luaJSONBridge) luaObjectToGo(table *vm.Table, active map[*vm.Table]bool) (map[string]any, error) {
	result := make(map[string]any)
	key := vm.Nil
	for {
		next, value, err := table.Next(key)
		if err != nil {
			return nil, err
		}
		if next.IsNil() {
			break
		}
		if !next.IsString() {
			return nil, fmt.Errorf("lua JSON object has a non-string key of type %s", next.Type())
		}
		converted, err := b.luaToGo(value, active)
		if err != nil {
			return nil, err
		}
		result[next.AsString()] = converted
		key = next
	}
	return result, nil
}
