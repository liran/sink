package luabench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

type iceLuaEngine struct {
	vm         *vm.VM
	merge      vm.Value
	withLimits bool
}

func newIceLuaEngine(withLimits bool) (*iceLuaEngine, error) {
	block, err := parser.Parse("product_merge.lua", string(productMergeLua))
	if err != nil {
		return nil, err
	}
	proto, err := compiler.Compile("product_merge.lua", block)
	if err != nil {
		return nil, err
	}

	options := make([]vm.VMOption, 0, 1)
	if withLimits {
		limits := vm.Limits{
			MaxCallDepth:    256,
			MaxStackSlots:   65_536,
			MaxInstructions: 100_000_000,
		}
		options = append(options, vm.WithLimits(limits))
	}
	luaVM := vm.New(options...)
	stdlib.Open(luaVM)
	results, err := luaVM.Run(proto)
	if err != nil {
		luaVM.Close(context.Background())
		return nil, err
	}
	if len(results) != 1 || !results[0].IsFunction() {
		luaVM.Close(context.Background())
		return nil, errors.New("product merge script did not return a function")
	}
	engine := &iceLuaEngine{
		vm:         luaVM,
		merge:      results[0],
		withLimits: withLimits,
	}
	return engine, nil
}

func (e *iceLuaEngine) Close() {
	e.vm.Close(context.Background())
}

func (e *iceLuaEngine) Merge(currentJSON, incomingJSON []byte) ([]byte, error) {
	current, err := decodeJSON(currentJSON)
	if err != nil {
		return nil, err
	}
	incoming, err := decodeJSON(incomingJSON)
	if err != nil {
		return nil, err
	}
	currentValue, err := goToIceLua(current)
	if err != nil {
		return nil, err
	}
	incomingValue, err := goToIceLua(incoming)
	if err != nil {
		return nil, err
	}
	arguments := []vm.Value{currentValue, incomingValue}

	if e.withLimits {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e.vm.SetContext(ctx)
		defer e.vm.SetContext(context.Background())
	}
	results, err := e.vm.ProtectedCall(e.merge, arguments)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("merge returned %d values, expected one", len(results))
	}
	result, err := iceLuaToGo(results[0], make(map[*vm.Table]bool))
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func goToIceLua(value any) (vm.Value, error) {
	switch typed := value.(type) {
	case nil:
		return vm.Nil, nil
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
		if err != nil {
			return vm.Nil, err
		}
		return vm.NewFloat(number), nil
	case []any:
		table := vm.NewTableWithSize(len(typed), 0)
		for index, item := range typed {
			converted, err := goToIceLua(item)
			if err != nil {
				return vm.Nil, err
			}
			table.SetInt(index+1, converted)
		}
		return vm.NewTable(table), nil
	case map[string]any:
		table := vm.NewTableWithSize(0, len(typed))
		for key, item := range typed {
			converted, err := goToIceLua(item)
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

func iceLuaToGo(value vm.Value, active map[*vm.Table]bool) (any, error) {
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
		return value.AsFloat(), nil
	case value.IsTable():
		table, ok := value.AsTable().(*vm.Table)
		if !ok {
			return nil, errors.New("Lua result contains a virtual table")
		}
		if active[table] {
			return nil, errors.New("Lua result contains a table cycle")
		}
		active[table] = true
		defer delete(active, table)
		return iceLuaTableToGo(table, active)
	default:
		return nil, fmt.Errorf("unsupported Lua result %s", value.Type())
	}
}

func iceLuaTableToGo(table *vm.Table, active map[*vm.Table]bool) (any, error) {
	length := table.Len()
	count := 0
	array := true
	key := vm.Nil
	for {
		next, _, err := table.Next(key)
		if err != nil {
			return nil, err
		}
		if next.IsNil() {
			break
		}
		count++
		if !next.IsInt() || next.AsInt() < 1 || next.AsInt() > int64(length) {
			array = false
		}
		key = next
	}
	if array && count == length {
		result := make([]any, length)
		for index := 1; index <= length; index++ {
			item, err := iceLuaToGo(table.GetInt(index), active)
			if err != nil {
				return nil, err
			}
			result[index-1] = item
		}
		return result, nil
	}

	result := make(map[string]any, count)
	key = vm.Nil
	for {
		next, value, err := table.Next(key)
		if err != nil {
			return nil, err
		}
		if next.IsNil() {
			break
		}
		if !next.IsString() {
			return nil, fmt.Errorf("Lua object has a non-string key of type %s", next.Type())
		}
		converted, err := iceLuaToGo(value, active)
		if err != nil {
			return nil, err
		}
		result[next.AsString()] = converted
		key = next
	}
	return result, nil
}
