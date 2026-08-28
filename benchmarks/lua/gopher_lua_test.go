package luabench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	lua "github.com/yuin/gopher-lua"
)

type gopherLuaEngine struct {
	state       *lua.LState
	merge       *lua.LFunction
	withTimeout bool
}

func newGopherLuaEngine(withTimeout bool) (*gopherLuaEngine, error) {
	options := lua.Options{
		SkipOpenLibs:  true,
		CallStackSize: 256,
		RegistrySize:  4096,
	}
	state := lua.NewState(options)
	lua.OpenBase(state)
	lua.OpenString(state)
	lua.OpenTable(state)

	chunk, err := state.Load(bytes.NewReader(productMergeLua), "product_merge.lua")
	if err != nil {
		state.Close()
		return nil, err
	}
	state.Push(chunk)
	if err := state.PCall(0, 1, nil); err != nil {
		state.Close()
		return nil, err
	}
	value := state.Get(-1)
	merge, ok := value.(*lua.LFunction)
	if !ok {
		state.Close()
		return nil, errors.New("product merge script did not return a function")
	}
	state.Pop(1)
	engine := &gopherLuaEngine{
		state:       state,
		merge:       merge,
		withTimeout: withTimeout,
	}
	return engine, nil
}

func (e *gopherLuaEngine) Close() {
	e.state.Close()
}

func (e *gopherLuaEngine) Merge(currentJSON, incomingJSON []byte) ([]byte, error) {
	current, err := decodeJSON(currentJSON)
	if err != nil {
		return nil, err
	}
	incoming, err := decodeJSON(incomingJSON)
	if err != nil {
		return nil, err
	}

	currentValue, err := goToGopherLua(e.state, current)
	if err != nil {
		return nil, err
	}
	incomingValue, err := goToGopherLua(e.state, incoming)
	if err != nil {
		return nil, err
	}
	contextTable := e.state.NewTable()
	e.state.SetField(contextTable, "observed_at", lua.LString(benchmarkObservedAt))

	if e.withTimeout {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e.state.SetContext(ctx)
		defer e.state.RemoveContext()
	}
	call := lua.P{
		Fn:      e.merge,
		NRet:    1,
		Protect: true,
	}
	err = e.state.CallByParam(call, currentValue, incomingValue, contextTable)
	if err != nil {
		return nil, err
	}
	resultValue := e.state.Get(-1)
	result, err := gopherLuaToGo(resultValue, make(map[*lua.LTable]bool))
	e.state.Pop(1)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func decodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func goToGopherLua(state *lua.LState, value any) (lua.LValue, error) {
	switch typed := value.(type) {
	case nil:
		return lua.LNil, nil
	case bool:
		return lua.LBool(typed), nil
	case string:
		return lua.LString(typed), nil
	case json.Number:
		number, err := typed.Float64()
		if err != nil {
			return nil, err
		}
		return lua.LNumber(number), nil
	case []any:
		table := state.CreateTable(len(typed), 0)
		for index, item := range typed {
			converted, err := goToGopherLua(state, item)
			if err != nil {
				return nil, err
			}
			table.RawSetInt(index+1, converted)
		}
		return table, nil
	case map[string]any:
		table := state.CreateTable(0, len(typed))
		for key, item := range typed {
			converted, err := goToGopherLua(state, item)
			if err != nil {
				return nil, err
			}
			table.RawSetString(key, converted)
		}
		return table, nil
	default:
		return nil, fmt.Errorf("unsupported Go value %T", value)
	}
}

func gopherLuaToGo(value lua.LValue, active map[*lua.LTable]bool) (any, error) {
	switch typed := value.(type) {
	case *lua.LNilType:
		return nil, nil
	case lua.LBool:
		return bool(typed), nil
	case lua.LString:
		return string(typed), nil
	case lua.LNumber:
		number := float64(typed)
		if !math.IsInf(number, 0) && !math.IsNaN(number) && math.Trunc(number) == number {
			integer := int64(number)
			if float64(integer) == number {
				return json.Number(strconv.FormatInt(integer, 10)), nil
			}
		}
		return number, nil
	case *lua.LTable:
		if active[typed] {
			return nil, errors.New("Lua result contains a table cycle")
		}
		active[typed] = true
		defer delete(active, typed)
		return gopherLuaTableToGo(typed, active)
	default:
		return nil, fmt.Errorf("unsupported Lua result %s", value.Type().String())
	}
}

func gopherLuaTableToGo(table *lua.LTable, active map[*lua.LTable]bool) (any, error) {
	length := table.Len()
	count := 0
	array := true
	table.ForEach(func(key, _ lua.LValue) {
		count++
		number, ok := key.(lua.LNumber)
		if !ok || number < 1 || math.Trunc(float64(number)) != float64(number) || int(number) > length {
			array = false
		}
	})
	if array && count == length {
		result := make([]any, length)
		for index := 1; index <= length; index++ {
			item, err := gopherLuaToGo(table.RawGetInt(index), active)
			if err != nil {
				return nil, err
			}
			result[index-1] = item
		}
		return result, nil
	}

	result := make(map[string]any, count)
	var conversionErr error
	table.ForEach(func(key, value lua.LValue) {
		if conversionErr != nil {
			return
		}
		stringKey, ok := key.(lua.LString)
		if !ok {
			conversionErr = fmt.Errorf("Lua object has a non-string key of type %s", key.Type().String())
			return
		}
		converted, err := gopherLuaToGo(value, active)
		if err != nil {
			conversionErr = err
			return
		}
		result[string(stringKey)] = converted
	})
	if conversionErr != nil {
		return nil, conversionErr
	}
	return result, nil
}
