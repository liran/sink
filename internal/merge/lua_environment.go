package merge

import (
	"context"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

// Capture only the restricted standard library, before installing any
// request-bound JSON or sink functions. Executions share immutable native
// functions; every global/library/metatable remains a fresh mutable table.
type luaEnvironment struct {
	globals    *luaEnvironmentTable
	stringMeta *luaEnvironmentTable
}

type luaEnvironmentTable struct {
	entries   []luaEnvironmentEntry
	metatable *luaEnvironmentTable
}

type luaEnvironmentEntry struct {
	key   vm.Value
	value vm.Value
	table *luaEnvironmentTable
}

func newLuaEnvironment() *luaEnvironment {
	luaVM := vm.New()
	defer luaVM.Close(context.Background())
	stdlib.Open(luaVM)
	addUnicodeTextFunctions(luaVM)
	restrictLuaEnvironment(luaVM)
	captured := make(map[vm.LuaTable]*luaEnvironmentTable)
	environment := &luaEnvironment{
		globals:    captureLuaEnvironment(luaVM.Globals(), captured),
		stringMeta: captureLuaEnvironment(luaVM.StringMeta(), captured),
	}
	return environment
}

func captureLuaEnvironment(source vm.LuaTable, captured map[vm.LuaTable]*luaEnvironmentTable) *luaEnvironmentTable {
	if source == nil {
		return nil
	}
	if existing := captured[source]; existing != nil {
		return existing
	}
	table := &luaEnvironmentTable{}
	captured[source] = table
	source.(*vm.Table).ForEach(func(key, value vm.Value) bool {
		entry := luaEnvironmentEntry{key: key, value: value}
		if value.IsTable() {
			entry.table = captureLuaEnvironment(value.AsTable(), captured)
			entry.value = vm.Nil
		}
		table.entries = append(table.entries, entry)
		return true
	})
	table.metatable = captureLuaEnvironment(source.Metatable(), captured)
	return table
}

func (e *luaEnvironment) install(luaVM *vm.VM) {
	cloned := make(map[*luaEnvironmentTable]vm.LuaTable)
	cloned[e.globals] = luaVM.Globals()
	for _, entry := range e.globals.entries {
		value := entry.value
		if entry.table != nil {
			value = vm.NewTable(entry.table.clone(cloned))
		}
		_ = luaVM.Globals().Set(entry.key, value)
	}
	luaVM.SetStringMeta(e.stringMeta.clone(cloned))
}

func (t *luaEnvironmentTable) clone(cloned map[*luaEnvironmentTable]vm.LuaTable) vm.LuaTable {
	if t == nil {
		return nil
	}
	if existing := cloned[t]; existing != nil {
		return existing
	}
	table := vm.NewTableWithSize(0, len(t.entries))
	cloned[t] = table
	for _, entry := range t.entries {
		value := entry.value
		if entry.table != nil {
			value = vm.NewTable(entry.table.clone(cloned))
		}
		_ = table.Set(entry.key, value)
	}
	table.SetMetatable(t.metatable.clone(cloned))
	return table
}
