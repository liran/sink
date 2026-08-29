package merge

import (
	"fmt"
	"strings"

	"github.com/iceisfun/golua/vm"
)

func addUnicodeTextFunctions(luaVM *vm.VM) {
	value := luaVM.GetGlobal("utf8")
	if !value.IsTable() {
		panic("Lua UTF-8 library is unavailable")
	}
	library, ok := value.AsTable().(*vm.Table)
	if !ok {
		panic("Lua UTF-8 library is not a concrete table")
	}
	library.SetString("upper", vm.NewNativeFunc(unicodeUpper))
}

func unicodeUpper(state *vm.VM) int {
	value := state.Get(1)
	if !value.IsString() {
		got := "no value"
		if state.ArgCount() >= 1 {
			got = value.Type()
		}
		panic(fmt.Sprintf("bad argument #1 to 'utf8.upper' (string expected, got %s)", got))
	}
	state.Set(0, vm.NewString(strings.ToUpper(value.AsString())))
	return 1
}
