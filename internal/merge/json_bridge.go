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
	"strings"
	"time"
	"unsafe"

	"github.com/iceisfun/golua/vm"
	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type luaJSONBridge struct {
	objectMeta    *vm.Table
	arrayMeta     *vm.Table
	nullMeta      *vm.Table
	nullTable     *vm.Table
	dateTimes     map[luaStringIdentity]struct{}
	integerKinds  map[int64]uint8
	integerFields map[*vm.Table]map[vm.Value]bsonInteger
	outputBSON    bool
}

type bsonInteger struct {
	value int64
	wide  bool
}

type bsonDateTime string

// The string backing pointer preserves a directly propagated datetime's origin
// without confusing it with an equal ordinary string from another field.
type luaStringIdentity struct {
	value string
	data  *byte
}

func identityOfLuaString(value string) luaStringIdentity {
	identity := luaStringIdentity{value: value, data: unsafe.StringData(value)}
	return identity
}

type decodedJSONObject struct {
	encoding storage.DocumentEncoding
	value    map[string]any
}

func newLuaJSONBridge(luaVM *vm.VM) *luaJSONBridge {
	bridge := &luaJSONBridge{
		objectMeta:    protectedMetatable("JSON object"),
		arrayMeta:     protectedMetatable("JSON array"),
		nullMeta:      protectedMetatable("JSON null"),
		nullTable:     vm.NewEmptyTable(),
		dateTimes:     make(map[luaStringIdentity]struct{}),
		integerKinds:  make(map[int64]uint8),
		integerFields: make(map[*vm.Table]map[vm.Value]bsonInteger),
	}
	bridge.nullTable.SetMetatable(bridge.nullMeta)

	jsonLibrary := vm.NewEmptyTable()
	jsonLibrary.SetString("null", vm.NewTable(bridge.nullTable))
	jsonLibrary.SetString("object", vm.NewNativeFunc(func(state *vm.VM) int {
		table := bridge.newObject(0)
		state.Set(0, vm.NewTable(table))
		return 1
	}))
	jsonLibrary.SetString("array", vm.NewNativeFunc(func(state *vm.VM) int {
		table := bridge.newArray(0)
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

func (b *luaJSONBridge) newObject(capacity int) *vm.Table {
	table := vm.NewTableWithSize(0, capacity)
	table.SetMetatable(b.objectMeta)
	return table
}

func (b *luaJSONBridge) newArray(capacity int) *vm.Table {
	table := vm.NewTableWithSize(capacity, 0)
	table.SetMetatable(b.arrayMeta)
	return table
}

func protectedMetatable(label string) *vm.Table {
	meta := vm.NewEmptyTable()
	meta.SetString(vm.MetaMetatable, vm.NewString(label))
	return meta
}

func decodeJSONObject(document storage.Document) (decodedJSONObject, error) {
	var result decodedJSONObject
	if err := storage.ValidateDocument(document); err != nil {
		return result, err
	}
	encoded := document.Payload
	if document.Encoding == storage.DocumentEncodingBSON {
		// Canonical Extended JSON retains BSON numeric types before Lua conversion.
		extended, err := bson.MarshalExtJSON(bson.Raw(document.Payload), true, false)
		if err != nil {
			return result, fmt.Errorf("encode BSON as Extended JSON: %w", err)
		}
		encoded = extended
	}
	decoded, err := decodeJSONValue(encoded)
	if err != nil {
		return result, err
	}
	if document.Encoding == storage.DocumentEncodingBSON {
		decoded, err = normalizeExtendedJSON(decoded)
		if err != nil {
			return result, err
		}
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return result, errors.New("document must be an object")
	}
	result.encoding = document.Encoding
	result.value = object
	return result, nil
}

func decodeJSONValue(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode JSON: unexpected trailing content")
	}
	return decoded, nil
}

func normalizeExtendedJSON(value any) (any, error) {
	switch typed := value.(type) {
	case []any:
		for index, item := range typed {
			converted, err := normalizeExtendedJSON(item)
			if err != nil {
				return nil, err
			}
			typed[index] = converted
		}
	case map[string]any:
		dateTime, isDateTime, err := extendedJSONDateTime(typed)
		if err != nil {
			return nil, err
		}
		if isDateTime {
			return bsonDateTime(dateTime), nil
		}
		if len(typed) == 1 {
			for key, item := range typed {
				text, ok := item.(string)
				if !ok {
					break
				}
				switch key {
				case "$numberInt", "$numberLong":
					integer, err := strconv.ParseInt(text, 10, 64)
					if err != nil {
						return nil, err
					}
					number := bsonInteger{value: integer, wide: key == "$numberLong"}
					return number, nil
				case "$numberDouble":
					number, err := strconv.ParseFloat(text, 64)
					if err != nil {
						return nil, err
					}
					// Keep non-finite BSON doubles in their existing Extended JSON form.
					if !math.IsNaN(number) && !math.IsInf(number, 0) {
						return number, nil
					}
				}
			}
		}
		for key, item := range typed {
			converted, err := normalizeExtendedJSON(item)
			if err != nil {
				return nil, err
			}
			typed[key] = converted
		}
	}
	return value, nil
}

func extendedJSONDateTime(value map[string]any) (string, bool, error) {
	raw, exists := value["$date"]
	if !exists || len(value) != 1 {
		return "", false, nil
	}
	var timestamp time.Time
	switch typed := raw.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return "", false, fmt.Errorf("parse BSON date-time: %w", err)
		}
		timestamp = parsed
	case map[string]any:
		number, ok := typed["$numberLong"].(string)
		if !ok || len(typed) != 1 {
			return "", false, errors.New("BSON date-time has an invalid $numberLong value")
		}
		milliseconds, err := strconv.ParseInt(number, 10, 64)
		if err != nil {
			return "", false, fmt.Errorf("parse BSON date-time milliseconds: %w", err)
		}
		timestamp = time.UnixMilli(milliseconds)
	default:
		return "", false, fmt.Errorf("BSON date-time has type %T", raw)
	}
	encoded, err := timestamp.UTC().MarshalJSON()
	if err != nil {
		return "", false, fmt.Errorf("encode BSON date-time: %w", err)
	}
	var text string
	if err := json.Unmarshal(encoded, &text); err != nil {
		return "", false, err
	}
	return text, true, nil
}

func (b *luaJSONBridge) goToLua(value any) (vm.Value, error) {
	switch typed := value.(type) {
	case nil:
		return vm.NewTable(b.nullTable), nil
	case bool:
		return vm.NewBool(typed), nil
	case string:
		return vm.NewString(typed), nil
	case bsonDateTime:
		text := strings.Clone(string(typed))
		b.dateTimes[identityOfLuaString(text)] = struct{}{}
		return vm.NewString(text), nil
	case bsonInteger:
		kind := uint8(1)
		if typed.wide {
			kind = 2
		}
		b.integerKinds[typed.value] |= kind
		return vm.NewInt(typed.value), nil
	case float64:
		return vm.NewFloat(typed), nil
	case json.Number:
		return jsonNumberToLua(typed)
	case []any:
		table := vm.NewTableWithSize(len(typed), 0)
		table.SetMetatable(b.arrayMeta)
		for index, item := range typed {
			converted, err := b.goToLua(item)
			if err != nil {
				return vm.Nil, err
			}
			table.SetInt(index+1, converted)
			b.rememberInteger(table, vm.NewInt(int64(index+1)), item)
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
			b.rememberInteger(table, vm.NewString(key), item)
		}
		return vm.NewTable(table), nil
	default:
		return vm.Nil, fmt.Errorf("unsupported Go value %T", value)
	}
}

func jsonNumberToLua(number json.Number) (vm.Value, error) {
	integer, err := number.Int64()
	if err == nil {
		return vm.NewInt(integer), nil
	}
	value, err := number.Float64()
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return vm.Nil, fmt.Errorf("invalid JSON number %q", number)
	}
	integer, integral, err := exactJSONInteger(string(number))
	if err != nil {
		return vm.Nil, err
	}
	if integral {
		// Decimal/exponent notation must not bypass the integer precision check.
		if float64(integer) != value || value < -0x1p63 || value >= 0x1p63 || int64(value) != integer {
			return vm.NewInt(integer), nil
		}
	} else if value == 0 {
		return vm.Nil, fmt.Errorf("JSON number %q underflows a Lua number", number)
	}
	return vm.NewFloat(value), nil
}

// Examine decimal digits without constructing powers of ten. A very large
// exponent must not turn a short input into an unbounded big-number allocation.
func exactJSONInteger(text string) (int64, bool, error) {
	mantissa := text
	exponent := int64(0)
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		mantissa = text[:index]
		parsed, err := strconv.ParseInt(text[index+1:], 10, 32)
		if err != nil {
			return 0, false, fmt.Errorf("JSON number %q has an unsupported exponent", text)
		}
		exponent = parsed
	}
	sign := ""
	if strings.HasPrefix(mantissa, "-") {
		sign, mantissa = "-", mantissa[1:]
	}
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		exponent -= int64(len(mantissa) - index - 1)
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return 0, true, nil
	}
	if exponent < 0 {
		end := int64(len(digits)) + exponent
		if end <= 0 || strings.Trim(digits[end:], "0") != "" {
			return 0, false, nil
		}
		digits, exponent = digits[:end], 0
	}
	if int64(len(digits))+exponent > 19 {
		return 0, true, fmt.Errorf("JSON integer %q is outside the signed 64-bit range", text)
	}
	digits += strings.Repeat("0", int(exponent))
	integer, err := strconv.ParseInt(sign+digits, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("JSON integer %q is outside the signed 64-bit range", text)
	}
	return integer, true, nil
}

func (b *luaJSONBridge) rememberInteger(table *vm.Table, key vm.Value, item any) {
	integer, ok := item.(bsonInteger)
	if !ok {
		return
	}
	fields := b.integerFields[table]
	if fields == nil {
		fields = make(map[vm.Value]bsonInteger)
		b.integerFields[table] = fields
	}
	fields[key] = integer
}

func (b *luaJSONBridge) encodeJSONObject(value vm.Value, encoding storage.DocumentEncoding) (storage.Document, error) {
	var document storage.Document
	b.outputBSON = encoding == storage.DocumentEncodingBSON
	decoded, err := b.luaToGo(value, make(map[*vm.Table]bool))
	if err != nil {
		return document, err
	}
	if _, ok := decoded.(map[string]any); !ok {
		return document, errors.New("merge result must be a JSON object")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return document, fmt.Errorf("encode JSON: %w", err)
	}
	switch encoding {
	case storage.DocumentEncodingJSON:
	case storage.DocumentEncodingBSON:
		var fields bson.D
		if err := bson.UnmarshalExtJSON(encoded, false, &fields); err != nil {
			return document, fmt.Errorf("decode BSON Extended JSON: %w", err)
		}
		encoded, err = bson.Marshal(fields)
		if err != nil {
			return document, fmt.Errorf("encode BSON: %w", err)
		}
	default:
		return document, errors.New("merge result encoding is required")
	}
	document.Encoding = encoding
	document.Payload = encoded
	return document, nil
}

func bsonIntegerJSON(value int64, wide bool) map[string]string {
	name := "$numberInt"
	if wide || value < math.MinInt32 || value > math.MaxInt32 {
		name = "$numberLong"
	}
	result := map[string]string{name: strconv.FormatInt(value, 10)}
	return result
}

func (b *luaJSONBridge) luaFieldToGo(table *vm.Table, key vm.Value, value vm.Value, active map[*vm.Table]bool) (any, error) {
	if b.outputBSON && value.IsInt() {
		if integer, exists := b.integerFields[table][key]; exists {
			return bsonIntegerJSON(value.AsInt(), integer.wide), nil
		}
	}
	return b.luaToGo(value, active)
}

func (b *luaJSONBridge) luaToGo(value vm.Value, active map[*vm.Table]bool) (any, error) {
	switch {
	case value.IsNil():
		return nil, nil
	case value.IsBool():
		return value.AsBool(), nil
	case value.IsString():
		text := value.AsString()
		if _, typed := b.dateTimes[identityOfLuaString(text)]; typed && b.outputBSON {
			result := map[string]string{"$date": text}
			return result, nil
		}
		return text, nil
	case value.IsInt():
		number := value.AsInt()
		if b.outputBSON {
			kind := b.integerKinds[number]
			if kind == 3 {
				return nil, errors.New("copied BSON integer has ambiguous int32/int64 origins; retain its original document field")
			}
			return bsonIntegerJSON(number, kind == 2), nil
		}
		return json.Number(strconv.FormatInt(number, 10)), nil
	case value.IsFloat():
		number := value.AsFloat()
		if math.IsInf(number, 0) || math.IsNaN(number) {
			return nil, errors.New("lua result contains a non-finite number")
		}
		if b.outputBSON {
			result := map[string]string{"$numberDouble": strconv.FormatFloat(number, 'g', -1, 64)}
			return result, nil
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
		item, err := b.luaFieldToGo(table, vm.NewInt(int64(index)), table.GetInt(index), active)
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
		converted, err := b.luaFieldToGo(table, next, value, active)
		if err != nil {
			return nil, err
		}
		result[next.AsString()] = converted
		key = next
	}
	return result, nil
}
