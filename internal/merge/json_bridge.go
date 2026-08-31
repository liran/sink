package merge

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/iceisfun/golua/vm"
	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type luaJSONBridge struct {
	objectMeta      *vm.Table
	arrayMeta       *vm.Table
	nullMeta        *vm.Table
	nullTable       *vm.Table
	dateTimeValues  map[string]struct{}
	forcedDateTimes map[string]struct{}
	typedPathValues map[string]map[string]struct{}
	untypedValues   map[string]struct{}
}

type decodedJSONObject struct {
	encoding        storage.DocumentEncoding
	value           map[string]any
	dateTimeValues  map[string]struct{}
	typedPathValues map[string]map[string]struct{}
	untypedValues   map[string]struct{}
}

func newLuaJSONBridge(luaVM *vm.VM) *luaJSONBridge {
	bridge := &luaJSONBridge{
		objectMeta:      protectedMetatable("JSON object"),
		arrayMeta:       protectedMetatable("JSON array"),
		nullMeta:        protectedMetatable("JSON null"),
		nullTable:       vm.NewEmptyTable(),
		dateTimeValues:  make(map[string]struct{}),
		forcedDateTimes: make(map[string]struct{}),
		typedPathValues: make(map[string]map[string]struct{}),
		untypedValues:   make(map[string]struct{}),
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
	dateTimePaths := make([]string, 0)
	if document.Encoding == storage.DocumentEncodingBSON {
		extendedJSON, err := bson.MarshalExtJSON(bson.Raw(document.Payload), false, false)
		if err != nil {
			return result, fmt.Errorf("encode BSON as Extended JSON: %w", err)
		}
		decoded, err := decodeJSONValue(extendedJSON)
		if err != nil {
			return result, err
		}
		normalized, err := normalizeExtendedJSON(decoded, "", &dateTimePaths)
		if err != nil {
			return result, err
		}
		encoded, err = json.Marshal(normalized)
		if err != nil {
			return result, fmt.Errorf("encode normalized BSON document: %w", err)
		}
	}
	decoded, err := decodeJSONValue(encoded)
	if err != nil {
		return result, err
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return result, errors.New("document must be an object")
	}
	dateTimeValues := make(map[string]struct{}, len(dateTimePaths))
	typedPaths := make(map[string]struct{}, len(dateTimePaths))
	for _, path := range dateTimePaths {
		typedPaths[path] = struct{}{}
		value, valueErr := valueAtJSONPointer(decoded, jsontext.Pointer(path))
		if valueErr != nil {
			return result, fmt.Errorf("decode BSON date-time path %q: %w", path, valueErr)
		}
		text, textOK := value.(string)
		if !textOK {
			return result, fmt.Errorf("decode BSON date-time path %q: value has type %T", path, value)
		}
		dateTimeValues[text] = struct{}{}
	}
	result.encoding = document.Encoding
	result.value = object
	result.dateTimeValues = dateTimeValues
	result.typedPathValues = make(map[string]map[string]struct{}, len(dateTimePaths))
	result.untypedValues = make(map[string]struct{})
	classifyDateTimeStrings(decoded, "", typedPaths, result.typedPathValues, result.untypedValues)
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

func normalizeExtendedJSON(value any, pointer jsontext.Pointer, dateTimePaths *[]string) (any, error) {
	switch typed := value.(type) {
	case []any:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			child := pointer.AppendToken(strconv.Itoa(index))
			converted, err := normalizeExtendedJSON(item, child, dateTimePaths)
			if err != nil {
				return nil, err
			}
			normalized[index] = converted
		}
		return normalized, nil
	case map[string]any:
		dateTime, isDateTime, err := extendedJSONDateTime(typed)
		if err != nil {
			return nil, err
		}
		if isDateTime {
			*dateTimePaths = append(*dateTimePaths, string(pointer))
			return dateTime, nil
		}
		normalized := make(map[string]any, len(typed))
		for key, item := range typed {
			child := pointer.AppendToken(key)
			converted, err := normalizeExtendedJSON(item, child, dateTimePaths)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}
		return normalized, nil
	default:
		return value, nil
	}
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

func valueAtJSONPointer(value any, pointer jsontext.Pointer) (any, error) {
	current := value
	for token := range pointer.Tokens() {
		switch typed := current.(type) {
		case map[string]any:
			next, exists := typed[token]
			if !exists {
				return nil, fmt.Errorf("object member %q does not exist", token)
			}
			current = next
		case []any:
			index, err := jsonArrayIndex(token, len(typed))
			if err != nil {
				return nil, err
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("cannot traverse %T with token %q", current, token)
		}
	}
	return current, nil
}

func jsonArrayIndex(token string, length int) (int, error) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	index, err := strconv.Atoi(token)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("array index %q is invalid", token)
	}
	if index >= length {
		return 0, fmt.Errorf("array index %d is out of bounds", index)
	}
	return index, nil
}

func cloneJSONValue(value any) any {
	switch typed := value.(type) {
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneJSONValue(item)
		}
		return cloned
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, item := range typed {
			cloned[key] = cloneJSONValue(item)
		}
		return cloned
	default:
		return value
	}
}

func setExtendedJSONDateTime(value any, pointer jsontext.Pointer) error {
	tokens := make([]string, 0)
	for token := range pointer.Tokens() {
		tokens = append(tokens, token)
	}
	if len(tokens) == 0 {
		return errors.New("BSON date-time cannot identify the document root")
	}
	return setExtendedJSONDateTimeAt(value, tokens)
}

func setExtendedJSONDateTimeAt(value any, tokens []string) error {
	token := tokens[0]
	switch typed := value.(type) {
	case map[string]any:
		current, exists := typed[token]
		if !exists {
			return fmt.Errorf("object member %q does not exist", token)
		}
		if len(tokens) > 1 {
			return setExtendedJSONDateTimeAt(current, tokens[1:])
		}
		text, ok := current.(string)
		if !ok {
			return fmt.Errorf("BSON date-time value has type %T", current)
		}
		typed[token] = map[string]any{"$date": text}
		return nil
	case []any:
		index, err := jsonArrayIndex(token, len(typed))
		if err != nil {
			return err
		}
		if len(tokens) > 1 {
			return setExtendedJSONDateTimeAt(typed[index], tokens[1:])
		}
		text, ok := typed[index].(string)
		if !ok {
			return fmt.Errorf("BSON date-time value has type %T", typed[index])
		}
		typed[index] = map[string]any{"$date": text}
		return nil
	default:
		return fmt.Errorf("cannot traverse %T with token %q", value, token)
	}
}

func (b *luaJSONBridge) addDateTimeDocument(document decodedJSONObject) {
	for value := range document.dateTimeValues {
		b.dateTimeValues[value] = struct{}{}
	}
	for value := range document.untypedValues {
		b.untypedValues[value] = struct{}{}
	}
	for path, values := range document.typedPathValues {
		known := b.typedPathValues[path]
		if known == nil {
			known = make(map[string]struct{}, len(values))
			b.typedPathValues[path] = known
		}
		for value := range values {
			known[value] = struct{}{}
		}
	}
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

func (b *luaJSONBridge) encodeJSONObject(
	value vm.Value,
	encoding storage.DocumentEncoding,
) (storage.Document, error) {
	var document storage.Document
	decoded, err := b.luaToGo(value, make(map[*vm.Table]bool))
	if err != nil {
		return document, err
	}
	if _, ok := decoded.(map[string]any); !ok {
		return document, errors.New("merge result must be a JSON object")
	}
	dateTimePaths := b.dateTimePaths(decoded)
	var encoded []byte
	switch encoding {
	case storage.DocumentEncodingJSON:
		encoded, err = json.Marshal(decoded)
		if err != nil {
			return document, fmt.Errorf("encode JSON: %w", err)
		}
	case storage.DocumentEncodingBSON:
		extended := cloneJSONValue(decoded)
		for _, path := range dateTimePaths {
			if err := setExtendedJSONDateTime(extended, jsontext.Pointer(path)); err != nil {
				return document, err
			}
		}
		extendedJSON, marshalErr := json.Marshal(extended)
		if marshalErr != nil {
			return document, fmt.Errorf("encode BSON Extended JSON: %w", marshalErr)
		}
		var bsonDocument bson.D
		if unmarshalErr := bson.UnmarshalExtJSON(extendedJSON, false, &bsonDocument); unmarshalErr != nil {
			return document, fmt.Errorf("decode BSON Extended JSON: %w", unmarshalErr)
		}
		encoded, err = bson.Marshal(bsonDocument)
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

func (b *luaJSONBridge) dateTimePaths(value any) []string {
	paths := make([]string, 0)
	b.collectResultDateTimePaths(value, "", &paths)
	sort.Strings(paths)
	return paths
}

func (b *luaJSONBridge) collectResultDateTimePaths(value any, pointer jsontext.Pointer, paths *[]string) {
	switch typed := value.(type) {
	case string:
		if _, isDateTime := b.dateTimeValues[typed]; !isDateTime {
			return
		}
		path := string(pointer)
		if _, forced := b.forcedDateTimes[typed]; forced {
			*paths = append(*paths, path)
			return
		}
		valuesAtPath := b.typedPathValues[path]
		_, preservedAtPath := valuesAtPath[typed]
		_, alsoUntyped := b.untypedValues[typed]
		if preservedAtPath || !alsoUntyped {
			*paths = append(*paths, path)
		}
	case []any:
		for index, item := range typed {
			child := pointer.AppendToken(strconv.Itoa(index))
			b.collectResultDateTimePaths(item, child, paths)
		}
	case map[string]any:
		for key, item := range typed {
			child := pointer.AppendToken(key)
			b.collectResultDateTimePaths(item, child, paths)
		}
	}
}

func classifyDateTimeStrings(
	value any,
	pointer jsontext.Pointer,
	typedPaths map[string]struct{},
	typedPathValues map[string]map[string]struct{},
	untypedValues map[string]struct{},
) {
	switch typed := value.(type) {
	case string:
		path := string(pointer)
		if _, isTyped := typedPaths[path]; !isTyped {
			untypedValues[typed] = struct{}{}
			return
		}
		values := typedPathValues[path]
		if values == nil {
			values = make(map[string]struct{})
			typedPathValues[path] = values
		}
		values[typed] = struct{}{}
	case []any:
		for index, item := range typed {
			child := pointer.AppendToken(strconv.Itoa(index))
			classifyDateTimeStrings(item, child, typedPaths, typedPathValues, untypedValues)
		}
	case map[string]any:
		for key, item := range typed {
			child := pointer.AppendToken(key)
			classifyDateTimeStrings(item, child, typedPaths, typedPathValues, untypedValues)
		}
	}
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
