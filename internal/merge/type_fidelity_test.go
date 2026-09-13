package merge_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/merge"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLuaMergePreservesBSONNumericTypes(t *testing.T) {
	fields := bson.D{
		{Key: "small", Value: int32(1)}, {Key: "long", Value: int64(1)},
		{Key: "double", Value: float64(1)}, {Key: "negative_zero", Value: math.Copysign(0, -1)},
		{Key: "large", Value: int64(math.MaxInt64)},
		{Key: "array", Value: bson.A{int32(1), int64(1), float64(1)}},
	}
	for _, source := range []string{
		`return function(current, incoming) return incoming end`,
		`return function(current, incoming) incoming.added = true; return incoming end`,
	} {
		t.Run(source, func(t *testing.T) {
			opts := merge.LuaOptions{}
			merger := compileTestProgram(t, []byte(source), opts)
			incoming := bsonDocument(t, fields)
			req := merge.Request{Incoming: incoming}
			result, err := merger.Merge(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			before, after := bson.Raw(incoming.Payload), bson.Raw(result.Document.Payload)
			for _, field := range fields {
				if !before.Lookup(field.Key).Equal(after.Lookup(field.Key)) {
					t.Errorf("field %s changed: %v -> %v", field.Key, before.Lookup(field.Key), after.Lookup(field.Key))
				}
			}
		})
	}
}

func TestLuaMergeBSONIntegersRetainFieldWidthsThroughArithmetic(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming)
 incoming.small = incoming.small + 1
 incoming.long = incoming.long + 1
 incoming.promoted = incoming.promoted + 1
 return incoming
end`)
	merger := compileTestProgram(t, source, opts)
	fields := bson.D{{Key: "small", Value: int32(1)}, {Key: "long", Value: int64(1)}, {Key: "promoted", Value: int32(math.MaxInt32)}}
	incoming := bsonDocument(t, fields)
	req := merge.Request{Incoming: incoming}
	result, err := merger.Merge(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(result.Document.Payload)
	if raw.Lookup("small").Type != bson.TypeInt32 || raw.Lookup("long").Type != bson.TypeInt64 || raw.Lookup("promoted").Type != bson.TypeInt64 {
		t.Fatal(raw)
	}
}

func TestLuaMergeCopiesBSONNumbersWithoutGuessingConflictingWidths(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming) return {value = incoming.value} end`)
	merger := compileTestProgram(t, source, opts)
	for _, conflicting := range []bool{false, true} {
		fields := bson.D{{Key: "value", Value: int64(1)}}
		if conflicting {
			other := bson.E{Key: "other", Value: int32(1)}
			fields = append(fields, other)
		}
		incoming := bsonDocument(t, fields)
		req := merge.Request{Incoming: incoming}
		result, err := merger.Merge(t.Context(), req)
		if conflicting {
			if !errors.Is(err, merge.ErrInvalidResult) || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("ambiguous width: %v", err)
			}
		} else if err != nil || bson.Raw(result.Document.Payload).Lookup("value").Type != bson.TypeInt64 {
			t.Fatalf("copied int64: %v, %v", result, err)
		}
	}
}

func TestLuaMergeDateTimesFollowValuesInsteadOfPaths(t *testing.T) {
	timestamp := time.Date(2026, time.September, 13, 1, 2, 3, 0, time.UTC)
	text := timestamp.Format(time.RFC3339Nano)
	opts := merge.LuaOptions{}
	for _, source := range []string{
		`return function(current, incoming) return incoming end`,
		`return function(current, incoming) current.value = incoming.value; return current end`,
		`return function(current, incoming) return {value = incoming.value} end`,
	} {
		for _, incomingDate := range []bool{false, true} {
			currentFields := bson.D{{Key: "value", Value: timestamp}}
			incomingFields := bson.D{{Key: "value", Value: text}}
			want := bson.TypeString
			if incomingDate {
				currentFields, incomingFields = incomingFields, currentFields
				want = bson.TypeDateTime
			}
			current, incoming := bsonDocument(t, currentFields), bsonDocument(t, incomingFields)
			merger := compileTestProgram(t, []byte(source), opts)
			req := merge.Request{Current: &current, Incoming: incoming}
			result, err := merger.Merge(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got := bson.Raw(result.Document.Payload).Lookup("value").Type; got != want {
				t.Errorf("%s: got %s, want %s", source, got, want)
			}
		}
	}
	source := []byte(`return function(current, incoming) return {copied = {incoming.date, incoming.literal}} end`)
	merger := compileTestProgram(t, source, opts)
	fields := bson.D{{Key: "date", Value: timestamp}, {Key: "literal", Value: text}}
	req := merge.Request{Incoming: bsonDocument(t, fields)}
	result, err := merger.Merge(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	array := bson.Raw(result.Document.Payload).Lookup("copied").Array()
	if array.Index(0).Type != bson.TypeDateTime || array.Index(1).Type != bson.TypeString {
		t.Fatal(array)
	}
}

func TestLuaMergeRejectsJSONIntegerPrecisionLoss(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming) return incoming end`)
	merger := compileTestProgram(t, source, opts)
	for _, number := range []string{"18446744073709551615", "9223372036854775808", "-9223372036854775809", "18446744073709551615.0", "18446744073709551615e0", "1e1000000000", "1e-1000000000"} {
		t.Run(number, func(t *testing.T) {
			req := merge.Request{Incoming: jsonDocument(`{"value":` + number + `}`)}
			_, err := merger.Merge(t.Context(), req)
			if !errors.Is(err, merge.ErrInvalidIncoming) {
				t.Fatalf("accepted inexact or unbounded input: %v", err)
			}
		})
	}
	for input, expected := range map[string]string{
		"9223372036854775807": "9223372036854775807", "-9223372036854775808": "-9223372036854775808",
		"9007199254740993.0": "9007199254740993", "9223372036854775807e0": "9223372036854775807",
		"1.25": "1.25", "1.5e1": "15", "0e100": "0", "-0.0": "-0",
	} {
		req := merge.Request{Incoming: jsonDocument(`{"value":` + input + `}`)}
		result, err := merger.Merge(t.Context(), req)
		if err != nil || string(result.Document.Payload) != `{"value":`+expected+`}` {
			t.Errorf("%s: %s, %v", input, result.Document.Payload, err)
		}
	}
}
