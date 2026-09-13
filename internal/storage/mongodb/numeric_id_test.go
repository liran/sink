package mongodb

import (
	"math"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNumericIDEqualityAndReadKeys(t *testing.T) {
	cases := []struct {
		name  string
		value any
		key   int64
		match bool
	}{
		{name: "int32", value: int32(42), key: 42, match: true},
		{name: "int64", value: int64(42), key: 42, match: true},
		{name: "integral double", value: float64(42), key: 42, match: true},
		{name: "fraction", value: 42.5, key: 42},
		{name: "negative fraction", value: -42.5, key: -42},
		{name: "negative zero", value: math.Copysign(0, -1), key: 0, match: true},
		{name: "nan", value: math.NaN(), key: math.MinInt64},
		{name: "infinity", value: math.Inf(1), key: math.MinInt64},
		{name: "negative infinity", value: math.Inf(-1), key: math.MinInt64},
		{name: "overflow", value: float64(0x1p63), key: math.MinInt64},
		{name: "minimum", value: float64(-0x1p63), key: math.MinInt64, match: true},
		{name: "underflow", value: math.Nextafter(-0x1p63, math.Inf(-1)), key: math.MinInt64},
		{name: "large integer", value: float64(0x1p53), key: 1 << 53, match: true},
		{name: "rounded integer", value: float64(0x1p53), key: (1 << 53) + 1},
		{name: "string", value: "42", key: 42},
	}
	for _, decimal := range []struct {
		text  string
		key   int64
		match bool
	}{
		{text: "42.00", key: 42, match: true},
		{text: "42.5", key: 42},
		{text: "9223372036854775807", key: math.MaxInt64, match: true},
		{text: "-9223372036854775808", key: math.MinInt64, match: true},
		{text: "9223372036854775808", key: math.MinInt64},
		{text: "1E+18", key: 1_000_000_000_000_000_000, match: true},
		{text: "1E+19", key: 1},
		{text: "1E-100", key: 0},
		{text: "0E-100", key: 0, match: true},
		{text: "NaN", key: 0},
		{text: "Infinity", key: math.MinInt64},
	} {
		value, err := bson.ParseDecimal128(decimal.text)
		if err != nil {
			t.Fatal(err)
		}
		item := struct {
			name  string
			value any
			key   int64
			match bool
		}{
			name: "decimal " + decimal.text, value: value, key: decimal.key, match: decimal.match,
		}
		cases = append(cases, item)
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			actual, err := rawValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := rawValue(test.key)
			if err != nil {
				t.Fatal(err)
			}
			if got := recordIDsEqual(actual, expected); got != test.match {
				t.Fatalf("recordIDsEqual(%v, %d) = %t", test.value, test.key, got)
			}
			if got := rawValueKey(actual) == rawValueKey(expected); got != test.match {
				t.Fatalf("read key equality(%v, %d) = %t", test.value, test.key, got)
			}
			store := &Store{metadataField: defaultMetadataField}
			value := bson.D{{Key: "_id", Value: test.value}}
			document := bsonTestDocument(t, value)
			revision := storage.Revision{Data: []byte("test")}
			replacement, err := store.replacement(document, test.key, revision)
			if (err == nil) != test.match {
				t.Fatalf("replacement: %v", err)
			}
			if test.match && !replacement.Lookup("_id").Equal(actual) {
				t.Fatal("replacement changed the original BSON ID representation")
			}
		})
	}
}
