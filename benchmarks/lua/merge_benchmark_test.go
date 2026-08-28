package luabench

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

type luaMergeEngine interface {
	Merge(currentJSON, incomingJSON []byte) ([]byte, error)
	Close()
}

type engineFactory func() (luaMergeEngine, error)

var benchmarkResult []byte

func TestLuaProductMergeMatchesNative(t *testing.T) {
	factories := map[string]engineFactory{
		"gopher-lua": func() (luaMergeEngine, error) {
			return newGopherLuaEngine(true)
		},
		"ice-lua": func() (luaMergeEngine, error) {
			return newIceLuaEngine(true)
		},
	}
	options := fixtureOptions{name: "semantic", itemCount: 50}
	fixtures := []benchmarkFixture{
		makeBenchmarkFixture(options),
		makeSparseBenchmarkFixture(),
		makeTrimOnlyBenchmarkFixture(),
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			expected, err := nativeProductMerge(fixture.currentJSON, fixture.incomingJSON)
			if err != nil {
				t.Fatal(err)
			}
			for name, factory := range factories {
				t.Run(name, func(t *testing.T) {
					engine, err := factory()
					if err != nil {
						t.Fatal(err)
					}
					defer engine.Close()
					actual, err := engine.Merge(fixture.currentJSON, fixture.incomingJSON)
					if err != nil {
						t.Fatal(err)
					}
					assertEquivalentDocuments(t, expected, actual)
				})
			}
		})
	}
}

func TestLuaIntegerPrecision(t *testing.T) {
	currentJSON := []byte(`{"uid":"generic:precision","platform":"generic","id":"precision","url":"https://example.com/precision","comment_count":9007199254740993}`)
	incomingJSON := []byte(`{"uid":"generic:precision","platform":"generic","id":"precision","url":"https://example.com/precision"}`)

	gopher, err := newGopherLuaEngine(false)
	if err != nil {
		t.Fatal(err)
	}
	gopherResult, err := gopher.Merge(currentJSON, incomingJSON)
	gopher.Close()
	if err != nil {
		t.Fatal(err)
	}
	gopherValue := jsonIntegerField(t, gopherResult, "comment_count")
	if gopherValue == "9007199254740993" {
		t.Fatal("expected Lua 5.1 float conversion to demonstrate unsafe integer precision")
	}
	t.Logf("gopher-lua converted 9007199254740993 to %s", gopherValue)

	iceLua, err := newIceLuaEngine(false)
	if err != nil {
		t.Fatal(err)
	}
	iceLuaResult, err := iceLua.Merge(currentJSON, incomingJSON)
	iceLua.Close()
	if err != nil {
		t.Fatal(err)
	}
	if value := jsonIntegerField(t, iceLuaResult, "comment_count"); value != "9007199254740993" {
		t.Fatalf("Lua 5.4 integer precision changed: got %s", value)
	}
}

func TestBenchmarkFixtureSizes(t *testing.T) {
	t.Log(fixtureSummary(benchmarkFixtures()))
}

func BenchmarkProductMerge(b *testing.B) {
	factories := map[string]engineFactory{
		"gopher-lua": func() (luaMergeEngine, error) {
			return newGopherLuaEngine(false)
		},
		"gopher-lua-timeout": func() (luaMergeEngine, error) {
			return newGopherLuaEngine(true)
		},
		"ice-lua": func() (luaMergeEngine, error) {
			return newIceLuaEngine(false)
		},
		"ice-lua-limits": func() (luaMergeEngine, error) {
			return newIceLuaEngine(true)
		},
	}
	for _, fixture := range benchmarkFixtures() {
		fixture := fixture
		b.Run(fixture.name, func(b *testing.B) {
			b.Run("json-round-trip", func(b *testing.B) {
				benchmarkJSONRoundTrip(b, fixture)
			})
			b.Run("native-go", func(b *testing.B) {
				benchmarkNativeMerge(b, fixture)
			})
			for name, factory := range factories {
				name := name
				factory := factory
				b.Run(name, func(b *testing.B) {
					benchmarkLuaMerge(b, fixture, factory)
				})
			}
		})
	}
}

func BenchmarkProductMergeParallelMedium(b *testing.B) {
	options := fixtureOptions{name: "medium", itemCount: 50}
	fixture := makeBenchmarkFixture(options)
	factories := map[string]engineFactory{
		"gopher-lua-timeout": func() (luaMergeEngine, error) {
			return newGopherLuaEngine(true)
		},
		"ice-lua-limits": func() (luaMergeEngine, error) {
			return newIceLuaEngine(true)
		},
	}
	for name, factory := range factories {
		name := name
		factory := factory
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
			b.RunParallel(func(parallel *testing.PB) {
				engine, err := factory()
				if err != nil {
					b.Fatal(err)
				}
				defer engine.Close()
				var result []byte
				for parallel.Next() {
					result, err = engine.Merge(fixture.currentJSON, fixture.incomingJSON)
					if err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(result)
			})
		})
	}
}

func BenchmarkLuaEngineColdStart(b *testing.B) {
	factories := map[string]engineFactory{
		"gopher-lua": func() (luaMergeEngine, error) {
			return newGopherLuaEngine(false)
		},
		"ice-lua": func() (luaMergeEngine, error) {
			return newIceLuaEngine(false)
		},
	}
	for name, factory := range factories {
		name := name
		factory := factory
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				engine, err := factory()
				if err != nil {
					b.Fatal(err)
				}
				engine.Close()
			}
		})
	}
}

func BenchmarkProductMergeFreshVM(b *testing.B) {
	for _, fixture := range benchmarkFixtures() {
		fixture := fixture
		b.Run(fixture.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
			for index := 0; index < b.N; index++ {
				engine, err := newIceLuaEngine(true)
				if err != nil {
					b.Fatal(err)
				}
				result, err := engine.Merge(fixture.currentJSON, fixture.incomingJSON)
				engine.Close()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkResult = result
			}
		})
	}
}

func BenchmarkFiveKilobyteDocuments(b *testing.B) {
	fixture := makeBalancedBenchmarkFixture("five-kilobyte-documents", 5)
	b.Logf("current=%dB incoming=%dB", len(fixture.currentJSON), len(fixture.incomingJSON))
	b.Run("reused-vm", func(b *testing.B) {
		factory := func() (luaMergeEngine, error) {
			return newIceLuaEngine(true)
		}
		benchmarkLuaMerge(b, fixture, factory)
	})
	b.Run("fresh-vm", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
		for index := 0; index < b.N; index++ {
			engine, err := newIceLuaEngine(true)
			if err != nil {
				b.Fatal(err)
			}
			result, err := engine.Merge(fixture.currentJSON, fixture.incomingJSON)
			engine.Close()
			if err != nil {
				b.Fatal(err)
			}
			benchmarkResult = result
		}
	})
}

func benchmarkNativeMerge(b *testing.B, fixture benchmarkFixture) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := nativeProductMerge(fixture.currentJSON, fixture.incomingJSON)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkResult = result
	}
}

func benchmarkJSONRoundTrip(b *testing.B, fixture benchmarkFixture) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		current, err := decodeJSON(fixture.currentJSON)
		if err != nil {
			b.Fatal(err)
		}
		_, err = decodeJSON(fixture.incomingJSON)
		if err != nil {
			b.Fatal(err)
		}
		result, err := json.Marshal(current)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkResult = result
	}
}

func benchmarkLuaMerge(b *testing.B, fixture benchmarkFixture, factory engineFactory) {
	b.Helper()
	engine, err := factory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.currentJSON) + len(fixture.incomingJSON)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := engine.Merge(fixture.currentJSON, fixture.incomingJSON)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkResult = result
	}
}

func assertEquivalentDocuments(t *testing.T, expectedJSON, actualJSON []byte) {
	t.Helper()
	expected := newBenchmarkProduct()
	if err := json.Unmarshal(expectedJSON, expected); err != nil {
		t.Fatal(err)
	}
	actual := newBenchmarkProduct()
	if err := json.Unmarshal(actualJSON, actual); err != nil {
		t.Fatal(err)
	}
	if expected.FirstFoundAt != nil && expected.LastFoundAt != nil && expected.FirstFoundAt.Equal(*expected.LastFoundAt) {
		expected.FirstFoundAt = nil
	}
	if actual.FirstFoundAt != nil && actual.LastFoundAt != nil && actual.FirstFoundAt.Equal(*actual.LastFoundAt) {
		actual.FirstFoundAt = nil
	}
	expected.LastFoundAt = nil
	actual.LastFoundAt = nil
	if !reflect.DeepEqual(expected, actual) {
		expectedPretty, _ := json.MarshalIndent(expected, "", "  ")
		actualPretty, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf("merged document mismatch\nexpected:\n%s\nactual:\n%s", expectedPretty, actualPretty)
	}
}

func jsonIntegerField(t *testing.T, data []byte, field string) string {
	t.Helper()
	value, err := decodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatal("result is not an object")
	}
	number, ok := object[field].(json.Number)
	if !ok {
		t.Fatalf("field %s is %T, not json.Number", field, object[field])
	}
	return number.String()
}

func fixtureSummary(fixtures []benchmarkFixture) string {
	result := ""
	for _, fixture := range fixtures {
		if result != "" {
			result += ", "
		}
		result += fmt.Sprintf(
			"%s=%dB+%dB",
			fixture.name,
			len(fixture.currentJSON),
			len(fixture.incomingJSON),
		)
	}
	return result
}
