package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"gopkg.in/yaml.v3"
)

type luaTestFlags struct {
	script     string
	config     string
	cases      string
	encoding   string
	current    string
	incoming   string
	expected   string
	observedAt string
}

type luaTestCaseFile struct {
	Name       string `yaml:"name"`
	Encoding   string `yaml:"encoding"`
	Current    string `yaml:"current"`
	Incoming   string `yaml:"incoming"`
	Expected   string `yaml:"expected"`
	ObservedAt string `yaml:"observed_at"`
}

type luaTestCase struct {
	name       string
	encoding   storage.DocumentEncoding
	current    string
	incoming   string
	expected   string
	observedAt time.Time
}

type luaTestExecution struct {
	merger     merge.Merger
	encoding   storage.DocumentEncoding
	current    string
	incoming   string
	expected   string
	observedAt time.Time
}

func runLuaCommand(args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("lua subcommand is required; available subcommand: test")
	}
	if args[0] != "test" {
		return fmt.Errorf("unknown lua subcommand %q; available subcommand: test", args[0])
	}
	return runLuaTestCommand(args[1:], stdout, stderr)
}

func runLuaTestCommand(args []string, stdout io.Writer, stderr io.Writer) error {
	parsed, err := parseLuaTestFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	source, err := os.ReadFile(parsed.script)
	if err != nil {
		return fmt.Errorf("read Lua script %q: %w", parsed.script, err)
	}
	luaOptions := merge.LuaOptions{}
	if parsed.config != "" {
		loaded, loadErr := loadConfig(parsed.config)
		if loadErr != nil {
			return fmt.Errorf("load Lua test limits from config: %w", loadErr)
		}
		luaOptions = loaded.luaOptions
	}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		return fmt.Errorf("create Lua test engine: %w", err)
	}
	program := merge.Program{Source: source}
	merger, err := engine.Compile(program)
	if err != nil {
		return fmt.Errorf("compile Lua script %q: %w", parsed.script, err)
	}
	if parsed.cases != "" {
		return runLuaTestCases(parsed.cases, merger, stdout)
	}
	return runSingleLuaTest(parsed, merger, stdout)
}

func parseLuaTestFlags(args []string, stderr io.Writer) (luaTestFlags, error) {
	var parsed luaTestFlags
	flags := flag.NewFlagSet("sink lua test", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&parsed.script, "script", "", "path to the Lua merge script")
	flags.StringVar(&parsed.config, "config", "", "optional Sink config whose service.lua limits are applied")
	flags.StringVar(&parsed.cases, "cases", "", "path to one YAML case or a directory of YAML cases")
	flags.StringVar(&parsed.encoding, "encoding", "", "single-case document encoding: json or bson")
	flags.StringVar(&parsed.current, "current", "", "optional single-case current JSON or Extended JSON document")
	flags.StringVar(&parsed.incoming, "incoming", "", "single-case incoming JSON or Extended JSON document")
	flags.StringVar(&parsed.expected, "expected", "", "optional single-case expected JSON or Extended JSON document")
	flags.StringVar(&parsed.observedAt, "observed-at", "", "single-case fixed RFC3339 observation time")
	if err := flags.Parse(args); err != nil {
		return parsed, fmt.Errorf("parse lua test arguments: %w", err)
	}
	if flags.NArg() != 0 {
		return parsed, fmt.Errorf("unexpected lua test argument %q", flags.Arg(0))
	}
	parsed.script = strings.TrimSpace(parsed.script)
	parsed.config = strings.TrimSpace(parsed.config)
	parsed.cases = strings.TrimSpace(parsed.cases)
	parsed.encoding = strings.TrimSpace(parsed.encoding)
	parsed.current = strings.TrimSpace(parsed.current)
	parsed.incoming = strings.TrimSpace(parsed.incoming)
	parsed.expected = strings.TrimSpace(parsed.expected)
	parsed.observedAt = strings.TrimSpace(parsed.observedAt)
	if parsed.script == "" {
		return parsed, errors.New("--script is required")
	}
	if parsed.cases != "" {
		if parsed.encoding != "" || parsed.current != "" || parsed.incoming != "" ||
			parsed.expected != "" || parsed.observedAt != "" {
			return parsed, errors.New("--cases cannot be combined with single-case document options")
		}
		return parsed, nil
	}
	if parsed.encoding == "" {
		return parsed, errors.New("--encoding is required when --cases is not set")
	}
	if parsed.incoming == "" {
		return parsed, errors.New("--incoming is required when --cases is not set")
	}
	if parsed.observedAt == "" {
		return parsed, errors.New("--observed-at is required when --cases is not set")
	}
	return parsed, nil
}

func runSingleLuaTest(parsed luaTestFlags, merger merge.Merger, stdout io.Writer) error {
	encoding, err := parseLuaDocumentEncoding(parsed.encoding)
	if err != nil {
		return err
	}
	observedAt, err := parseLuaObservedAt(parsed.observedAt)
	if err != nil {
		return err
	}
	execution := luaTestExecution{
		merger:     merger,
		encoding:   encoding,
		current:    parsed.current,
		incoming:   parsed.incoming,
		expected:   parsed.expected,
		observedAt: observedAt,
	}
	result, err := executeLuaTest(execution)
	if err != nil {
		return err
	}
	if parsed.expected != "" {
		_, err = fmt.Fprintln(stdout, "PASS single")
		return err
	}
	formatted, err := formatLuaTestDocument(result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, string(formatted))
	return err
}

func runLuaTestCases(path string, merger merge.Merger, stdout io.Writer) error {
	casePaths, err := discoverLuaTestCases(path)
	if err != nil {
		return err
	}
	failures := make([]error, 0)
	names := make(map[string]string)
	for _, casePath := range casePaths {
		loaded, loadErr := loadLuaTestCase(casePath)
		if loadErr != nil {
			if _, err := fmt.Fprintf(stdout, "FAIL %s\n", filepath.Base(casePath)); err != nil {
				return err
			}
			failures = append(failures, loadErr)
			continue
		}
		if previous, exists := names[loaded.name]; exists {
			if _, err := fmt.Fprintf(stdout, "FAIL %s\n", loaded.name); err != nil {
				return err
			}
			failure := fmt.Errorf("case %q duplicates the name from %q", casePath, previous)
			failures = append(failures, failure)
			continue
		}
		names[loaded.name] = casePath
		execution := luaTestExecution{
			merger:     merger,
			encoding:   loaded.encoding,
			current:    loaded.current,
			incoming:   loaded.incoming,
			expected:   loaded.expected,
			observedAt: loaded.observedAt,
		}
		_, executionErr := executeLuaTest(execution)
		if executionErr != nil {
			if _, err := fmt.Fprintf(stdout, "FAIL %s\n", loaded.name); err != nil {
				return err
			}
			failure := fmt.Errorf("case %q: %w", loaded.name, executionErr)
			failures = append(failures, failure)
			continue
		}
		if _, err := fmt.Fprintf(stdout, "PASS %s\n", loaded.name); err != nil {
			return err
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d Lua test cases failed: %w", len(failures), len(casePaths), errors.Join(failures...))
	}
	_, err = fmt.Fprintf(stdout, "PASS %d cases\n", len(casePaths))
	return err
}

func discoverLuaTestCases(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect Lua test cases %q: %w", path, err)
	}
	if !info.IsDir() {
		if !isYAMLPath(path) {
			return nil, fmt.Errorf("lua test case %q must use a .yaml or .yml extension", path)
		}
		paths := []string{path}
		return paths, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read Lua test case directory %q: %w", path, err)
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !isYAMLPath(entry.Name()) {
			continue
		}
		paths = append(paths, filepath.Join(path, entry.Name()))
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("lua test case directory %q contains no .yaml or .yml files", path)
	}
	return paths, nil
}

func isYAMLPath(path string) bool {
	extension := strings.ToLower(filepath.Ext(path))
	return extension == ".yaml" || extension == ".yml"
}

func loadLuaTestCase(path string) (luaTestCase, error) {
	var loaded luaTestCase
	raw, err := os.ReadFile(path)
	if err != nil {
		return loaded, fmt.Errorf("read Lua test case %q: %w", path, err)
	}
	var file luaTestCaseFile
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		return loaded, fmt.Errorf("decode Lua test case %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return loaded, fmt.Errorf("decode Lua test case %q: %w", path, err)
		}
		return loaded, fmt.Errorf("decode Lua test case %q: multiple YAML documents are not supported", path)
	}
	file.Name = strings.TrimSpace(file.Name)
	file.Encoding = strings.TrimSpace(file.Encoding)
	file.Current = strings.TrimSpace(file.Current)
	file.Incoming = strings.TrimSpace(file.Incoming)
	file.Expected = strings.TrimSpace(file.Expected)
	file.ObservedAt = strings.TrimSpace(file.ObservedAt)
	if file.Name == "" {
		return loaded, fmt.Errorf("lua test case %q: name is required", path)
	}
	if file.Incoming == "" {
		return loaded, fmt.Errorf("lua test case %q: incoming is required", path)
	}
	if file.Expected == "" {
		return loaded, fmt.Errorf("lua test case %q: expected is required", path)
	}
	if file.ObservedAt == "" {
		return loaded, fmt.Errorf("lua test case %q: observed_at is required", path)
	}
	encoding, err := parseLuaDocumentEncoding(file.Encoding)
	if err != nil {
		return loaded, fmt.Errorf("lua test case %q: %w", path, err)
	}
	observedAt, err := parseLuaObservedAt(file.ObservedAt)
	if err != nil {
		return loaded, fmt.Errorf("lua test case %q: %w", path, err)
	}
	base := filepath.Dir(path)
	loaded = luaTestCase{
		name:       file.Name,
		encoding:   encoding,
		current:    resolveLuaTestPath(base, file.Current),
		incoming:   resolveLuaTestPath(base, file.Incoming),
		expected:   resolveLuaTestPath(base, file.Expected),
		observedAt: observedAt,
	}
	return loaded, nil
}

func resolveLuaTestPath(base string, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

func parseLuaDocumentEncoding(value string) (storage.DocumentEncoding, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "json":
		return storage.DocumentEncodingJSON, nil
	case "bson":
		return storage.DocumentEncodingBSON, nil
	default:
		return storage.DocumentEncodingUnspecified, errors.New("encoding must be json or bson")
	}
}

func parseLuaObservedAt(value string) (time.Time, error) {
	observedAt, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		var empty time.Time
		return empty, fmt.Errorf("observed time must use RFC3339: %w", err)
	}
	return observedAt.UTC(), nil
}

func executeLuaTest(execution luaTestExecution) (storage.Document, error) {
	var empty storage.Document
	incoming, err := readLuaTestDocument(execution.incoming, execution.encoding)
	if err != nil {
		return empty, fmt.Errorf("read incoming document: %w", err)
	}
	var current *storage.Document
	if execution.current != "" {
		decoded, currentErr := readLuaTestDocument(execution.current, execution.encoding)
		if currentErr != nil {
			return empty, fmt.Errorf("read current document: %w", currentErr)
		}
		current = &decoded
	}
	request := merge.Request{
		Current:    current,
		Incoming:   incoming,
		ObservedAt: execution.observedAt,
	}
	result, err := execution.merger.Merge(context.Background(), request)
	if err != nil {
		return empty, fmt.Errorf("execute Lua merge: %w", err)
	}
	if execution.expected == "" {
		return result.Document, nil
	}
	expected, err := readLuaTestDocument(execution.expected, execution.encoding)
	if err != nil {
		return empty, fmt.Errorf("read expected document: %w", err)
	}
	if err := compareLuaTestDocuments(expected, result.Document); err != nil {
		return empty, err
	}
	return result.Document, nil
}

func readLuaTestDocument(path string, encoding storage.DocumentEncoding) (storage.Document, error) {
	var document storage.Document
	raw, err := os.ReadFile(path)
	if err != nil {
		return document, fmt.Errorf("read document %q: %w", path, err)
	}
	switch encoding {
	case storage.DocumentEncodingJSON:
		document.Encoding = storage.DocumentEncodingJSON
		document.Payload = raw
	case storage.DocumentEncodingBSON:
		var value bson.D
		if err := bson.UnmarshalExtJSON(raw, false, &value); err != nil {
			return document, fmt.Errorf("decode Extended JSON document %q: %w", path, err)
		}
		encoded, err := bson.Marshal(value)
		if err != nil {
			return document, fmt.Errorf("encode BSON document %q: %w", path, err)
		}
		document.Encoding = storage.DocumentEncodingBSON
		document.Payload = encoded
	default:
		return document, errors.New("document encoding is required")
	}
	if err := storage.ValidateDocument(document); err != nil {
		return document, fmt.Errorf("validate document %q: %w", path, err)
	}
	return document, nil
}

func compareLuaTestDocuments(expected storage.Document, actual storage.Document) error {
	if expected.Encoding != actual.Encoding {
		return fmt.Errorf("document encoding = %d, want %d", actual.Encoding, expected.Encoding)
	}
	expectedValue, expectedFormatted, err := canonicalLuaTestDocument(expected)
	if err != nil {
		return fmt.Errorf("canonicalize expected document: %w", err)
	}
	actualValue, actualFormatted, err := canonicalLuaTestDocument(actual)
	if err != nil {
		return fmt.Errorf("canonicalize actual document: %w", err)
	}
	if reflect.DeepEqual(expectedValue, actualValue) {
		return nil
	}
	return fmt.Errorf("document mismatch\nexpected:\n%s\nactual:\n%s", expectedFormatted, actualFormatted)
}

func formatLuaTestDocument(document storage.Document) ([]byte, error) {
	_, formatted, err := canonicalLuaTestDocument(document)
	return formatted, err
}

func canonicalLuaTestDocument(document storage.Document) (any, []byte, error) {
	var encoded []byte
	var err error
	switch document.Encoding {
	case storage.DocumentEncodingJSON:
		encoded = document.Payload
	case storage.DocumentEncodingBSON:
		encoded, err = bson.MarshalExtJSON(bson.Raw(document.Payload), true, false)
		if err != nil {
			return nil, nil, fmt.Errorf("encode canonical Extended JSON: %w", err)
		}
	default:
		return nil, nil, errors.New("document encoding is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, nil, fmt.Errorf("decode canonical document: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, nil, fmt.Errorf("decode canonical document: %w", err)
		}
		return nil, nil, errors.New("decode canonical document: unexpected trailing content")
	}
	formatted, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("format canonical document: %w", err)
	}
	return value, formatted, nil
}
