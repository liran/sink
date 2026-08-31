package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type luaCommandFixture struct {
	directory string
	script    string
}

func TestLuaTestCommandRunsSingleJSONCase(t *testing.T) {
	fixture := newLuaCommandFixture(t, `
return function(current, incoming)
    current = current or json.object()
    current.keep = current.keep
    current.value = incoming.value
    current.updated_at = sink.v1.time.now()
    return current
end`)
	current := fixture.write(t, "current.json", `{"keep":true,"value":"old"}`)
	incoming := fixture.write(t, "incoming.json", `{"value":"new"}`)
	expected := fixture.write(t, "expected.json", `{"keep":true,"value":"new","updated_at":"2026-08-31T10:20:30.456Z"}`)
	args := []string{
		"--script", fixture.script,
		"--encoding", "json",
		"--current", current,
		"--incoming", incoming,
		"--expected", expected,
		"--observed-at", "2026-08-31T10:20:30.456Z",
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runLuaTestCommand() error = %v, stderr = %s", err, stderr.String())
	}
	if stdout.String() != "PASS single\n" {
		t.Fatalf("runLuaTestCommand() stdout = %q", stdout.String())
	}
}

func TestLuaTestCommandPrintsCanonicalBSONResult(t *testing.T) {
	fixture := newLuaCommandFixture(t, `
return function(current, incoming)
    return {
        created_at = incoming.created_at,
        literal = incoming.literal,
        updated_at = sink.v1.time.now(),
    }
end`)
	incoming := fixture.write(t, "incoming.extjson", `{
  "created_at": {"$date": "2026-08-29T04:34:56.789Z"},
  "literal": "2026-08-29T04:34:56.789Z"
}`)
	args := []string{
		"--script", fixture.script,
		"--encoding", "bson",
		"--incoming", incoming,
		"--observed-at", "2026-08-31T10:20:30.456Z",
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runLuaTestCommand() error = %v, stderr = %s", err, stderr.String())
	}
	output := stdout.String()
	for _, wanted := range []string{
		`"created_at": {`,
		`"$date": {`,
		`"$numberLong": "1787978096789"`,
		`"literal": "2026-08-29T04:34:56.789Z"`,
		`"$numberLong": "1788171630456"`,
	} {
		if !strings.Contains(output, wanted) {
			t.Fatalf("runLuaTestCommand() stdout does not contain %q:\n%s", wanted, output)
		}
	}
}

func TestLuaTestCommandRunsCaseDirectoryAndReportsAllFailures(t *testing.T) {
	fixture := newLuaCommandFixture(t, `return function(current, incoming) return incoming end`)
	casesDirectory := filepath.Join(fixture.directory, "cases")
	if err := os.Mkdir(casesDirectory, 0o755); err != nil {
		t.Fatalf("Mkdir(cases) error = %v", err)
	}
	fixture.write(t, "cases/incoming.json", `{"value":"actual"}`)
	fixture.write(t, "cases/expected-pass.json", `{"value":"actual"}`)
	fixture.write(t, "cases/expected-fail.json", `{"value":"expected"}`)
	fixture.write(t, "cases/01-pass.yaml", `
name: matching result
encoding: json
incoming: incoming.json
expected: expected-pass.json
observed_at: 2026-08-31T10:20:30Z
`)
	fixture.write(t, "cases/02-fail.yaml", `
name: mismatching result
encoding: json
incoming: incoming.json
expected: expected-fail.json
observed_at: 2026-08-31T10:20:30Z
`)
	args := []string{"--script", fixture.script, "--cases", casesDirectory}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "1 of 2 Lua test cases failed") ||
		!strings.Contains(err.Error(), "document mismatch") {
		t.Fatalf("runLuaTestCommand() error = %v", err)
	}
	wantedOutput := "PASS matching result\nFAIL mismatching result\n"
	if stdout.String() != wantedOutput {
		t.Fatalf("runLuaTestCommand() stdout = %q, want %q", stdout.String(), wantedOutput)
	}
}

func TestLuaTestCommandRejectsUnknownCaseFields(t *testing.T) {
	fixture := newLuaCommandFixture(t, `return function(current, incoming) return incoming end`)
	casePath := fixture.write(t, "case.yaml", `
name: invalid field
encoding: json
incoming: incoming.json
expected: expected.json
observed_at: 2026-08-31T10:20:30Z
unexpected: true
`)
	args := []string{"--script", fixture.script, "--cases", casePath}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("runLuaTestCommand() error = %v", err)
	}
}

func TestLuaTestCommandUsesRestrictedProductionEnvironment(t *testing.T) {
	fixture := newLuaCommandFixture(t, `
return function(current, incoming)
    return {value = os.time()}
end`)
	incoming := fixture.write(t, "incoming.json", `{}`)
	args := []string{
		"--script", fixture.script,
		"--encoding", "json",
		"--incoming", incoming,
		"--observed-at", "2026-08-31T10:20:30Z",
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "execute Lua merge") {
		t.Fatalf("runLuaTestCommand() error = %v", err)
	}
}

func TestLuaTestCommandUsesConfiguredLimitsWithoutOpeningBackends(t *testing.T) {
	fixture := newLuaCommandFixture(t, `return function(current, incoming) return incoming end`)
	config := fixture.write(t, "sink.yaml", `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://unreachable.invalid:27017
service:
  lua:
    max_source_bytes: 1
`)
	incoming := fixture.write(t, "incoming.json", `{}`)
	args := []string{
		"--script", fixture.script,
		"--config", config,
		"--encoding", "json",
		"--incoming", incoming,
		"--observed-at", "2026-08-31T10:20:30Z",
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "maximum is 1") {
		t.Fatalf("runLuaTestCommand() error = %v", err)
	}
}

func TestLuaTestCommandValidatesArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "script", args: nil, want: "--script is required"},
		{name: "encoding", args: []string{"--script", "merge.lua"}, want: "--encoding is required"},
		{name: "incoming", args: []string{"--script", "merge.lua", "--encoding", "json"}, want: "--incoming is required"},
		{name: "observation time", args: []string{"--script", "merge.lua", "--encoding", "json", "--incoming", "incoming.json"}, want: "--observed-at is required"},
		{name: "mixed modes", args: []string{"--script", "merge.lua", "--cases", "cases", "--encoding", "json"}, want: "--cases cannot be combined"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			_, err := parseLuaTestFlags(test.args, &stderr)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parseLuaTestFlags() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLuaTestCommandHelpSucceeds(t *testing.T) {
	args := []string{"--help"}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runLuaTestCommand(--help) error = %v", err)
	}
	if !strings.Contains(stderr.String(), "Usage of sink lua test") ||
		!strings.Contains(stderr.String(), "-cases") {
		t.Fatalf("runLuaTestCommand(--help) stderr = %q", stderr.String())
	}
}

func TestLuaTestCommandRejectsInvalidProgram(t *testing.T) {
	fixture := newLuaCommandFixture(t, `return function(`)
	incoming := fixture.write(t, "incoming.json", `{}`)
	args := []string{
		"--script", fixture.script,
		"--encoding", "json",
		"--incoming", incoming,
		"--observed-at", "2026-08-31T10:20:30Z",
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLuaTestCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "compile Lua script") {
		t.Fatalf("runLuaTestCommand() error = %v", err)
	}
}

func TestExecuteCommandDispatchesVersionAndLua(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	versionArgs := []string{"version"}
	err := executeCommand(versionArgs, &stdout, &stderr)
	if err != nil {
		t.Fatalf("executeCommand(version) error = %v", err)
	}
	if stdout.String() != version+"\n" {
		t.Fatalf("executeCommand(version) stdout = %q", stdout.String())
	}

	unknownArgs := []string{"lua", "unknown"}
	err = executeCommand(unknownArgs, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unknown lua subcommand") {
		t.Fatalf("executeCommand(lua unknown) error = %v", err)
	}
}

func newLuaCommandFixture(t *testing.T, source string) luaCommandFixture {
	t.Helper()
	directory := t.TempDir()
	fixture := luaCommandFixture{directory: directory}
	fixture.script = fixture.write(t, "merge.lua", source)
	return fixture
}

func (f luaCommandFixture) write(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(f.directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", name, err)
	}
	return path
}
