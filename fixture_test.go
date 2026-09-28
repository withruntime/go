package withruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

/* The SDK against the REAL API router: packages/cloud-sdk/scripts/python-
   fixture.ts serves the server's own routes, validation and error shapes over
   a fake guest, as it does for the Python SDK. Set RUNTIME_FIXTURE_URL to a
   running one, or have bun on PATH and TestMain starts it. Without either,
   these tests are skipped, never faked. */

var fixtureURL string

const fixtureID = "11111111-2222-4333-8444-555555555555"

func TestMain(m *testing.M) {
	fixtureURL = os.Getenv("RUNTIME_FIXTURE_URL")
	var server *exec.Cmd
	script, _ := filepath.Abs("../../packages/cloud-sdk/scripts/python-fixture.ts")
	if bun, err := exec.LookPath("bun"); fixtureURL == "" && err == nil {
		if _, err := os.Stat(script); err == nil {
			server = exec.Command(bun, script)
			server.Stderr = os.Stderr
			out, _ := server.StdoutPipe()
			if server.Start() == nil {
				line, err := bufio.NewReader(out).ReadString('\n')
				if err == nil {
					fixtureURL = strings.TrimSpace(line)
				}
			}
		}
	}
	code := m.Run()
	if server != nil && server.Process != nil {
		_ = server.Process.Kill()
		_ = server.Wait()
	}
	os.Exit(code)
}

func fixture(t *testing.T) *Client {
	t.Helper()
	if fixtureURL == "" {
		t.Skip("no fixture: set RUNTIME_FIXTURE_URL or put bun on PATH")
	}
	c, err := New(WithAPIKey("rk_test"), WithBaseURL(fixtureURL), WithMaxRetries(3))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFixtureHelloWorld(t *testing.T) {
	c := fixture(t)
	ctx := context.Background()
	sbx, err := c.Sandboxes.Create(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sbx.State() != "running" || sbx.Info().VCPU != 2 {
		t.Fatalf("state %s vcpu %d", sbx.State(), sbx.Info().VCPU)
	}
	result, err := sbx.Exec(ctx, "python3 -c 'print(6*7)'", nil)
	if err != nil {
		t.Fatal(err)
	}
	if *result.ExitCode != 0 || result.Stdout != "ran python3 -c 'print(6*7)'\n" {
		t.Fatalf("result %+v", result)
	}
	piped, err := sbx.ExecArgv(ctx, []string{"cat"}, &ExecOptions{Stdin: []byte("in"), Env: map[string]string{"TOKEN": "secret"}})
	if err != nil || !strings.Contains(piped.Stdout, "<in>") {
		t.Fatalf("piped %+v %v", piped, err)
	}
}

func TestFixtureCheckAndTypedErrors(t *testing.T) {
	c := fixture(t)
	ctx := context.Background()
	sbx, err := c.Sandboxes.Get(ctx, fixtureID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sbx.Exec(ctx, "exit 3", &ExecOptions{Check: true})
	var failed *CommandError
	if !errors.As(err, &failed) || *failed.ExitCode != 3 || failed.Stderr != "boom\n" {
		t.Fatalf("got %v", err)
	}
	_, err = c.Sandboxes.Create(ctx, &CreateOptions{Extra: map[string]any{"vcpus": 2}})
	var failure *Error
	if !errors.As(err, &failure) || !errors.Is(err, ErrInvalidRequest) || failure.RequestID == "" || failure.Hint == "" {
		t.Fatalf("want a typed invalid_request, got %#v", err)
	}
	// Forks are switched off on the fixture: the router's deliberate refusal,
	// not a 400, which proves Funding is a field it knows.
	_, err = sbx.Fork(ctx, &ForkOptions{Funding: "trial"})
	if !errors.As(err, &failure) || failure.Code != "fork_unavailable" || failure.Retryable() {
		t.Fatalf("want fork_unavailable, got %v", err)
	}
}

func TestFixtureFiles(t *testing.T) {
	c := fixture(t)
	ctx := context.Background()
	sbx, _ := c.Sandboxes.Get(ctx, fixtureID)
	if err := sbx.Files.Write(ctx, "/workspace/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if text, err := sbx.Files.ReadText(ctx, "/workspace/a.txt"); err != nil || text != "hello" {
		t.Fatalf("%q %v", text, err)
	}
	big := make([]byte, 3<<20+17)
	for i := range big {
		big[i] = 7
	}
	if err := sbx.Files.Write(ctx, "/workspace/big.bin", big); err != nil {
		t.Fatal(err)
	}
	if back, err := sbx.Files.Read(ctx, "/workspace/big.bin"); err != nil || string(back) != string(big) {
		t.Fatalf("large file did not round trip: %v", err)
	}
	entries, err := sbx.Files.List(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		found = found || entry.Path == "/workspace/a.txt"
	}
	if !found {
		t.Fatalf("list %+v", entries)
	}
	if ok, err := sbx.Files.Exists(ctx, "/workspace/a.txt"); !ok || err != nil {
		t.Fatal("exists")
	}
	if ok, err := sbx.Files.Exists(ctx, "/workspace/none"); ok || err != nil {
		t.Fatal("not exists")
	}
	_, err = sbx.Files.Read(ctx, "/workspace/none")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "file_not_found" || failure.RequestID == "" || failure.Hint == "" {
		t.Fatalf("got %v", err)
	}
}

func TestFixtureProcessesAndStreams(t *testing.T) {
	c := fixture(t)
	ctx := context.Background()
	sbx, _ := c.Sandboxes.Get(ctx, fixtureID)
	proc, err := sbx.Spawn(ctx, "python3 server.py", &SpawnOptions{PipeStdin: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Write(ctx, []byte("line\n"), false); err != nil {
		t.Fatal(err)
	}
	result, err := proc.Wait(ctx)
	if err != nil || *result.ExitCode != 0 || result.Stdout != "hello\n" {
		t.Fatalf("%+v %v", result, err)
	}
	var types []string
	for event, err := range sbx.ExecStream(ctx, "echo hi", nil) {
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, event.Type)
	}
	if strings.Join(types, ",") != "start,stdout,exit" {
		t.Fatalf("events %v", types)
	}
}

func TestFixturePages(t *testing.T) {
	c := fixture(t)
	page, err := c.Sandboxes.List(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 1 || page.Data[0].ID() != fixtureID || page.HasMore() {
		t.Fatalf("page %+v", page)
	}
}

// The file JavaScript's connectionStore writes is the file this SDK reads.
func TestFixtureTheCLIsSavedConnectionIsRead(t *testing.T) {
	fixture(t)
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun is not on PATH")
	}
	root := t.TempDir()
	store, _ := filepath.Abs("../../packages/cloud-sdk/src/credentials.ts")
	script := `const { connectionStore } = await import(process.argv[1]);
await connectionStore(process.env).save({ version: 1, apiOrigin: "https://api.withruntime.com",
  authOrigin: "https://withruntime.com", key: process.argv[2], connectionId: "c1", orgId: "o1", agentName: "laptop" });`
	cmd := exec.Command(bun, "-e", script, store, testKey)
	cmd.Env = []string{"XDG_CONFIG_HOME=" + root}
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "RUNTIME_") && !strings.HasPrefix(variable, "XDG_CONFIG_HOME=") {
			cmd.Env = append(cmd.Env, variable)
		}
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("RUNTIME_API_KEY", "")
	t.Setenv("RUNTIME_API_URL", "")
	t.Setenv("RUNTIME_AUTH_URL", "")
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if c.apiKey != testKey {
		t.Fatal("the CLI's saved key was not read")
	}
}

// Last in this file, so it sees every route the tests above called.
func TestFixtureZZOnlyRegisteredRoutesWereCalled(t *testing.T) {
	fixture(t)
	response, err := http.Get(fixtureURL + "/__routes")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var seen struct {
		Routes     []string `json:"routes"`
		Mismatches []string `json:"mismatches"`
	}
	if err := json.NewDecoder(response.Body).Decode(&seen); err != nil {
		t.Fatal(err)
	}
	if len(seen.Mismatches) != 0 {
		t.Fatalf("answers broke their published schema: %v", seen.Mismatches)
	}
	if len(seen.Routes) < 8 {
		t.Fatalf("only %d routes reached: %v", len(seen.Routes), seen.Routes)
	}
}

func TestFixtureTerminal(t *testing.T) {
	c := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sbx, _ := c.Sandboxes.Get(ctx, fixtureID)
	term, err := sbx.Terminal(ctx, &TerminalOptions{Cols: 100, Rows: 30})
	if err != nil {
		t.Fatal(err)
	}
	defer term.Close()
	if term.ProcessID() == "" {
		t.Fatal("no process id")
	}
	if err := term.Resize(120, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := term.Write([]byte("echo hi\n")); err != nil {
		t.Fatal(err)
	}
	var seen strings.Builder
	buffer := make([]byte, 1024)
	for !strings.Contains(seen.String(), "echo hi") {
		n, err := term.Read(buffer)
		if err != nil {
			t.Fatalf("read %q: %v", seen.String(), err)
		}
		seen.Write(buffer[:n])
	}
	if _, err := term.Write([]byte("exit\n")); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := term.Read(buffer); err != nil {
			break
		}
	}
	if code := term.ExitCode(); code == nil || *code != 0 {
		t.Fatalf("exit code %v", code)
	}
}
