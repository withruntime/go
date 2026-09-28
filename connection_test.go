package withruntime

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The file `runtime login` writes, named as the CLI names it.
func TestTheConnectionFileIsNamedAsTheCLINamesIt(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/config")
	_, file, err := connectionFile("https://api.withruntime.com", "https://withruntime.com")
	if err != nil {
		t.Fatal(err)
	}
	// sha256("https://withruntime.com\nhttps://api.withruntime.com")
	want := "/config/runtime-cloud/46e10a03492c1fab7df112410db1b48ddf6c946c3c8df5e66996722273a383d4.json"
	if file != want {
		t.Fatalf("got %s", file)
	}
}

func save(t *testing.T, root, apiOrigin string, value any, dirMode, fileMode os.FileMode) {
	t.Helper()
	directory, file, err := connectionFile(apiOrigin, "https://withruntime.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, dirMode); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(directory, dirMode)
	text, _ := json.Marshal(value)
	if err := os.WriteFile(file, text, fileMode); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(file, fileMode)
}

func TestTheSavedConnectionIsUsedWhenNoKeyIsGiven(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("RUNTIME_API_KEY", "")
	t.Setenv("RUNTIME_API_URL", "")
	t.Setenv("RUNTIME_AUTH_URL", "")
	if _, err := New(); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("with nothing saved, want missing_api_key, got %v", err)
	}
	connection := map[string]any{
		"version": 1, "apiOrigin": "https://api.withruntime.com", "authOrigin": "https://withruntime.com",
		"key": testKey, "connectionId": "c1", "orgId": "o1", "agentName": "laptop",
	}
	save(t, root, "https://api.withruntime.com", connection, 0o700, 0o600)
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if c.apiKey != testKey {
		t.Fatal("the saved key was not used")
	}
	t.Setenv("RUNTIME_API_KEY", "rk_env")
	if c, _ := New(); c.apiKey != "rk_env" {
		t.Fatal("RUNTIME_API_KEY must win over the saved connection")
	}
	if c, _ := New(WithAPIKey("rk_given")); c.apiKey != "rk_given" {
		t.Fatal("a given key must win")
	}
}

func TestASavedConnectionOthersCanReadIsRefused(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows keeps its own access lists")
	}
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("RUNTIME_API_KEY", "")
	t.Setenv("RUNTIME_API_URL", "")
	t.Setenv("RUNTIME_AUTH_URL", "")
	connection := map[string]any{
		"version": 1, "apiOrigin": "https://api.withruntime.com", "authOrigin": "https://withruntime.com",
		"key": testKey, "connectionId": "c1", "orgId": "o1", "agentName": "laptop",
	}
	save(t, root, "https://api.withruntime.com", connection, 0o700, 0o644)
	if _, err := New(); err == nil {
		t.Fatal("a readable file must be refused")
	}
	save(t, root, "https://api.withruntime.com", connection, 0o755, 0o600)
	if _, err := New(); err == nil {
		t.Fatal("a readable directory must be refused")
	}
	_ = os.Chmod(filepath.Join(root, "runtime-cloud"), 0o700)
	connection["apiOrigin"] = "https://elsewhere.example"
	save(t, root, "https://api.withruntime.com", connection, 0o700, 0o600)
	if _, err := New(); err == nil {
		t.Fatal("a connection for another origin must be refused")
	}
}

func TestOrigins(t *testing.T) {
	for value, want := range map[string]string{
		"https://api.withruntime.com":  "https://api.withruntime.com",
		"https://api.withruntime.com/": "https://api.withruntime.com",
		"http://localhost:4000":        "http://localhost:4000",
		"http://127.0.0.1:9":           "http://127.0.0.1:9",
	} {
		if got, err := origin(value); err != nil || got != want {
			t.Fatalf("%s: %s %v", value, got, err)
		}
	}
	for _, bad := range []string{"http://api.withruntime.com", "https://api.withruntime.com/v1", "https://u:p@api.withruntime.com", "https://a.example?x=1", "nope"} {
		if _, err := origin(bad); err == nil {
			t.Fatalf("%s should be refused", bad)
		}
	}
}
