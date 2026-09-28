package withruntime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "rtcloud_00000000-0000-4000-8000-000000000000_" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// server is a fake API: handle answers each request, and every request is
// recorded.
type server struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
}

func newServer(t *testing.T, handle func(n int, w http.ResponseWriter, r *http.Request, body []byte)) *server {
	t.Helper()
	s := &server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, r)
		s.bodies = append(s.bodies, body)
		n := len(s.requests)
		s.mu.Unlock()
		handle(n, w, r, body)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) client(t *testing.T, options ...Option) *Client {
	t.Helper()
	c, err := New(append([]Option{WithAPIKey(testKey), WithBaseURL(s.URL)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func apiError(w http.ResponseWriter, status int, code string, extra map[string]any) {
	body := map[string]any{"code": code, "message": "m " + code, "hint": "h " + code, "requestId": "req_1", "status": status}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, map[string]any{"error": body})
}

var running = map[string]any{"id": "sbx-1", "kind": "sandbox", "state": "running", "status": "active", "vcpu": 2,
	"memoryMiB": 4096, "funding": "trial", "createdAt": "2026-09-23T10:00:00.000Z", "labels": map[string]string{}}

func TestCreateRetriesWithTheSameKeyAndHeaders(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch n {
		case 1:
			w.WriteHeader(http.StatusBadGateway)
		case 2:
			w.Header().Set("Retry-After", "0.01")
			apiError(w, 503, "host_unavailable", nil)
		default:
			writeJSON(w, 200, running)
		}
	})
	sbx, err := s.client(t).Sandboxes.Create(context.Background(), &CreateOptions{Funding: "trial", Labels: map[string]string{"team": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if sbx.ID() != "sbx-1" || sbx.State() != "running" || sbx.Info().VCPU != 2 {
		t.Fatalf("unexpected sandbox %+v", sbx.Info())
	}
	if len(s.requests) != 3 {
		t.Fatalf("want 3 attempts, got %d", len(s.requests))
	}
	key := s.requests[0].Header.Get("Idempotency-Key")
	for i, r := range s.requests {
		if r.Header.Get("Idempotency-Key") != key || key == "" {
			t.Fatalf("attempt %d changed the idempotency key", i)
		}
		if r.Header.Get("Authorization") != "Bearer "+testKey || r.Header.Get("X-Runtime-Client") != "sdk-go/"+Version ||
			r.Header.Get("Prefer") != "wait=60" || r.URL.Path != "/v1/sandboxes" {
			t.Fatalf("headers or path wrong: %v %v", r.Header, r.URL)
		}
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if sent["funding"] != "trial" || sent["labels"].(map[string]any)["team"] != "a" || len(sent) != 2 {
		t.Fatalf("body %s", s.bodies[0])
	}
}

func TestAnExplicitKeyIsSentOnEveryRetry(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if n == 1 {
			apiError(w, 429, "rate_limited", map[string]any{"retryAfterMs": 5})
			return
		}
		writeJSON(w, 200, running)
	})
	if _, err := s.client(t).Sandboxes.Create(context.Background(), &CreateOptions{IdempotencyKey: "mine-1"}); err != nil {
		t.Fatal(err)
	}
	for _, r := range s.requests {
		if r.Header.Get("Idempotency-Key") != "mine-1" {
			t.Fatal("the given key was not sent")
		}
	}
}

func TestADeliberate503FailsAtOnceAndErrorsAreTyped(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		apiError(w, 503, "fork_unavailable", map[string]any{"details": map[string]any{"why": "off"}})
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	_, err := sbx.Fork(context.Background(), &ForkOptions{Funding: "trial"})
	var failure *Error
	if !errors.As(err, &failure) {
		t.Fatalf("want *Error, got %v", err)
	}
	if len(s.requests) != 1 || failure.Code != "fork_unavailable" || failure.Status != 503 ||
		failure.Hint != "h fork_unavailable" || failure.RequestID != "req_1" || failure.Retryable() ||
		!errors.Is(err, ErrServiceUnavailable) || errors.Is(err, ErrNotFound) || failure.IdempotencyKey == "" {
		t.Fatalf("unexpected %d calls, %#v", len(s.requests), failure)
	}
	if strings.Contains(err.Error(), testKey) {
		t.Fatal("the key leaked into an error")
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		apiError(w, 404, "not_found", nil)
	})
	_, err := s.client(t).Sandboxes.Get(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) || len(s.requests) != 1 {
		t.Fatalf("got %v after %d calls", err, len(s.requests))
	}
}

func TestACreateWaitsOutAFullTrialAndCanRefuseAtOnce(t *testing.T) {
	var busy atomic.Int32
	busy.Store(2)
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if busy.Add(-1) >= 0 {
			apiError(w, 409, "trial_busy", map[string]any{"retryAfterMs": 10})
			return
		}
		writeJSON(w, 200, running)
	})
	c := s.client(t, WithMaxRetries(0))
	if _, err := c.Sandboxes.Create(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(s.requests) != 3 || s.requests[0].Header.Get("Idempotency-Key") != s.requests[2].Header.Get("Idempotency-Key") {
		t.Fatalf("want 3 attempts with one key, got %d", len(s.requests))
	}
	busy.Store(1)
	zero := time.Duration(0)
	_, err := c.Sandboxes.Create(context.Background(), &CreateOptions{WaitForCapacity: &zero})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "trial_busy" || !failure.Retryable() {
		t.Fatalf("want trial_busy at once, got %v", err)
	}
}

func TestExecCheckReturnsTheOutput(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		writeJSON(w, 200, map[string]any{"exitCode": 3, "stdout": "out\n", "stderr": "boom\n", "timedOut": false})
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	result, err := sbx.Exec(context.Background(), "exit 3", &ExecOptions{Check: true, Env: map[string]string{"TOKEN": "t"}, Stdin: []byte("in")})
	var failed *CommandError
	if !errors.As(err, &failed) || *failed.ExitCode != 3 || failed.Stderr != "boom\n" || *result.ExitCode != 3 {
		t.Fatalf("got %v", err)
	}
	var inner *Error
	if !errors.As(err, &inner) || inner.Code != "command_failed" {
		t.Fatalf("the inner error is %v", inner)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if sent["command"] != "exit 3" || sent["stdinBase64"] != base64.StdEncoding.EncodeToString([]byte("in")) ||
		s.requests[0].URL.Path != "/v1/sandboxes/sbx-1:exec" {
		t.Fatalf("body %s path %s", s.bodies[0], s.requests[0].URL.Path)
	}
}

func TestAStreamFollowsTheProcessAcrossContinues(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		lines := []string{}
		switch {
		case strings.HasSuffix(r.URL.Path, ":exec"):
			lines = []string{`{"type":"start","processId":"p1"}`, `{"type":"stdout","data":"one\n","offset":0}`, `{"type":"continue","processId":"p1","cursor":4}`}
		case r.URL.Query().Get("cursor") == "4":
			lines = []string{`{"type":"stderr","data":"two\n","offset":4}`, `{"type":"continue","processId":"p1","cursor":8}`}
		default:
			lines = []string{`{"type":"stdout","data":"three\n","offset":8}`, `{"type":"exit","exitCode":0,"state":"exited","timedOut":false,"durationMs":5}`}
		}
		fmt.Fprint(w, strings.Join(lines, "\n")+"\n")
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	var seen []string
	result, err := sbx.Exec(context.Background(), "run", &ExecOptions{OnStdout: func(text string) { seen = append(seen, text) }})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "one\nthree\n" || result.Stderr != "two\n" || *result.ExitCode != 0 || result.ProcessID != "p1" ||
		strings.Join(seen, "") != "one\nthree\n" || result.StdoutTruncated {
		t.Fatalf("got %+v", result)
	}
	if got := s.requests[1].URL.Query(); got.Get("follow") != "true" || got.Get("cursor") != "4" {
		t.Fatalf("follow query %v", got)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if sent["stream"] != true || sent["timeoutMs"] != float64(86_400_000) {
		t.Fatalf("stream body %s", s.bodies[0])
	}
}

func TestLargeFilesUploadInCheckedChunks(t *testing.T) {
	var mu sync.Mutex
	chunks := map[string][]byte{}
	var begin map[string]any
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch {
		case r.URL.Path == "/v1/sandboxes/sbx-1/uploads":
			_ = json.Unmarshal(body, &begin)
			writeJSON(w, 200, map[string]any{"uploadId": "u1", "chunkBytes": 1 << 20})
		case r.URL.Path == "/v1/sandboxes/sbx-1/uploads/u1":
			mu.Lock()
			chunks[r.URL.Query().Get("offset")] = body
			mu.Unlock()
			writeJSON(w, 200, map[string]any{})
		default:
			writeJSON(w, 200, map[string]any{"committed": true})
		}
	})
	data := make([]byte, 3<<20+17)
	for i := range data {
		data[i] = byte(i)
	}
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	if err := sbx.Files.Write(context.Background(), "/workspace/big.bin", data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if begin["sha256"] != hex.EncodeToString(sum[:]) || begin["size"] != float64(len(data)) || len(chunks) != 4 {
		t.Fatalf("begin %v, %d chunks", begin, len(chunks))
	}
	joined := append(append(append(chunks["0"], chunks["1048576"]...), chunks["2097152"]...), chunks["3145728"]...)
	if string(joined) != string(data) {
		t.Fatal("chunks do not reassemble the file")
	}
	if last := s.requests[len(s.requests)-1].URL.Path; last != "/v1/sandboxes/sbx-1/uploads/u1:commit" {
		t.Fatalf("last call %s", last)
	}
}

func TestPagesWalkEveryItem(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.Query().Get("cursor") == "" {
			writeJSON(w, 200, map[string]any{"data": []any{running}, "nextCursor": "c2"})
			return
		}
		second := map[string]any{}
		for k, v := range running {
			second[k] = v
		}
		second["id"] = "sbx-2"
		writeJSON(w, 200, map[string]any{"data": []any{second}, "nextCursor": nil})
	})
	var ids []string
	for sbx, err := range s.client(t).Sandboxes.All(context.Background(), &ListOptions{Labels: map[string]string{"team": "a"}, State: []string{"running"}}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, sbx.ID())
	}
	if strings.Join(ids, ",") != "sbx-1,sbx-2" {
		t.Fatalf("ids %v", ids)
	}
	if q := s.requests[1].URL.Query(); q.Get("label") != "team:a" || q.Get("state") != "running" || q.Get("cursor") != "c2" {
		t.Fatalf("second page query %v", q)
	}
}

func TestARedirectIsRefusedAndACancelledCallSaysSo(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
	})
	_, err := s.client(t, WithMaxRetries(0)).Me(context.Background())
	if !errors.Is(err, ErrConnection) {
		t.Fatalf("want a connection error, got %v", err)
	}
	slow := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = slow.client(t).Me(ctx)
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "timeout" {
		t.Fatalf("want timeout, got %v", err)
	}
}

func TestLifecycleCallsAndPreviews(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if strings.Contains(r.URL.Path, "/previews") {
			writeJSON(w, 200, map[string]any{"id": "pv", "port": 3000, "url": "https://3000-sbx-1.runtimehost.com/", "visibility": "public", "createdAt": "2026-09-23T10:00:00Z"})
			return
		}
		info := map[string]any{}
		for k, v := range running {
			info[k] = v
		}
		info["state"] = map[string]string{":pause": "paused", ":stop": "stopped"}[r.URL.Path[strings.LastIndex(r.URL.Path, ":"):]]
		if info["state"] == "" {
			info["state"] = "running"
		}
		writeJSON(w, 200, info)
	})
	ctx := context.Background()
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	steps := []struct {
		run   func() error
		path  string
		state string
	}{
		{func() error { return sbx.Pause(ctx, nil) }, "/v1/sandboxes/sbx-1:pause", "paused"},
		{func() error { return sbx.Wake(ctx, 20*time.Minute, nil) }, "/v1/sandboxes/sbx-1:wake", "running"},
		{func() error { return sbx.Extend(ctx, 10*time.Minute, nil) }, "/v1/sandboxes/sbx-1:extend", "running"},
		{func() error { return sbx.Stop(ctx, nil) }, "/v1/sandboxes/sbx-1:stop", "stopped"},
	}
	for i, step := range steps {
		if err := step.run(); err != nil {
			t.Fatal(err)
		}
		if s.requests[i].URL.Path != step.path || sbx.State() != step.state {
			t.Fatalf("step %d: %s %s", i, s.requests[i].URL.Path, sbx.State())
		}
	}
	var wake map[string]any
	_ = json.Unmarshal(s.bodies[1], &wake)
	if wake["timeoutSeconds"] != float64(1200) {
		t.Fatalf("wake body %s", s.bodies[1])
	}
	preview, err := sbx.Previews.Create(ctx, 3000, &PreviewOptions{Visibility: "public", TTL: time.Hour})
	if err != nil || preview.URL == "" {
		t.Fatal(err)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[4], &sent)
	if sent["port"] != float64(3000) || sent["visibility"] != "public" || sent["ttlSeconds"] != float64(3600) {
		t.Fatalf("preview body %s", s.bodies[4])
	}
}

func TestExtraFieldsAreMerged(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) { writeJSON(w, 200, running) })
	if _, err := s.client(t).Sandboxes.Create(context.Background(), &CreateOptions{VCPU: 4, Extra: map[string]any{"future": 1}}); err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if sent["future"] != float64(1) || sent["vcpu"] != float64(4) {
		t.Fatalf("body %s", s.bodies[0])
	}
}

func TestIdlePauseCanBeTurnedOffAtCreate(t *testing.T) {
	// A new sandbox pauses after 60 idle seconds unless it says otherwise; an
	// int left 0 is omitted, so "never" rides in Extra, as types.go says.
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) { writeJSON(w, 200, running) })
	if _, err := s.client(t).Sandboxes.Create(context.Background(), &CreateOptions{Extra: map[string]any{"idlePauseSeconds": 0}}); err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if v, ok := sent["idlePauseSeconds"]; !ok || v != float64(0) {
		t.Fatalf("body %s", s.bodies[0])
	}
}

func TestUnpackRefusesEntriesOutsideTheDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "src", "deep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "src", "deep", "a.py"), []byte("print(1)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	archive, err := packDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out")
	if err := unpackArchive(archive, out); err != nil {
		t.Fatal(err)
	}
	if text, _ := os.ReadFile(filepath.Join(out, "src", "deep", "a.py")); string(text) != "print(1)\n" {
		t.Fatal("round trip lost the file")
	}
}
