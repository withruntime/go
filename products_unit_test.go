package withruntime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func sign(secret string, stamp int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", stamp, body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyWebhookAcceptsARotationAndRefusesForgeriesAndReplays(t *testing.T) {
	body := `{"id":"evt_1","type":"sandbox.stopped","createdAt":"2026-09-23T10:00:00Z","data":{"sandbox":{"id":"s"}}}`
	now := time.Unix(1_790_000_000, 0)
	header := fmt.Sprintf("t=%d,v1=%s,v1=%s", now.Unix(), sign("old", now.Unix(), body), sign("new", now.Unix(), body))
	event, err := verifyWebhookAt([]byte(body), header, []string{"new"}, 0, now)
	if err != nil || event.Type != "sandbox.stopped" || event.ID != "evt_1" {
		t.Fatalf("a good delivery was refused: %v %+v", err, event)
	}
	for name, attempt := range map[string]struct {
		body, header string
		at           time.Time
	}{
		"tampered":  {strings.Replace(body, "stopped", "running", 1), header, now},
		"replayed":  {body, header, now.Add(6 * time.Minute)},
		"unsigned":  {body, "", now},
		"malformed": {body, "v1=abc", now},
		"wrong key": {body, fmt.Sprintf("t=%d,v1=%s", now.Unix(), sign("guess", now.Unix(), body)), now},
	} {
		if _, err := verifyWebhookAt([]byte(attempt.body), attempt.header, []string{"new"}, 0, attempt.at); !errors.Is(err, ErrWebhookSignature) {
			t.Errorf("%s: want ErrWebhookSignature, got %v", name, err)
		}
	}
}

func TestDockerignoreFollowsDockersRules(t *testing.T) {
	ignored, err := dockerignoreFilter("# comment\nnode_modules\n**/*.log\n!keep.log\nbuild/\n/secret?.txt\n")
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{
		"node_modules":         true,
		"node_modules/a/b.js":  true,
		"src/app.log":          true,
		"keep.log":             false,
		"build/out.bin":        true,
		"secret1.txt":          true,
		"secret12.txt":         false,
		"src/main.go":          false,
		"src/node_modules.txt": false,
	} {
		if ignored(path) != want {
			t.Errorf("%s: ignored %v, want %v", path, !want, want)
		}
	}
}

func TestAContextUploadsOnlyTheChunksTheServerLacks(t *testing.T) {
	folder := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(folder, ".git"), 0o755))
	must(os.WriteFile(filepath.Join(folder, ".git", "HEAD"), []byte("ref"), 0o644))
	must(os.WriteFile(filepath.Join(folder, "main.py"), []byte("print(1)\n"), 0o644))
	noise := make([]byte, 3<<20)
	random := rand.New(rand.NewPCG(1, 2))
	for i := range noise {
		noise[i] = byte(random.UintN(256))
	}
	must(os.WriteFile(filepath.Join(folder, "data.bin"), noise, 0o600))
	var puts atomic.Int32
	var missing []string
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch {
		case r.URL.Path == "/v1/images/context/missing":
			var asked struct {
				Digests []string `json:"digests"`
			}
			_ = json.Unmarshal(body, &asked)
			missing = asked.Digests[1:]
			writeJSON(w, 200, map[string]any{"missing": missing})
		case strings.HasPrefix(r.URL.Path, "/v1/images/context/"):
			sum := sha256.Sum256(body)
			if r.Method != http.MethodPut || r.Header.Get("Content-Type") != "application/octet-stream" || "/v1/images/context/"+hex.EncodeToString(sum[:]) != r.URL.Path {
				apiError(w, 400, "digest_mismatch", nil)
				return
			}
			puts.Add(1)
			writeJSON(w, 200, map[string]any{"stored": true})
		default:
			apiError(w, 404, "not_found", nil)
		}
	})
	uploaded, dockerignore, err := s.client(t).Images.UploadContext(context.Background(), folder, "")
	if err != nil {
		t.Fatal(err)
	}
	if dockerignore != "" || len(uploaded.Files) != 2 || uploaded.Files[0].Path != "data.bin" || uploaded.Files[0].Mode != 0o600 {
		t.Fatalf("files %+v (the .git folder must be left out)", uploaded.Files)
	}
	if len(uploaded.Archive.Chunks) < 2 || int(puts.Load()) != len(missing) || len(missing) != len(uploaded.Archive.Chunks)-1 {
		t.Fatalf("uploaded %d chunks of %d; the server lacked %d", puts.Load(), len(uploaded.Archive.Chunks), len(missing))
	}
	again, _, err := s.client(t).Images.UploadContext(context.Background(), folder, "")
	if err != nil || again.Archive.SHA256 != uploaded.Archive.SHA256 {
		t.Fatal("the same folder must pack to the same archive, so a rebuild uploads nothing new")
	}
}

func TestABuildStreamsItsLogAndAFailureIsAnError(t *testing.T) {
	image := map[string]any{"id": "22222222-3333-4444-8555-666666666666", "state": "queued", "createdAt": "2026-09-23T10:00:00Z"}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/images":
			var sent map[string]any
			_ = json.Unmarshal(body, &sent)
			if sent["cache"] != false || sent["recipe"] == nil {
				apiError(w, 400, "invalid_request", nil)
				return
			}
			writeJSON(w, 200, image)
		case strings.HasSuffix(r.URL.Path, "/logs") && r.URL.Query().Get("follow") == "true":
			if r.Header.Get("Accept") != "application/x-ndjson" {
				apiError(w, 400, "invalid_request", nil)
				return
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			if r.URL.Query().Get("after") == "0" {
				fmt.Fprintln(w, `{"type":"line","seq":1,"at":"2026-09-23T10:00:01Z","stream":"build","text":"step 1"}`)
				fmt.Fprintln(w, `{"type":"continue","after":1}`)
				return
			}
			fmt.Fprintln(w, `{"type":"line","seq":2,"at":"2026-09-23T10:00:02Z","stream":"build","text":"pip failed"}`)
			failed := map[string]any{"id": image["id"], "state": "failed", "error": "pip exited 1", "createdAt": "2026-09-23T10:00:00Z"}
			done, _ := json.Marshal(map[string]any{"type": "done", "state": "failed", "image": failed})
			fmt.Fprintln(w, string(done))
		default:
			apiError(w, 404, "not_found", nil)
		}
	})
	var lines []string
	_, err := s.client(t).Images.Build(context.Background(), CreateImageOptions{Recipe: &ImageRecipe{Pip: []string{"nothing"}}, NoCache: true}, func(line ImageLogLine) {
		lines = append(lines, line.Text)
	})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "image_failed" || !strings.Contains(failure.Message, "pip exited 1") {
		t.Fatalf("want image_failed, got %v", err)
	}
	if strings.Join(lines, "|") != "step 1|pip failed" {
		t.Fatalf("lines %v", lines)
	}
}

func TestGetOrCreateUpdateAndKeepAliveSendWhatTheAPIReads(t *testing.T) {
	var extended atomic.Int32
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		info := map[string]any{}
		for k, v := range running {
			info[k] = v
		}
		info["expiresAt"] = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
		if strings.HasSuffix(r.URL.Path, ":extend") {
			extended.Add(1)
		}
		writeJSON(w, 200, info)
	})
	c := s.client(t)
	ctx := context.Background()
	sbx, err := c.Sandboxes.GetOrCreate(ctx, "ci-cache", &CreateOptions{Funding: "trial"})
	if err != nil {
		t.Fatal(err)
	}
	var created map[string]any
	_ = json.Unmarshal(s.bodies[0], &created)
	if created["name"] != "ci-cache" || created["getOrCreate"] != true || created["funding"] != "trial" {
		t.Fatalf("create body %s", s.bodies[0])
	}
	off := false
	if err := sbx.Update(ctx, SandboxSettings{AutoWake: &off, RemoveMaxTotalCost: true}, nil); err != nil {
		t.Fatal(err)
	}
	var updated map[string]any
	_ = json.Unmarshal(s.bodies[1], &updated)
	if value, ok := updated["maxTotalCostMicros"]; !ok || value != nil || updated["autoWake"] != false || len(updated) != 2 {
		t.Fatalf("update body %s", s.bodies[1])
	}
	sbx.KeepAlive(ctx, &KeepAliveOptions{Every: 10 * time.Second, Margin: 10 * time.Minute})
	deadline := time.Now().Add(5 * time.Second)
	for extended.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if err := sbx.Stop(ctx, &LifecycleOptions{NoWait: true}); err != nil {
		t.Fatal(err)
	}
	if extended.Load() != 1 {
		t.Fatalf("keep-alive extended %d times", extended.Load())
	}
	for i, r := range s.requests {
		if strings.HasSuffix(r.URL.Path, ":extend") {
			var sent map[string]any
			_ = json.Unmarshal(s.bodies[i], &sent)
			if seconds, _ := sent["seconds"].(float64); seconds < 470 || seconds > 490 {
				t.Fatalf("extended by %v s; wanted about 8 minutes", sent["seconds"])
			}
		}
	}
}

func TestAnInterpreterCellStreamsItsParts(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, line := range []string{
			`{"k":"start","n":1}`,
			`{"k":"stdout","text":"hi\n"}`,
			`{"k":"result","main":true,"data":{"text/plain":"2"},"refs":{}}`,
			`{"k":"error","name":"ValueError","value":"x","traceback":"tb"}`,
			`{"k":"execution","execution":{"id":"e1","contextId":"python","language":"python","status":"error","stdout":"hi\n","stderr":"","results":[],"error":{"name":"ValueError","value":"x","traceback":"tb"},"durationMs":4}}`,
		} {
			fmt.Fprintln(w, line)
		}
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	var out, errs []string
	execution, err := sbx.Interpreter.Run(context.Background(), "print('hi'); 1+1", &RunOptions{
		OnStdout: func(text string) { out = append(out, text) },
		OnResult: func(result ExecutionResult) { out = append(out, fmt.Sprint(result.Data["text/plain"])) },
		OnError:  func(e ExecutionError) { errs = append(errs, e.Name) },
	})
	if err != nil || execution.Status != "error" || execution.Error.Name != "ValueError" {
		t.Fatalf("%+v %v", execution, err)
	}
	if strings.Join(out, "|") != "hi\n|2" || len(errs) != 1 {
		t.Fatalf("callbacks %v %v", out, errs)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if sent["stream"] != true || sent["code"] == nil || s.requests[0].URL.Path != "/v1/sandboxes/sbx-1/interpreter:run" {
		t.Fatalf("request %s %s", s.requests[0].URL, s.bodies[0])
	}
}

func TestRequestReachesANewerEndpointWithTheSameRules(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if n == 1 {
			apiError(w, 503, "host_unavailable", map[string]any{"retryAfterMs": 1})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	var out map[string]any
	if err := s.client(t).Request(context.Background(), "POST", "/v1/future:thing", nil, &out); err != nil || out["ok"] != true {
		t.Fatalf("%v %v", out, err)
	}
	if s.requests[0].Header.Get("Idempotency-Key") == "" || s.requests[0].Header.Get("Idempotency-Key") != s.requests[1].Header.Get("Idempotency-Key") || string(s.bodies[0]) != "{}" {
		t.Fatal("a write through Request must keep its key across retries and send a JSON body")
	}
}

// Straight after a fork the sandbox is still resuming; a snapshot then waits
// for it to run before it pauses it, or the API refuses it as not paused.
func TestASnapshotWaitsForAResumingSandboxToSettle(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		info := map[string]any{}
		for k, v := range running {
			info[k] = v
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("waitFor") == "":
			info["state"] = "resuming"
		case strings.HasSuffix(r.URL.Path, ":pause"):
			info["state"] = "paused"
		case strings.HasSuffix(r.URL.Path, ":snapshot"):
			writeJSON(w, 200, map[string]any{"id": "snap-1", "state": "ready", "createdAt": "2026-09-23T10:00:00Z"})
			return
		}
		writeJSON(w, 200, info)
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	if _, err := sbx.Snapshot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, r := range s.requests {
		step := r.Method + " " + r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if wait := r.URL.Query().Get("waitFor"); wait != "" {
			step += "?waitFor=" + wait
		}
		steps = append(steps, step)
	}
	want := "GET sbx-1,GET sbx-1?waitFor=running,POST sbx-1:pause,POST sbx-1:snapshot,POST sbx-1:wake"
	if strings.Join(steps, ",") != want {
		t.Fatalf("steps %v, want %s", steps, want)
	}
}
