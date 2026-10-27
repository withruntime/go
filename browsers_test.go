package withruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
)

func browserRunAnswer(state string, extra map[string]any) map[string]any {
	run := map[string]any{"id": "run-1", "state": state, "task": "Find the title", "model": "claude-sonnet-5-5",
		"startUrl": nil, "output": nil, "error": nil, "steps": 0, "url": nil, "sandboxId": nil,
		"labels": map[string]string{}, "createdAt": "2026-10-26T00:00:00Z", "startedAt": nil, "endedAt": nil}
	for k, v := range extra {
		run[k] = v
	}
	return run
}

func TestBrowserRunStartsOneAndFollowsItsStepsUntilItEnds(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []string
	var started map[string]any
	reads := 0
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/browsers/runs":
			_ = json.Unmarshal(body, &started)
			writeJSON(w, 201, browserRunAnswer("queued", nil))
		case r.URL.Path == "/v1/browsers/runs/run-1/steps" && r.URL.Query().Get("after") == "0":
			writeJSON(w, 200, map[string]any{"data": []any{
				map[string]any{"n": 0, "seq": 2, "url": "https://example.com/", "frame": nil},
				map[string]any{"n": 1, "seq": 3, "url": "https://example.com/", "frame": nil},
			}, "after": 3, "ended": false})
		case r.URL.Path == "/v1/browsers/runs/run-1/steps":
			writeJSON(w, 200, map[string]any{"data": []any{}, "after": 3, "ended": true})
		case r.URL.Path == "/v1/browsers/runs/run-1:stop":
			writeJSON(w, 200, browserRunAnswer("stopped", nil))
		case r.URL.Path == "/v1/browsers/runs/run-1":
			reads++
			if reads == 1 {
				writeJSON(w, 200, browserRunAnswer("running", nil))
			} else {
				writeJSON(w, 200, browserRunAnswer("done", map[string]any{"output": "Example Domain", "steps": 1}))
			}
		default:
			writeJSON(w, 200, map[string]any{"data": []any{browserRunAnswer("done", nil)}, "nextCursor": nil})
		}
	})
	c := s.client(t)
	ctx := context.Background()
	run, err := c.Browsers.Run(ctx, BrowserRunOptions{Task: "Find the title", StartURL: "https://example.com", MaxSteps: 5})
	if err != nil {
		t.Fatal(err)
	}
	if run.State != "done" || run.Output == nil || *run.Output != "Example Domain" {
		t.Fatalf("run %+v", run)
	}
	want := map[string]any{"task": "Find the title", "startUrl": "https://example.com", "maxSteps": float64(5)}
	if !reflect.DeepEqual(started, want) {
		t.Fatalf("sent %v, want %v", started, want)
	}
	wantSeen := []string{
		"POST /v1/browsers/runs",
		"GET /v1/browsers/runs/run-1",
		"GET /v1/browsers/runs/run-1/steps?after=0&frames=false&wait=30",
		"GET /v1/browsers/runs/run-1/steps?after=3&frames=false&wait=30",
		"GET /v1/browsers/runs/run-1",
	}
	if !reflect.DeepEqual(seen, wantSeen) {
		t.Fatalf("calls %v, want %v", seen, wantSeen)
	}

	var steps []int
	if err := c.Browsers.Runs.Follow(ctx, "run-1", 0, true, func(step BrowserRunStep) bool {
		steps = append(steps, step.N)
		return true
	}); err != nil || !reflect.DeepEqual(steps, []int{0, 1}) {
		t.Fatalf("steps %v, %v", steps, err)
	}
	if stopped, err := c.Browsers.Runs.Stop(ctx, "run-1"); err != nil || stopped.State != "stopped" {
		t.Fatalf("stop %+v, %v", stopped, err)
	}
	if page, err := c.Browsers.Runs.List(ctx, 5); err != nil || len(page.Data) != 1 {
		t.Fatalf("list %+v, %v", page, err)
	}
}
