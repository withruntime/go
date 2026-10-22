package withruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppsAndBuildsSendWhatTheAPIReads(t *testing.T) {
	ctx := context.Background()
	var method, path, key string
	var received map[string]any
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		method, path, key = r.Method, r.URL.RequestURI(), r.Header.Get("Idempotency-Key")
		received = nil
		_ = json.Unmarshal(body, &received)
		switch {
		case strings.HasSuffix(r.URL.Path, "/versions"):
			writeJSON(w, 200, []any{map[string]any{"version": 2, "live": true, "deploy": "dep", "build": nil}})
		case strings.HasSuffix(r.URL.Path, "/copies"):
			writeJSON(w, 200, []any{map[string]any{"id": "copy", "version": 2, "region": "us-west", "state": "serving", "since": 1}})
		case strings.Contains(r.URL.Path, "deploys") || strings.HasSuffix(r.URL.Path, "/rollback"):
			writeJSON(w, 200, map[string]any{"id": "dep", "version": 2, "url": "https://a.example", "state": "building"})
		case r.URL.Path == "/v1/repositories/github:install":
			writeJSON(w, 200, map[string]any{"url": "https://github.com/apps/runtime", "expiresAt": "2026-10-18T12:00:00Z"})
		case strings.HasSuffix(r.URL.Path, "/logs") && !strings.HasPrefix(r.URL.Path, "/v1/builds/"):
			writeJSON(w, 200, map[string]any{"lines": []any{
				map[string]any{"at": 5, "stream": "stderr", "copy": "cp", "version": 3, "text": "boom"},
				map[string]any{"at": 6, "stream": "stdout", "copy": nil, "version": nil, "text": "ok"},
			}, "nextCursor": "older", "hasMore": true})
		case strings.HasSuffix(r.URL.Path, "/logs"):
			writeJSON(w, 200, map[string]any{"lines": []any{map[string]any{"cursor": "c1", "at": 5, "step": "build", "text": "ok", "stream": "stdout"}}, "cursor": "c1", "state": "failed", "reason": "exit 1", "failingStep": map[string]any{"name": "build", "exitCode": 1, "lastLines": []string{"boom"}}, "hasMore": false, "nextCursor": nil})
		default:
			writeJSON(w, 200, map[string]any{"id": "res", "state": "live", "data": []any{}, "nextCursor": nil, "liveVersion": 2, "url": "https://web-acme.runtimehost.com/", "timings": map[string]any{"total": 1500}})
		}
	})
	c := s.client(t)
	two, three, off, on, sleep := 2, 3, false, true, 400
	name := "web"
	watchOff := "off"
	cases := []struct {
		name, method, path string
		run                func() error
		body               map[string]any
	}{
		{"app create", "POST", "/v1/apps", func() error {
			a, e := c.Apps.Create(ctx, CreateAppOptions{Name: "web", MaxCopies: 3, VCPU: 1, MemoryMiB: 512, MinCopies: &two, Spares: new(int), CDN: &off, IdempotencyKey: "make-web"})
			if e == nil && (a.LiveVersion == nil || *a.LiveVersion != 2 || a.URL != "https://web-acme.runtimehost.com/" || key != "make-web") {
				t.Fatalf("app or key lost: %+v %q", a, key)
			}
			return e
		}, map[string]any{"name": "web", "maxCopies": float64(3), "vcpu": float64(1), "memoryMiB": float64(512), "minCopies": float64(2), "spares": float64(0), "cdn": false}},
		{"function create", "POST", "/v1/functions", func() error {
			_, e := c.Functions.Create(ctx, CreateAppOptions{MaxCopies: 1, VCPU: 1, MemoryMiB: 256, SleepAfterMs: &sleep})
			return e
		}, map[string]any{"maxCopies": float64(1), "vcpu": float64(1), "memoryMiB": float64(256), "sleepAfterMs": float64(400)}},
		{"app get", "GET", "/v1/apps/res?timeoutSeconds=10&waitFor=live", func() error {
			_, e := c.Apps.Get(ctx, "res", &AppGetOptions{WaitFor: "live", TimeoutSeconds: 10})
			return e
		}, nil},
		{"app list", "GET", "/v1/apps?label=team%3Aweb&label=tier%3A1&limit=5&state=live&state=failed", func() error {
			_, e := c.Apps.List(ctx, &AppListOptions{Label: []string{"team:web", "tier:1"}, State: []string{"live", "failed"}, Limit: 5})
			return e
		}, nil},
		{"app update", "POST", "/v1/apps/res:update", func() error {
			_, e := c.Apps.Update(ctx, "res", UpdateAppOptions{ClearName: true, Labels: map[string]string{}, MaxCopies: &three, ReleaseWatch: &watchOff, CDN: &on})
			return e
		}, map[string]any{"name": nil, "labels": map[string]any{}, "maxCopies": float64(3), "releaseWatch": "off", "cdn": true}},
		{"app deploy", "POST", "/v1/apps/res/deploys", func() error {
			d, e := c.Apps.Deploy(ctx, "res", DeployAppOptions{Build: "bld", Env: map[string]string{"A": "1"}, KeepEnv: true})
			if e == nil && d.State != "building" {
				t.Fatalf("deploy lost: %+v", d)
			}
			return e
		}, map[string]any{"build": "bld", "env": map[string]any{"A": "1"}, "keepEnv": true}},
		{"app get deploy", "GET", "/v1/apps/res/deploys/dep", func() error { _, e := c.Apps.GetDeploy(ctx, "res", "dep", nil); return e }, nil},
		{"app rollback", "POST", "/v1/apps/res/rollback", func() error { _, e := c.Apps.Rollback(ctx, "res", nil); return e }, map[string]any{}},
		{"app versions", "GET", "/v1/apps/res/versions", func() error {
			v, e := c.Apps.Versions(ctx, "res")
			if e == nil && (len(v) != 1 || !v[0].Live || v[0].Build != nil) {
				t.Fatalf("versions lost: %+v", v)
			}
			return e
		}, nil},
		{"app copies", "GET", "/v1/apps/res/copies", func() error {
			cs, e := c.Apps.Copies(ctx, "res")
			if e == nil && (len(cs) != 1 || cs[0].State != "serving") {
				t.Fatalf("copies lost: %+v", cs)
			}
			return e
		}, nil},
		{"app logs", "GET", "/v1/apps/res/logs?copy=cp&cursor=older&limit=50&since=2h&stream=stderr&version=3", func() error {
			p, e := c.Apps.Logs(ctx, "res", &AppLogsOptions{Since: "2h", Version: 3, Copy: "cp", Stream: "stderr", Limit: 50, Cursor: "older"})
			if e == nil && (len(p.Lines) != 2 || *p.Lines[0].Copy != "cp" || *p.Lines[0].Version != 3 || p.Lines[1].Copy != nil ||
				p.Lines[1].Version != nil || p.Lines[1].At != 6 || p.NextCursor == nil || *p.NextCursor != "older" || !p.HasMore) {
				t.Fatalf("log page lost: %+v", p)
			}
			return e
		}, nil},
		{"function logs", "GET", "/v1/functions/fn%2F1/logs", func() error { _, e := c.Functions.Logs(ctx, "fn/1", nil); return e }, nil},
		{"app delete", "DELETE", "/v1/apps/res", func() error { _, e := c.Apps.Delete(ctx, "res", nil); return e }, nil},
		{"build create", "POST", "/v1/builds", func() error {
			_, e := c.Builds.Create(ctx, CreateBuildOptions{RepositoryID: "repo", Ref: "main", Branch: "main"})
			return e
		}, map[string]any{"repositoryId": "repo", "ref": "main", "branch": "main"}},
		{"build get", "GET", "/v1/builds/res", func() error {
			b, e := c.Builds.Get(ctx, "res")
			if e == nil && b.Timings["total"] != 1500 {
				t.Fatalf("timings lost: %+v", b)
			}
			return e
		}, nil},
		{"build list", "GET", "/v1/builds?state=failed", func() error {
			_, e := c.Builds.List(ctx, &BuildListOptions{State: []string{"failed"}})
			return e
		}, nil},
		{"build cancel", "POST", "/v1/builds/res:cancel", func() error { _, e := c.Builds.Cancel(ctx, "res"); return e }, map[string]any{}},
		{"build logs", "GET", "/v1/builds/res/logs?pageCursor=older", func() error {
			l, e := c.Builds.Logs(ctx, "res", &BuildLogOptions{PageCursor: "older"})
			if e == nil && (len(l.Lines) != 1 || l.FailingStep == nil || *l.FailingStep.ExitCode != 1) {
				t.Fatalf("log lost: %+v", l)
			}
			return e
		}, nil},
		{"repository connect", "POST", "/v1/repositories", func() error {
			_, e := c.Builds.Repositories.Connect(ctx, ConnectRepositoryOptions{Provider: "github", Repository: "acme/web", Deploy: "app:web", Previews: &off, StartCommand: "bun start"})
			return e
		}, map[string]any{"provider": "github", "repository": "acme/web", "deploy": "app:web", "previews": false, "startCommand": "bun start"}},
		{"repository list", "GET", "/v1/repositories?state=connected", func() error {
			_, e := c.Builds.Repositories.List(ctx, &RepositoryListOptions{State: []string{"connected"}})
			return e
		}, nil},
		{"repository update", "POST", "/v1/repositories/res:update", func() error {
			_, e := c.Builds.Repositories.Update(ctx, "res", UpdateRepositoryOptions{Name: &name, BuildEnv: map[string]string{}, Clear: []string{"installCommand"}})
			return e
		}, map[string]any{"name": "web", "buildEnv": map[string]any{}, "installCommand": nil}},
		{"repository disconnect", "DELETE", "/v1/repositories/res", func() error { _, e := c.Builds.Repositories.Disconnect(ctx, "res"); return e }, nil},
		{"github install", "POST", "/v1/repositories/github:install", func() error {
			i, e := c.Builds.Repositories.InstallGithub(ctx)
			if e == nil && (i.URL == "" || i.ExpiresAt.IsZero()) {
				t.Fatalf("install link lost: %+v", i)
			}
			return e
		}, map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err != nil {
				t.Fatal(err)
			}
			if method != tc.method || path != tc.path {
				t.Fatalf("got %s %s want %s %s", method, path, tc.method, tc.path)
			}
			if tc.body != nil {
				want, _ := json.Marshal(tc.body)
				got, _ := json.Marshal(received)
				if string(want) != string(got) {
					t.Fatalf("body %s want %s", got, want)
				}
			}
			if method != "GET" && key == "" {
				t.Fatal("a write went without an idempotency key")
			}
		})
	}
}

func TestRefusalsTheSDKMakesSendNothing(t *testing.T) {
	ctx := context.Background()
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		t.Fatal("sent a request the SDK should have refused")
	})
	c := s.client(t)
	one := 1
	checks := map[string]error{}
	_, checks["function minCopies"] = c.Functions.Create(ctx, CreateAppOptions{MinCopies: &one, MaxCopies: 1, VCPU: 1, MemoryMiB: 256})
	_, checks["function update minCopies"] = c.Functions.Update(ctx, "f", UpdateAppOptions{MinCopies: &one})
	_, checks["deploy neither"] = c.Apps.Deploy(ctx, "a", DeployAppOptions{})
	_, checks["deploy both"] = c.Apps.Deploy(ctx, "a", DeployAppOptions{Image: "i", Build: "b"})
	_, checks["clear port"] = c.Builds.Repositories.Update(ctx, "r", UpdateRepositoryOptions{Clear: []string{"port"}})
	for name, err := range checks {
		if failure, ok := asError(err); !ok || failure.Code != "invalid_request" {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestABetaRefusalIsAPermissionError(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		apiError(w, 403, "product_in_beta", nil)
	})
	_, err := s.client(t).Apps.List(context.Background(), nil)
	failure, ok := asError(err)
	if !ok || failure.Code != "product_in_beta" || !errors.Is(err, ErrPermissionDenied) || failure.Retryable() {
		t.Fatalf("got %v", err)
	}
	if len(s.requests) != 1 {
		t.Fatalf("a beta refusal was retried: %d requests", len(s.requests))
	}
}

func TestAppListsPageWithTheSameFilters(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.URL.Query().Get("state") != "live" {
			t.Errorf("filter lost on page %d: %s", n, r.URL.RawQuery)
		}
		if n == 1 {
			writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"id": "a1"}}, "nextCursor": "next"})
			return
		}
		if r.URL.Query().Get("cursor") != "next" {
			t.Errorf("cursor lost: %s", r.URL.RawQuery)
		}
		writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"id": "a2"}}, "nextCursor": nil})
	})
	var ids []string
	for app, err := range s.client(t).Apps.All(context.Background(), &AppListOptions{State: []string{"live"}}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, app.ID)
	}
	if strings.Join(ids, ",") != "a1,a2" {
		t.Fatalf("got %v", ids)
	}
}

func TestADeployWaitFollowsItsRecordUnderOneKey(t *testing.T) {
	states := []string{"building", "warming", "live"}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if n == 2 {
			// A lost answer: the write is sent again with its key.
			if r.Method != "POST" {
				t.Errorf("request 2 is %s", r.Method)
			}
		}
		if r.Method == "GET" && (r.URL.Query().Get("waitFor") != "live" || r.URL.Query().Get("timeoutSeconds") != "60") {
			t.Errorf("deploy read without a server wait: %s", r.URL.RawQuery)
		}
		if n == 1 {
			apiError(w, 503, "busy", nil)
			return
		}
		writeJSON(w, 200, map[string]any{"id": "dep", "version": 3, "url": "https://a.example", "state": states[min(n-2, 2)]})
	})
	deploy, err := s.client(t).Apps.Deploy(context.Background(), "app", DeployAppOptions{Image: "web:1", Wait: true, WaitOptions: WaitOptions{Poll: time.Millisecond}})
	if err != nil || deploy.State != "live" || deploy.Version != 3 {
		t.Fatalf("got %+v %v", deploy, err)
	}
	if len(s.requests) != 4 {
		t.Fatalf("%d requests", len(s.requests))
	}
	first, second := s.requests[0].Header.Get("Idempotency-Key"), s.requests[1].Header.Get("Idempotency-Key")
	if first == "" || first != second {
		t.Fatalf("the write's retry changed its key: %q %q", first, second)
	}
}

func TestAFailedDeployIsAnAnswerAndAFailedAppAnError(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.Contains(r.URL.Path, "deploys") {
			writeJSON(w, 200, map[string]any{"id": "dep", "version": 1, "url": "u", "state": "failed",
				"error": map[string]any{"code": "deploy_health_failed", "message": "m", "hint": "h", "lastOutput": "panic"}})
			return
		}
		writeJSON(w, 200, map[string]any{"id": "app", "state": "failed", "error": map[string]any{"code": "image_missing", "message": "No such image."}})
	})
	c := s.client(t)
	deploy, err := c.Apps.WaitDeploy(context.Background(), "app", "dep", nil)
	if err != nil || deploy.Error == nil || deploy.Error.LastOutput != "panic" {
		t.Fatalf("got %+v %v", deploy, err)
	}
	_, err = c.Apps.Wait(context.Background(), "app", "live", nil)
	failure, ok := asError(err)
	if !ok || failure.Code != "image_missing" || failure.Details["state"] != "failed" {
		t.Fatalf("got %v", err)
	}
}

func TestADeleteWaitKeepsItsKeyThroughATimeout(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, 200, map[string]any{"id": "app", "state": "deleting"})
	})
	_, err := s.client(t).Apps.Delete(context.Background(), "app", &DeleteAppOptions{Wait: true, WaitOptions: WaitOptions{Timeout: 50 * time.Millisecond, Poll: 5 * time.Millisecond}})
	failure, ok := asError(err)
	if !ok || failure.Code != "timeout" || failure.IdempotencyKey == "" {
		t.Fatalf("got %v", err)
	}
	if got := s.requests[0].Header.Get("Idempotency-Key"); got != failure.IdempotencyKey {
		t.Fatalf("the timeout names %q, the delete sent %q", failure.IdempotencyKey, got)
	}
}

// ndjson writes events, one JSON line each.
func ndjson(w http.ResponseWriter, lines ...any) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, line := range lines {
		encoded, _ := json.Marshal(line)
		fmt.Fprintf(w, "%s\n", encoded)
	}
}

func TestABuildLogFollowResumesFromItsCursor(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		mu.Lock()
		cursors = append(cursors, r.URL.Query().Get("cursor"))
		mu.Unlock()
		if r.URL.Query().Get("follow") != "true" {
			t.Errorf("not a follow: %s", r.URL.RawQuery)
		}
		switch n {
		case 1:
			// The connection drops after one line.
			ndjson(w, map[string]any{"type": "line", "cursor": "c1", "at": 1, "step": "install", "text": "one", "stream": "stdout"})
		case 2:
			// Resent from c1: the repeat is dropped, then the server asks to continue.
			ndjson(w,
				map[string]any{"type": "line", "cursor": "c1", "at": 1, "step": "install", "text": "one", "stream": "stdout"},
				map[string]any{"type": "line", "cursor": "c2", "at": 2, "step": nil, "text": "two", "stream": "stderr"},
				map[string]any{"type": "continue", "cursor": "c2"})
		default:
			ndjson(w, map[string]any{"type": "terminal", "cursor": "c2", "state": "failed", "reason": "exit 2",
				"failingStep": map[string]any{"name": "build", "exitCode": 2, "lastLines": []string{"two"}}})
		}
	})
	var got []string
	for event, err := range s.client(t).Builds.FollowLogs(context.Background(), "bld", nil) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, event.Type+":"+event.Text+event.State)
		if event.Type == "terminal" && (event.FailingStep == nil || *event.FailingStep.ExitCode != 2) {
			t.Fatalf("failing step lost: %+v", event)
		}
	}
	if strings.Join(got, ",") != "line:one,line:two,terminal:failed" {
		t.Fatalf("events %v", got)
	}
	if strings.Join(cursors, ",") != ",c1,c2" {
		t.Fatalf("resumed from %v", cursors)
	}
}

func TestABuildLogFollowFailsWithWhereToResume(t *testing.T) {
	cases := map[string]struct {
		lines []any
		code  string
	}{
		"server error": {[]any{map[string]any{"type": "line", "cursor": "c1", "at": 1, "step": nil, "text": "a", "stream": "stdout"},
			map[string]any{"type": "error", "cursor": "c1", "code": "build_log_expired", "hint": "Read the page."}}, "build_log_expired"},
		"bad stream": {[]any{map[string]any{"type": "line", "cursor": "c1", "at": 1, "step": nil, "text": "a", "stream": "stdin"}}, "build_log_invalid"},
		"skipped continue": {[]any{map[string]any{"type": "line", "cursor": "c1", "at": 1, "step": nil, "text": "a", "stream": "stdout"},
			map[string]any{"type": "continue", "cursor": "c9"}}, "build_log_gap"},
		"never ends": {nil, "build_log_interrupted"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) { ndjson(w, tc.lines...) })
			var err error
			for _, e := range s.client(t).Builds.FollowLogs(context.Background(), "bld", nil) {
				err = e
			}
			failure, ok := asError(err)
			if !ok || failure.Code != tc.code {
				t.Fatalf("got %v", err)
			}
			if _, ok := failure.Details["cursor"]; !ok {
				t.Fatal("no cursor to resume from")
			}
		})
	}
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) { apiError(w, 404, "not_found", nil) })
	for _, err := range s.client(t).Builds.FollowLogs(context.Background(), "bld", &FollowBuildLogOptions{Cursor: "c1"}) {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("a refusal was not returned as it is: %v", err)
		}
	}
	if len(s.requests) != 1 {
		t.Fatalf("a refusal was reconnected: %d", len(s.requests))
	}
}

func TestAnAppLogFollowResumesFromItsCursor(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		query := r.URL.Query()
		mu.Lock()
		cursors = append(cursors, query.Get("cursor"))
		mu.Unlock()
		if r.URL.Path != "/v1/apps/web/logs" || query.Get("follow") != "true" || query.Get("stream") != "stderr" || query.Get("version") != "2" {
			t.Errorf("request %d lost the follow or its filters: %s %s", n, r.URL.Path, r.URL.RawQuery)
		}
		switch n {
		case 1:
			// The connection drops after one line.
			ndjson(w, map[string]any{"type": "line", "cursor": "c1", "at": 1, "stream": "stderr", "copy": "cp", "version": 2, "text": "one"})
		case 2:
			// Resent from c1: the repeat is dropped; lines were lost, then
			// the server ends this follow.
			ndjson(w,
				map[string]any{"type": "line", "cursor": "c1", "at": 1, "stream": "stderr", "copy": "cp", "version": 2, "text": "one"},
				map[string]any{"type": "line", "cursor": "c2", "at": 2, "stream": "stderr", "copy": nil, "version": nil, "text": "two"},
				map[string]any{"type": "gap", "cursor": "c3"},
				map[string]any{"type": "continue", "cursor": "c3"})
		case 3:
			ndjson(w,
				map[string]any{"type": "line", "cursor": "c4", "at": 4, "stream": "stderr", "copy": "cp", "version": 2, "text": "three"},
				map[string]any{"type": "error", "cursor": "c4", "code": "app_log_unavailable", "hint": "Retry in a minute."})
		default:
			t.Errorf("followed past an error: request %d", n)
		}
	})
	var got []string
	var failure error
	for event, err := range s.client(t).Apps.FollowLogs(context.Background(), "web", &AppLogsOptions{Stream: "stderr", Version: 2}) {
		if err != nil {
			failure = err
			continue
		}
		got = append(got, event.Type+":"+event.Text)
		if event.Text == "one" && (event.Copy == nil || *event.Copy != "cp" || event.Version == nil || *event.Version != 2) {
			t.Fatalf("copy or version lost: %+v", event)
		}
	}
	if strings.Join(got, ",") != "line:one,line:two,gap:,line:three" {
		t.Fatalf("events %v", got)
	}
	if strings.Join(cursors, ",") != ",c1,c3" {
		t.Fatalf("resumed from %v", cursors)
	}
	apiFailure, ok := asError(failure)
	if !ok || apiFailure.Code != "app_log_unavailable" || apiFailure.Hint != "Retry in a minute." || apiFailure.Details["cursor"] != "c4" {
		t.Fatalf("got %v", failure)
	}
}

func TestAnAppLogFollowFailsWithWhereToResume(t *testing.T) {
	cases := map[string]struct {
		lines []any
		code  string
	}{
		"bad stream": {[]any{map[string]any{"type": "line", "cursor": "c1", "at": 1, "text": "a", "stream": "stdin"}}, "app_log_invalid"},
		"no hint":    {[]any{map[string]any{"type": "error", "cursor": nil, "code": "x"}}, "app_log_invalid"},
		"never ends": {nil, "app_log_interrupted"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) { ndjson(w, tc.lines...) })
			var err error
			for _, e := range s.client(t).Functions.FollowLogs(context.Background(), "fn", nil) {
				err = e
			}
			failure, ok := asError(err)
			if !ok || failure.Code != tc.code {
				t.Fatalf("got %v", err)
			}
			if _, ok := failure.Details["cursor"]; !ok {
				t.Fatal("no cursor to resume from")
			}
		})
	}
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) { apiError(w, 404, "not_found", nil) })
	for _, err := range s.client(t).Apps.FollowLogs(context.Background(), "web", &AppLogsOptions{Cursor: "c1"}) {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("a refusal was not returned as it is: %v", err)
		}
	}
	if len(s.requests) != 1 {
		t.Fatalf("a refusal was reconnected: %d", len(s.requests))
	}
}

func TestAnAppLogFollowEndsWhenItsContextIs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.URL.Query().Has("cursor") {
			t.Errorf("a continue with no cursor sent one: %s", r.URL.RawQuery)
		}
		// Nothing printed yet: each follow ends with a null cursor, and
		// the SDK follows again until the caller stops.
		ndjson(w, map[string]any{"type": "continue", "cursor": nil})
		if n == 3 {
			cancel()
		}
	})
	var err error
	for _, e := range s.client(t).Apps.FollowLogs(ctx, "web", nil) {
		err = e
	}
	failure, ok := asError(err)
	if !ok || failure.Code != "timeout" {
		t.Fatalf("got %v", err)
	}
	if len(s.requests) < 3 {
		t.Fatalf("stopped following after %d requests", len(s.requests))
	}
}
