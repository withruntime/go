package withruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
)

const ciID = "22222222-3333-4444-8555-666666666666"

var ciRepositoryAnswer = map[string]any{"id": ciID, "name": nil, "labels": map[string]string{}, "createdAt": 1, "updatedAt": 2,
	"deletedAt": nil, "error": nil, "repository": "acme/web", "productionBranch": "main", "configPath": "runtime.yml",
	"installation": 7, "githubRepositoryId": nil, "state": "connected"}

var ciCheckAnswer = map[string]any{"id": ciID, "repositoryId": ciID, "repository": "acme/web", "commit": strings.Repeat("a", 40),
	"branch": "main", "event": "pull_request", "pullRequest": 3, "baseCommit": nil, "actor": "octo", "trusted": true, "waiting": false,
	"approvedBy": nil, "approvedAt": nil, "landed": false, "state": "failed", "reason": nil,
	"counts": map[string]any{"passed": 2, "failed": 1}, "createdAt": 1, "updatedAt": 2, "startedAt": 1, "endedAt": 2}

var ciJobAnswer = map[string]any{"id": ciID, "checkId": ciID, "repositoryId": ciID, "name": "test", "part": 1, "parts": 2,
	"needs": []string{"build"}, "command": "bun test", "cacheKey": nil, "state": "failed", "cachedFrom": nil, "exitCode": 1,
	"reason": nil, "sandboxIds": []string{"sbx-1"}, "githubCheckRun": 99, "ms": 1200, "createdAt": 1, "updatedAt": 2,
	"annotations": []map[string]any{{"path": "src/a.go", "startLine": 12, "endLine": 12, "startColumn": 3, "level": "failure",
		"message": "undefined: x", "title": "compile"}},
	"startedAt": 1, "endedAt": 2, "review": ciReviewAnswer}

var ciReviewAnswer = map[string]any{
	"verdict":  "block",
	"summary":  "Moves a table.",
	"findings": []any{map[string]any{"kind": "data-at-risk", "file": "db.sql", "line": 4, "what": "Drops it.", "fix": "Keep it."}},
	"notes":    []any{},
	"agent":    "ci-review",
	"account":  "CLAUDE_TOKEN_1",
	"model":    "opus",
	"session":  "s-1",
	"cut":      false,
}

func ciWith(base map[string]any, extra map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

var ciTestAnswer = map[string]any{"repositoryId": ciID, "file": "a.test.ts", "ms": 40, "quarantinedSince": nil,
	"runs": []any{map[string]any{"at": 5, "check": ciID, "job": ciID, "outcome": "flaky", "ms": nil, "writers": false}}, "updatedAt": 6}

func TestCIRoutesSendWhatTheAPIReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	var method, path, key string
	var received map[string]any
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		method, path, key = r.Method, r.URL.RequestURI(), r.Header.Get("Idempotency-Key")
		received = nil
		if len(body) > 0 {
			_ = json.Unmarshal(body, &received)
		}
		mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/ci/repositories") && r.Method == http.MethodGet:
			writeJSON(w, 200, map[string]any{"data": []any{ciRepositoryAnswer}, "nextCursor": nil})
		case strings.HasPrefix(r.URL.Path, "/v1/ci/repositories"):
			writeJSON(w, 200, ciRepositoryAnswer)
		case r.URL.Path == "/v1/ci/checks":
			writeJSON(w, 200, map[string]any{"data": []any{ciCheckAnswer}, "nextCursor": nil})
		case strings.HasPrefix(r.URL.Path, "/v1/ci/checks/"):
			writeJSON(w, 200, ciWith(ciCheckAnswer, map[string]any{"jobs": []any{ciJobAnswer}}))
		case r.URL.Path == "/v1/ci/releases":
			writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"id": 7, "commit": "a6049bf5a", "source": "hostd", "host": "web-4", "line": "web-4: 562211980 -> a6049bf5a", "at": 1}}, "nextCursor": nil})
		case r.URL.Path == "/v1/ci/flakes":
			writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"id": ciID, "file": "a.test.ts", "state": "quarantined", "owner": map[string]any{"login": "octo", "pullRequest": 7}}}, "nextCursor": nil})
		case strings.HasPrefix(r.URL.Path, "/v1/ci/flakes/"):
			writeJSON(w, 200, map[string]any{"id": ciID, "runs": []any{map[string]any{"passed": false, "attempt": "first", "main": true}}, "failures": []any{}})
		case strings.HasPrefix(r.URL.Path, "/v1/ci/jobs/"):
			writeJSON(w, 200, ciWith(ciJobAnswer, map[string]any{"log": "boom\n"}))
		default:
			writeJSON(w, 200, map[string]any{"data": []any{ciTestAnswer}, "nextCursor": nil})
		}
	})
	c := s.client(t)
	yes := true
	cases := []struct {
		name, method, path string
		run                func() error
		body               map[string]any
		key                string // "auto", "none" or the exact key
	}{
		{"connect", "POST", "/v1/ci/repositories", func() error {
			r, err := c.CI.Repositories.Connect(ctx, "acme/web", &ConnectCIRepositoryOptions{ProductionBranch: "trunk", ConfigPath: "ci.yml", Installation: 7, Name: "web", Labels: map[string]string{"team": "a"}})
			if err == nil && (r.ID != ciID || r.Installation != 7 || r.State != "connected" || r.GithubRepositoryID != nil) {
				t.Fatalf("repository %+v", r)
			}
			return err
		}, map[string]any{"repository": "acme/web", "productionBranch": "trunk", "configPath": "ci.yml", "installation": float64(7), "name": "web", "labels": map[string]any{"team": "a"}}, "auto"},
		{"connect sends only what is given", "POST", "/v1/ci/repositories", func() error {
			_, err := c.CI.Repositories.Connect(ctx, "acme/web", &ConnectCIRepositoryOptions{IdempotencyKey: "connect-1"})
			return err
		}, map[string]any{"repository": "acme/web"}, "connect-1"},
		{"connect without options", "POST", "/v1/ci/repositories", func() error {
			_, err := c.CI.Repositories.Connect(ctx, "acme/web", nil)
			return err
		}, map[string]any{"repository": "acme/web"}, "auto"},
		{"repositories list", "GET", "/v1/ci/repositories?label=team%3Aa&limit=5&name=web&state=connected&state=disconnected", func() error {
			page, err := c.CI.Repositories.List(ctx, &CIRepositoryListOptions{Name: "web", Label: []string{"team:a"}, State: []string{"connected", "disconnected"}, Limit: 5})
			if err == nil && (len(page.Data) != 1 || page.Data[0].Repository != "acme/web") {
				t.Fatalf("page %+v", page)
			}
			return err
		}, nil, "none"},
		{"set undo", "POST", "/v1/ci/repositories/" + ciID + ":update", func() error {
			_, err := c.CI.Repositories.SetUndo(ctx, ciID, true)
			return err
		}, map[string]any{"undoCulprits": true}, "auto"},
		{"set undo off", "POST", "/v1/ci/repositories/" + ciID + ":update", func() error {
			_, err := c.CI.Repositories.SetUndo(ctx, ciID, false)
			return err
		}, map[string]any{"undoCulprits": false}, "auto"},
		{"disconnect", "DELETE", "/v1/ci/repositories/" + ciID, func() error {
			_, err := c.CI.Repositories.Disconnect(ctx, ciID)
			return err
		}, nil, "auto"},
		{"checks list", "GET", "/v1/ci/checks", func() error {
			page, err := c.CI.Checks.List(ctx, nil)
			if err == nil {
				check := page.Data[0]
				if check.Event != "pull_request" || *check.PullRequest != 3 || check.Counts["failed"] != 1 || !check.Trusted || *check.EndedAt != 2 || check.BaseCommit != nil {
					t.Fatalf("check %+v", check)
				}
			}
			return err
		}, nil, "none"},
		{"checks list filtered", "GET", "/v1/ci/checks?branch=main&commit=" + strings.Repeat("a", 40) + "&cursor=c1&event=merge_group&limit=10&pullRequest=3&repositoryId=" + ciID + "&state=failed&state=running", func() error {
			_, err := c.CI.Checks.List(ctx, &CICheckListOptions{RepositoryID: ciID, State: []string{"failed", "running"}, Event: []string{"merge_group"}, Branch: "main", Commit: strings.Repeat("a", 40), PullRequest: 3, Limit: 10, Cursor: "c1"})
			return err
		}, nil, "none"},
		{"check get", "GET", "/v1/ci/checks/" + ciID, func() error {
			check, err := c.CI.Checks.Get(ctx, ciID)
			if err == nil && (check.ID != ciID || len(check.Jobs) != 1 || check.Jobs[0].Name != "test" || check.Jobs[0].SandboxIDs[0] != "sbx-1" ||
				*check.Jobs[0].ExitCode != 1 || *check.Jobs[0].GithubCheckRun != 99 ||
				check.Jobs[0].Annotations[0].Path != "src/a.go" || *check.Jobs[0].Annotations[0].StartColumn != 3 ||
				check.Jobs[0].Annotations[0].Level != "failure" ||
				check.Jobs[0].Review.Findings[0].Line != 4 || check.Jobs[0].Review.Account != "CLAUDE_TOKEN_1") {
				t.Fatalf("check %+v", check)
			}
			return err
		}, nil, "none"},
		{"job get", "GET", "/v1/ci/jobs/" + ciID, func() error {
			job, err := c.CI.Jobs.Get(ctx, ciID)
			if err == nil && (job.Log != "boom\n" || job.State != "failed" || job.Needs[0] != "build") {
				t.Fatalf("job %+v", job)
			}
			return err
		}, nil, "none"},
		{"tests list", "GET", "/v1/ci/tests?repositoryId=" + ciID, func() error {
			page, err := c.CI.Tests.List(ctx, ciID, nil)
			if err == nil && (page.Data[0].File != "a.test.ts" || page.Data[0].Runs[0].Outcome != "flaky" || page.Data[0].Runs[0].Writers || page.Data[0].Runs[0].Ms != nil) {
				t.Fatalf("test %+v", page.Data[0])
			}
			return err
		}, nil, "none"},
		{"flakes list", "GET", "/v1/ci/flakes?repositoryId=" + ciID + "&state=quarantined&state=gating", func() error {
			page, err := c.CI.Flakes.List(ctx, &CIFlakeListOptions{RepositoryID: ciID, State: []string{"quarantined", "gating"}})
			if err == nil && (page.Data[0].State != "quarantined" || page.Data[0].Owner.Login != "octo" || *page.Data[0].Owner.PullRequest != 7) {
				t.Fatalf("flake %+v", page.Data[0])
			}
			return err
		}, nil, "none"},
		{"flake get", "GET", "/v1/ci/flakes/" + ciID, func() error {
			flake, err := c.CI.Flakes.Get(ctx, ciID)
			if err == nil && (flake.Runs[0].Passed || flake.Runs[0].Attempt != "first" || !flake.Runs[0].Main) {
				t.Fatalf("flake %+v", flake)
			}
			return err
		}, nil, "none"},
		{"flake update", "POST", "/v1/ci/flakes/" + ciID + ":update", func() error {
			_, err := c.CI.Flakes.Update(ctx, ciID, CIFlakeUpdate{Owner: "acme/testers", Prove: true})
			return err
		}, map[string]any{"prove": true, "owner": "acme/testers"}, "auto"},
		{"release steps list", "GET", "/v1/ci/releases?commit=a6049bf5a&repositoryId=" + ciID, func() error {
			page, err := c.CI.Releases.List(ctx, &CIReleaseListOptions{RepositoryID: ciID, Commit: "a6049bf5a"})
			if err == nil && (page.Data[0].ID != 7 || *page.Data[0].Host != "web-4" || page.Data[0].Source != "hostd") {
				t.Fatalf("step %+v", page.Data[0])
			}
			return err
		}, nil, "none"},
		{"tests list filtered", "GET", "/v1/ci/tests?cursor=ZmlsZQ&limit=20&quarantined=true&repositoryId=" + ciID, func() error {
			_, err := c.CI.Tests.List(ctx, ciID, &CITestListOptions{Quarantined: &yes, Limit: 20, Cursor: "ZmlsZQ"})
			return err
		}, nil, "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if method != tc.method || path != tc.path {
				t.Fatalf("got %s %s want %s %s", method, path, tc.method, tc.path)
			}
			if tc.body != nil {
				a, _ := json.Marshal(received)
				b, _ := json.Marshal(tc.body)
				if !bytes.Equal(a, b) {
					t.Fatalf("body %s want %s", a, b)
				}
			}
			switch tc.key {
			case "auto":
				if key == "" {
					t.Fatal("no idempotency key")
				}
			case "none":
				if key != "" {
					t.Fatalf("a read sent key %q", key)
				}
			default:
				if key != tc.key {
					t.Fatalf("key %q want %q", key, tc.key)
				}
			}
		})
	}
}

func TestCIListsFollowNextCursor(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		row := any(ciCheckAnswer)
		switch r.URL.Path {
		case "/v1/ci/repositories":
			row = ciRepositoryAnswer
		case "/v1/ci/tests":
			row = ciTestAnswer
		}
		if r.URL.Query().Get("cursor") == "" {
			writeJSON(w, 200, map[string]any{"data": []any{row, row}, "nextCursor": "c2"})
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{row}, "nextCursor": nil})
	})
	c := s.client(t)
	ctx := context.Background()
	checks, repositories, tests := 0, 0, 0
	for check, err := range c.CI.Checks.All(ctx, &CICheckListOptions{Branch: "main", Limit: 2}) {
		if err != nil || check.ID != ciID {
			t.Fatal(check, err)
		}
		checks++
	}
	for repository, err := range c.CI.Repositories.All(ctx, nil) {
		if err != nil || repository.ID != ciID {
			t.Fatal(repository, err)
		}
		repositories++
	}
	for test, err := range c.CI.Tests.All(ctx, ciID, nil) {
		if err != nil || test.File != "a.test.ts" {
			t.Fatal(test, err)
		}
		tests++
	}
	if checks != 3 || repositories != 3 || tests != 3 {
		t.Fatalf("walked %d checks, %d repositories, %d tests", checks, repositories, tests)
	}
	var paths []string
	for _, r := range s.requests {
		paths = append(paths, r.URL.RequestURI())
	}
	want := "/v1/ci/checks?branch=main&limit=2 /v1/ci/checks?branch=main&cursor=c2&limit=2 " +
		"/v1/ci/repositories /v1/ci/repositories?cursor=c2 " +
		"/v1/ci/tests?repositoryId=" + ciID + " /v1/ci/tests?cursor=c2&repositoryId=" + ciID
	if strings.Join(paths, " ") != want {
		t.Fatalf("requests %v", paths)
	}
}

func TestCIErrorsAreTyped(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status int
		code   string
		kind   error
	}{
		{403, "product_in_beta", ErrPermissionDenied},
		{404, "not_found", ErrNotFound},
		{400, "invalid_request", ErrInvalidRequest},
		{401, "unauthorized", ErrAuthentication},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
				apiError(w, tc.status, tc.code, map[string]any{"message": "no such check"})
			})
			c := s.client(t, WithMaxRetries(0))
			_, err := c.CI.Checks.Get(context.Background(), ciID)
			failure, ok := asError(err)
			if !ok || failure.Code != tc.code || failure.Status != tc.status || !errors.Is(err, tc.kind) {
				t.Fatalf("got %#v", err)
			}
			if failure.Message != "no such check" || len(s.requests) != 1 {
				t.Fatalf("message %q after %d requests", failure.Message, len(s.requests))
			}
		})
	}
}
