package withruntime

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestASecretIsScopedByOnlyAndScopeAndAnAbsentOnlyIsNotSent(t *testing.T) {
	t.Parallel()
	secret := map[string]any{"name": "SLACK_APP_TOKEN", "hosts": []string{"slack.com"}, "placeholder": "rtsec_x", "valueBytes": 6,
		"only": map[string]any{"labels": []map[string]string{{"role": "hub"}}}, "enforced": true}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) { writeJSON(w, 200, secret) })
	c := s.client(t)
	ctx := context.Background()
	hub := SecretScope{Labels: []map[string]string{{"role": "hub"}}, Sandboxes: []string{"ef01c6ef-6648-450b-acd2-6fe82ac31960"}}
	saved, err := c.Secrets.Set(ctx, "SLACK_APP_TOKEN", SetSecretOptions{Value: "xapp_x", Hosts: []string{"slack.com"}, Only: &hub})
	if err != nil || saved.Only == nil || saved.Only.Labels[0]["role"] != "hub" {
		t.Fatalf("the scope was not read back: %v %+v", err, saved)
	}
	if _, err := c.Secrets.Set(ctx, "SLACK_APP_TOKEN", SetSecretOptions{Value: "xapp_x", Hosts: []string{"slack.com"}, Only: &SecretScope{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Secrets.Set(ctx, "SLACK_APP_TOKEN", SetSecretOptions{Value: "xapp_x", Hosts: []string{"slack.com"}}); err != nil {
		t.Fatal(err)
	}
	scoped, err := c.Secrets.Scope(ctx, "SLACK_APP_TOKEN", SecretScope{Labels: []map[string]string{{"role": "hub"}}})
	if err != nil || !scoped.Enforced {
		t.Fatalf("scope: %v %+v", err, scoped)
	}
	if _, err := c.Secrets.Scope(ctx, "A B", SecretScope{}); err != nil {
		t.Fatal(err)
	}
	want := []struct{ line, body string }{
		{"PUT /v1/egress-secrets/SLACK_APP_TOKEN", `{"value":"xapp_x","hosts":["slack.com"],"only":{"labels":[{"role":"hub"}],"sandboxes":["ef01c6ef-6648-450b-acd2-6fe82ac31960"]}}`},
		{"PUT /v1/egress-secrets/SLACK_APP_TOKEN", `{"value":"xapp_x","hosts":["slack.com"],"only":{}}`},
		{"PUT /v1/egress-secrets/SLACK_APP_TOKEN", `{"value":"xapp_x","hosts":["slack.com"]}`},
		{"PUT /v1/egress-secrets/SLACK_APP_TOKEN/sandboxes", `{"labels":[{"role":"hub"}]}`},
		{"PUT /v1/egress-secrets/A%20B/sandboxes", `{}`},
	}
	if len(s.requests) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(s.requests))
	}
	for i, w := range want {
		if line := s.requests[i].Method + " " + s.requests[i].URL.EscapedPath(); line != w.line || strings.TrimSpace(string(s.bodies[i])) != w.body {
			t.Errorf("request %d: got %s %s, want %s %s", i, line, s.bodies[i], w.line, w.body)
		}
	}
}

func TestASecretEnvIsSentWhenGivenAndReadBack(t *testing.T) {
	t.Parallel()
	secret := map[string]any{"name": "GH_REPO_A", "hosts": []string{"api.github.com"}, "placeholder": "rtsec_x", "valueBytes": 5, "env": "GITHUB_TOKEN"}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) { writeJSON(w, 200, secret) })
	c := s.client(t)
	ctx := context.Background()
	saved, err := c.Secrets.Set(ctx, "GH_REPO_A", SetSecretOptions{Value: "ghp_x", Hosts: []string{"api.github.com"}, Env: "GITHUB_TOKEN"})
	if err != nil || saved.Env != "GITHUB_TOKEN" {
		t.Fatalf("env was not read back: %v %+v", err, saved)
	}
	if _, err := c.Secrets.Set(ctx, "GH_REPO_A", SetSecretOptions{Value: "ghp_x", Hosts: []string{"api.github.com"}}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"value":"ghp_x","hosts":["api.github.com"],"env":"GITHUB_TOKEN"}`,
		`{"value":"ghp_x","hosts":["api.github.com"]}`,
	}
	if len(s.bodies) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(s.bodies))
	}
	for i, w := range want {
		if got := strings.TrimSpace(string(s.bodies[i])); got != w {
			t.Errorf("request %d: got %s, want %s", i, got, w)
		}
	}
}

func TestSecretsListFollowsNextOrKeepsToOnePage(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.Query().Get("after") == "A" {
			writeJSON(w, 200, map[string]any{"secrets": []map[string]any{{"name": "B"}}, "next": nil})
			return
		}
		writeJSON(w, 200, map[string]any{"secrets": []map[string]any{{"name": "A"}}, "next": "A"})
	})
	c := s.client(t)
	ctx := context.Background()
	all, err := c.Secrets.List(ctx)
	if err != nil || len(all) != 2 || all[0].Name != "A" || all[1].Name != "B" {
		t.Fatalf("every page: %v %+v", err, all)
	}
	one, err := c.Secrets.List(ctx, &SecretListOptions{Limit: 1, After: "A"})
	if err != nil || len(one) != 1 || one[0].Name != "B" {
		t.Fatalf("one page: %v %+v", err, one)
	}
	// After alone sends no limit: the API's default page, never limit=0.
	alone, err := c.Secrets.List(ctx, &SecretListOptions{After: "A"})
	if err != nil || len(alone) != 1 || alone[0].Name != "B" {
		t.Fatalf("after alone: %v %+v", err, alone)
	}
	want := []string{"", "after=A", "after=A&limit=1", "after=A"}
	if len(s.requests) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(s.requests))
	}
	for i, query := range want {
		if got := s.requests[i].URL.RawQuery; got != query {
			t.Errorf("request %d: query %q, want %q", i, got, query)
		}
	}
}

func TestAnExposedSecretSendsNoHostsAndExpectedVersionIsSent(t *testing.T) {
	t.Parallel()
	secret := map[string]any{"name": "DEPLOY_KEY", "hosts": []string{}, "exposed": true, "version": 1, "placeholder": "rtsec_x", "valueBytes": 52}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) { writeJSON(w, 200, secret) })
	c := s.client(t)
	ctx := context.Background()
	pem := "-----BEGIN FAKE KEY-----\nAAAA\n-----END FAKE KEY-----\n"
	// A real value goes only to the sandboxes and Apps named by id.
	worker, app := "5d2c8a1e-7b3f-4c6d-9e0a-1f2b3c4d5e6f", "0b6f3c1e-2d4a-4e5f-9a7b-8c9d0e1f2a3b"
	saved, err := c.Secrets.Set(ctx, "DEPLOY_KEY", SetSecretOptions{Value: pem, Exposed: true, Env: "SSH_KEY", Only: &SecretScope{Sandboxes: []string{worker}}})
	if err != nil || !saved.Exposed || saved.Version != 1 || len(saved.Hosts) != 0 {
		t.Fatalf("the kind and version were not read back: %v %+v", err, saved)
	}
	zero, three := 0, 3
	if _, err := c.Secrets.Set(ctx, "DB_PASSWORD", SetSecretOptions{Value: "fake-db-password", Exposed: true, Only: &SecretScope{Apps: []string{app}}, ExpectedVersion: &zero}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Secrets.Set(ctx, "OPENAI_API_KEY", SetSecretOptions{Value: "fake-api-key", Hosts: []string{"api.openai.com"}, ExpectedVersion: &three}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"value":"-----BEGIN FAKE KEY-----\nAAAA\n-----END FAKE KEY-----\n","only":{"sandboxes":["5d2c8a1e-7b3f-4c6d-9e0a-1f2b3c4d5e6f"]},"env":"SSH_KEY","exposed":true}`,
		`{"value":"fake-db-password","only":{"apps":["0b6f3c1e-2d4a-4e5f-9a7b-8c9d0e1f2a3b"]},"exposed":true,"expectedVersion":0}`,
		`{"value":"fake-api-key","hosts":["api.openai.com"],"expectedVersion":3}`,
	}
	if len(s.bodies) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(s.bodies))
	}
	for i, w := range want {
		if got := strings.TrimSpace(string(s.bodies[i])); got != w {
			t.Errorf("request %d: got %s, want %s", i, got, w)
		}
	}
}

func TestSecretsGetReadsOneByName(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.EscapedPath() == "/v1/egress-secrets/MISSING" {
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": "not_found", "message": "No secret named MISSING."}})
			return
		}
		writeJSON(w, 200, map[string]any{"name": "DB_PASSWORD", "hosts": []string{}, "exposed": true, "version": 2, "placeholder": "rtsec_x", "valueBytes": 16})
	})
	c := s.client(t)
	ctx := context.Background()
	found, err := c.Secrets.Get(ctx, "DB_PASSWORD")
	if err != nil || found.Name != "DB_PASSWORD" || !found.Exposed || found.Version != 2 {
		t.Fatalf("get: %v %+v", err, found)
	}
	if _, err := c.Secrets.Get(ctx, "A B"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Secrets.Get(ctx, "MISSING")
	if failure, ok := asError(err); !ok || failure.Status != 404 || failure.Code != "not_found" {
		t.Fatalf("want a 404 not_found, got %v", err)
	}
	want := []string{"GET /v1/egress-secrets/DB_PASSWORD", "GET /v1/egress-secrets/A%20B", "GET /v1/egress-secrets/MISSING"}
	if len(s.requests) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(s.requests))
	}
	for i, w := range want {
		if line := s.requests[i].Method + " " + s.requests[i].URL.EscapedPath(); line != w {
			t.Errorf("request %d: got %s, want %s", i, line, w)
		}
	}
}
