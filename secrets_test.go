package withruntime

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestASecretIsScopedByOnlyAndScopeAndAnAbsentOnlyIsNotSent(t *testing.T) {
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
