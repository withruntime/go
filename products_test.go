package withruntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

/* Every product method against the REAL API router (the fixture). The fixture
   has no database behind the product routes, so most answer with an error;
   what matters is that the router matched the route and accepted the body.
   A 404 with no error code means no such route, and a 400 or 415 means the
   router refused what the SDK sent. The last test in fixture_test.go
   (TestFixtureZZOnlyRegisteredRoutesWereCalled) and the check below prove the
   routes were reached. */

// reached fails the test when err says the route does not exist or the body
// broke the route's schema.
func reached(t *testing.T, name string, err error) {
	t.Helper()
	failure, ok := asError(err)
	if !ok {
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return
	}
	if failure.Status == 404 && failure.Code == "request_failed" {
		t.Fatalf("%s: the router has no such route", name)
	}
	if failure.Status == 400 || failure.Status == 415 || failure.Status == 422 {
		t.Fatalf("%s: the router refused the request: %v %v", name, failure, failure.Details)
	}
}

func TestFixtureEveryProductMethodReachesARegisteredRoute(t *testing.T) {
	fixture(t)
	// No retries: the fixture's product routes answer 503 for want of a
	// database, and retrying them proves nothing here.
	c, err := New(WithAPIKey("rk_test"), WithBaseURL(fixtureURL), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sbx := newSandbox(c, SandboxInfo{ID: fixtureID, State: "running"})
	folder := t.TempDir()
	if err := os.WriteFile(filepath.Join(folder, "Dockerfile"), []byte("FROM runtime\nCOPY app.py /app/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "app.py"), []byte("print(1)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	const imageID = "22222222-3333-4444-8555-666666666666"
	yes := true
	name := "renamed"
	calls := map[string]func() error{
		"GET /v1/limits":        func() error { _, err := c.Limits.Get(ctx); return err },
		"GET /v1/referrals":     func() error { _, err := c.Referrals.Get(ctx); return err },
		"GET /v1/usage/compare": func() error { _, err := c.Switching.Compare(ctx, "e2b", 30); return err },
		"GET /v1/switching":     func() error { _, err := c.Switching.Get(ctx); return err },
		"POST /v1/switching":    func() error { _, err := c.Switching.Record(ctx, "e2b"); return err },
		"GET /v1/audit":         func() error { _, err := c.Audit.List(ctx, &AuditListOptions{Action: "key.", Limit: 5}); return err },
		"POST /v1/feedback": func() error {
			_, err := c.Feedback.Submit(ctx, FeedbackOptions{Kind: "praise", Summary: "Go SDK fixture"})
			return err
		},
		"GET /v1/feedback": func() error { _, err := c.Feedback.List(ctx, 5); return err },
		"GET /v1/support/conversations/{conversationId}": func() error { _, err := c.Support.Read(ctx, fixtureID); return err },
		"POST /v1/volumes": func() error {
			_, err := c.Volumes.Create(ctx, CreateVolumeOptions{SizeMiB: 1024, Name: "data"})
			return err
		},
		"GET /v1/volumes":              func() error { _, err := c.Volumes.List(ctx, &VolumeListOptions{State: "ready", Limit: 5}); return err },
		"GET /v1/volumes/{id}":         func() error { _, err := c.Volumes.Get(ctx, fixtureID); return err },
		"POST /v1/volumes/{id}:delete": func() error { _, err := c.Volumes.Delete(ctx, fixtureID); return err },
		"PUT /v1/egress-secrets/{name}": func() error {
			_, err := c.Secrets.Set(ctx, "OPENAI_API_KEY", SetSecretOptions{Value: "sk-test", Hosts: []string{"api.openai.com"}})
			return err
		},
		"GET /v1/egress-secrets":           func() error { _, err := c.Secrets.List(ctx); return err },
		"DELETE /v1/egress-secrets/{name}": func() error { return c.Secrets.Delete(ctx, "OPENAI_API_KEY") },
		"GET /v1/events": func() error {
			_, err := c.Events.List(ctx, &EventListOptions{Type: "sandbox.stopped", Limit: 5})
			return err
		},
		"POST /v1/webhooks": func() error {
			_, err := c.Webhooks.Create(ctx, CreateWebhookOptions{URL: "https://example.com/hooks", Events: []string{"sandbox.stopped"}})
			return err
		},
		"GET /v1/webhooks":      func() error { _, err := c.Webhooks.List(ctx); return err },
		"GET /v1/webhooks/{id}": func() error { _, err := c.Webhooks.Get(ctx, fixtureID); return err },
		"POST /v1/webhooks/{id}:update": func() error {
			_, err := c.Webhooks.Update(ctx, fixtureID, UpdateWebhookOptions{Enabled: &yes})
			return err
		},
		"POST /v1/webhooks/{id}:rotate-secret": func() error {
			day := 24 * time.Hour
			_, err := c.Webhooks.RotateSecret(ctx, fixtureID, &day)
			return err
		},
		"POST /v1/webhooks/{id}:delete":          func() error { return c.Webhooks.Delete(ctx, fixtureID) },
		"POST /v1/webhooks/{id}:test":            func() error { _, err := c.Webhooks.Test(ctx, fixtureID); return err },
		"GET /v1/webhooks/{id}/deliveries":       func() error { _, err := c.Webhooks.Deliveries(ctx, fixtureID, "failed"); return err },
		"POST /v1/webhook-deliveries/{id}:retry": func() error { _, err := c.Webhooks.Retry(ctx, fixtureID); return err },
		"POST /v1/otel-exports": func() error {
			_, err := c.Otel.Create(ctx, OtelOptions{Endpoint: "https://otel.example.com", Signals: []string{"logs"}})
			return err
		},
		"GET /v1/otel-exports":              func() error { _, err := c.Otel.List(ctx); return err },
		"GET /v1/otel-exports/{id}":         func() error { _, err := c.Otel.Get(ctx, fixtureID); return err },
		"POST /v1/otel-exports/{id}:update": func() error { _, err := c.Otel.Update(ctx, fixtureID, OtelOptions{Enabled: &yes}); return err },
		"POST /v1/otel-exports/{id}:flush":  func() error { _, err := c.Otel.Flush(ctx, fixtureID); return err },
		"POST /v1/otel-exports/{id}:delete": func() error { return c.Otel.Delete(ctx, fixtureID) },
		"POST /v1/images": func() error {
			_, err := c.Images.Create(ctx, CreateImageOptions{Name: "app", Recipe: &ImageRecipe{Pip: []string{"requests"}}})
			return err
		},
		"GET /v1/images":                  func() error { _, err := c.Images.List(ctx, &ImageListOptions{Name: "app"}); return err },
		"GET /v1/images/{id}":             func() error { _, err := c.Images.Get(ctx, imageID); return err },
		"GET /v1/images/resolve":          func() error { _, err := c.Images.Resolve(ctx, "app:latest"); return err },
		"GET /v1/images/{id}/logs":        func() error { _, err := c.Images.Logs(ctx, imageID, 0); return err },
		"POST /v1/images/{id}:tag":        func() error { _, err := c.Images.Tag(ctx, imageID, "stable"); return err },
		"POST /v1/images/{id}:untag":      func() error { _, err := c.Images.Untag(ctx, imageID, "stable"); return err },
		"POST /v1/images/{id}:delete":     func() error { _, err := c.Images.Delete(ctx, imageID); return err },
		"POST /v1/images/context/missing": func() error { _, _, err := c.Images.UploadContext(ctx, folder, ""); return err },
		"GET /v1/images/registries":       func() error { _, err := c.Images.Registries.List(ctx); return err },
		"POST /v1/images/registries": func() error {
			_, err := c.Images.Registries.Set(ctx, RegistryCredentials{Registry: "ghcr.io", Username: "me", Password: "token"})
			return err
		},
		"POST /v1/images/registries:delete": func() error { _, err := c.Images.Registries.Delete(ctx, "ghcr.io"); return err },
		"GET /v1/snapshots":                 func() error { _, err := c.Snapshots.List(ctx, &SnapshotListOptions{State: "ready"}); return err },
		"POST /v1/sandboxes/{id}:snapshot": func() error {
			_, err := c.Snapshots.Create(ctx, fixtureID, &SnapshotOptions{Name: "ready"})
			return err
		},
		"POST /v1/sandboxes/{id}:update": func() error { return sbx.Update(ctx, SandboxSettings{Name: &name, AutoWake: &yes}, nil) },
		"GET /v1/sandboxes/{id}/network": func() error { _, err := sbx.Network.Get(ctx); return err },
		"PUT /v1/sandboxes/{id}/network": func() error {
			_, err := sbx.Network.Set(ctx, Network{Internet: true, Allow: []string{"pypi.org"}})
			return err
		},
		"GET /v1/sandboxes/{id}/metrics":              func() error { _, err := sbx.Metrics(ctx, "1h"); return err },
		"POST /v1/sandboxes/{id}/interpreter:run":     func() error { _, err := sbx.Interpreter.Run(ctx, "1 + 1", nil); return err },
		"GET /v1/sandboxes/{id}/interpreter/contexts": func() error { _, err := sbx.Interpreter.Contexts.List(ctx); return err },
		"POST /v1/sandboxes/{id}/interpreter/contexts": func() error {
			_, err := sbx.Interpreter.Contexts.Create(ctx, &ContextOptions{ID: "etl", Language: "python"})
			return err
		},
		"POST /v1/sandboxes/{id}/interpreter/contexts/{context}:restart":   func() error { _, err := sbx.Interpreter.Contexts.Restart(ctx, "etl"); return err },
		"POST /v1/sandboxes/{id}/interpreter/contexts/{context}:interrupt": func() error { _, err := sbx.Interpreter.Contexts.Interrupt(ctx, "etl"); return err },
		"DELETE /v1/sandboxes/{id}/interpreter/contexts/{context}":         func() error { _, err := sbx.Interpreter.Contexts.Remove(ctx, "etl"); return err },
		"POST /v1/sandboxes/{id}/desktop:start":                            func() error { _, err := sbx.Desktop.Start(ctx, 1280, 800); return err },
		"POST /v1/sandboxes/{id}/desktop:act":                              func() error { return sbx.Desktop.Click(ctx, 10, 20, "left") },
		"GET /v1/sandboxes/{id}/desktop/screenshot":                        func() error { _, err := sbx.Desktop.Screenshot(ctx, "jpeg", 80); return err },
		"POST /v1/sandboxes/{id}/desktop:stop":                             func() error { return sbx.Desktop.Stop(ctx) },
		"GET /v1/sandboxes/{id}/interpreter/contexts/{context}/results/{file}": func() error {
			_, err := sbx.Interpreter.Result(ctx, ResultRef{Path: "/workspace/.runtime/interpreter/python/out/r1.png"})
			return err
		},
		"GET /v1/sandboxes/{id}/processes/{processId}": func() error { _, err := sbx.Process(ctx, "p1"); return err },
		"POST /v1/sandboxes/{id}/processes/{processId}:signal": func() error {
			return (&Process{sandbox: sbx, Info: ProcessInfo{ID: "p1"}}).Kill(ctx, "SIGINT")
		},
		"POST /v1/sandboxes/{id}/processes/{processId}:resize": func() error {
			return (&Process{sandbox: sbx, Info: ProcessInfo{ID: "p1"}}).Resize(ctx, 100, 30)
		},
		"POST /v1/sandboxes/{id}/files:mkdir":  func() error { return sbx.Files.Mkdir(ctx, "/workspace/dir", true) },
		"POST /v1/sandboxes/{id}/files:rename": func() error { return sbx.Files.Rename(ctx, "/workspace/a", "/workspace/b", true) },
		"POST /v1/sandboxes/{id}:pause":        func() error { return sbx.Pause(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:wake":         func() error { return sbx.Wake(ctx, time.Minute, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:restart":      func() error { return sbx.Restart(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:stop":         func() error { return sbx.Stop(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/support/messages": func() error {
			_, err := c.Support.Message(ctx, SupportMessage{Message: "Go SDK fixture"})
			return err
		},
		"GET /v1/sandboxes/{id}/previews":                func() error { _, err := sbx.Previews.List(ctx); return err },
		"POST /v1/sandboxes/{id}/previews/{port}:rotate": func() error { _, err := sbx.Previews.Rotate(ctx, 3000); return err },
		"DELETE /v1/sandboxes/{id}/previews/{port}":      func() error { return sbx.Previews.Delete(ctx, 3000) },
	}
	for route, run := range calls {
		reached(t, route, run())
	}
	// The desktop's other actions share one route: their bodies must pass it.
	for name, run := range map[string]func() error{
		"move":   func() error { return sbx.Desktop.Move(ctx, 1, 2) },
		"double": func() error { return sbx.Desktop.DoubleClick(ctx, 1, 2) },
		"down":   func() error { return sbx.Desktop.MouseDown(ctx, "") },
		"up":     func() error { return sbx.Desktop.MouseUp(ctx, "right") },
		"drag":   func() error { return sbx.Desktop.Drag(ctx, 1, 2, 3, 4) },
		"scroll": func() error { return sbx.Desktop.Scroll(ctx, 3, -1) },
		"type":   func() error { return sbx.Desktop.Type(ctx, "hello") },
		"press":  func() error { return sbx.Desktop.Press(ctx, "ctrl+l") },
		"cursor": func() error { _, _, err := sbx.Desktop.Cursor(ctx); return err },
		"list":   func() error { _, err := sbx.Desktop.Windows(ctx); return err },
		"focus":  func() error { return sbx.Desktop.Focus(ctx, "0x1") },
		"open":   func() error { return sbx.Desktop.Open(ctx, "https://example.com") },
		"launch": func() error { return sbx.Desktop.Launch(ctx, "xterm") },
	} {
		reached(t, "desktop "+name, run())
	}
	response, err := http.Get(fixtureURL + "/__routes")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var seen struct {
		Routes []string `json:"routes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&seen); err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, route := range seen.Routes {
		known[route] = true
	}
	for route := range calls {
		if !known[route] {
			t.Errorf("the router never matched %s", route)
		}
	}
}

func TestFixtureAFailedCallIsTyped(t *testing.T) {
	c := fixture(t)
	_, err := c.Volumes.Get(context.Background(), "not-a-uuid")
	var failure *Error
	if !errors.As(err, &failure) || failure.Status != 400 || failure.RequestID == "" {
		t.Fatalf("want a typed 400 for a malformed id, got %v", err)
	}
}
