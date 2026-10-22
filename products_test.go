package withruntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

/* Every product method against the REAL API router (the fixture). The fixture
   has no database behind the product routes, so most answer with an error;
   what matters is that the router matched the route and accepted the body.
   A 404 with no error code means no such route, and a 400 or 415 (or a 422
   invalid_request) means the
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
	// A 422 other than invalid_request is the route's own answer: a program
	// in the fixture's sandbox that failed (interpreter_failed, archive_failed
	// ...) is 422 since 18 October 2026, and means the body was accepted.
	if failure.Status == 400 || failure.Status == 415 || (failure.Status == 422 && failure.Code == "invalid_request") {
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
		"GET /v1/limits":         func() error { _, err := c.Limits.Get(ctx); return err },
		"GET /v1/account/emails": func() error { _, err := c.Emails.Get(ctx); return err },
		"POST /v1/account/emails:update": func() error {
			_, err := c.Emails.Set(ctx, "own-deletions", false)
			return err
		},
		"GET /v1/referrals":     func() error { _, err := c.Referrals.Get(ctx); return err },
		"GET /v1/usage/compare": func() error { _, err := c.Switching.Compare(ctx, "e2b", 30); return err },
		"GET /v1/switching":     func() error { _, err := c.Switching.Get(ctx); return err },
		"POST /v1/switching":    func() error { _, err := c.Switching.Record(ctx, "e2b"); return err },
		"GET /v1/audit":         func() error { _, err := c.Audit.List(ctx, &AuditListOptions{Action: "key.", Limit: 5}); return err },
		"GET /v1/plans":         func() error { _, err := c.Plans.List(ctx); return err },
		"POST /v1/plans/choose": func() error {
			_, err := c.Plans.Choose(ctx, "pro", &PlanChooseOptions{Interval: "year"})
			return err
		},
		"POST /v1/plans/cancel":         func() error { _, err := c.Plans.Cancel(ctx); return err },
		"POST /v1/plans/resume":         func() error { _, err := c.Plans.Resume(ctx); return err },
		"POST /v1/plans/add-ons":        func() error { _, err := c.Plans.AddOn(ctx, "extra-seats"); return err },
		"DELETE /v1/plans/add-ons/{id}": func() error { _, err := c.Plans.RemoveAddOn(ctx, "extra-seats"); return err },
		"PUT /v1/plans/auto-upgrade":    func() error { _, err := c.Plans.AutoUpgrade(ctx, true); return err },
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
			calls, writes := 500, 0
			_, err := c.Secrets.Set(ctx, "OPENAI_API_KEY", SetSecretOptions{
				Value: "sk-test",
				Hosts: []string{"api.openai.com"},
				Git:   &SecretGit{Push: []string{"refs/heads/task/{label.task}"}},
				API: &SecretAPIPolicy{
					CallsPerHour:  &calls,
					WritesPerHour: &writes,
					GraphQL:       &SecretGraphQL{Mutations: []string{"createPullRequest"}},
				},
			})
			return err
		},
		"PUT /v1/egress-secrets/{name}/sandboxes": func() error {
			_, err := c.Secrets.Scope(ctx, "OPENAI_API_KEY", SecretScope{Labels: []map[string]string{{"role": "hub"}}})
			return err
		},
		"PUT /v1/egress-secrets/{name}/rules": func() error {
			_, err := c.Secrets.Narrow(ctx, "OPENAI_API_KEY", NarrowSecretOptions{Hosts: []string{"api.openai.com"}})
			return err
		},
		"GET /v1/egress-secrets":           func() error { _, err := c.Secrets.List(ctx); return err },
		"DELETE /v1/egress-secrets/{name}": func() error { return c.Secrets.Delete(ctx, "OPENAI_API_KEY") },
		"POST /v1/reservations": func() error {
			start := time.Now().Add(time.Hour)
			_, err := c.Reservations.Create(ctx, ReservationCreate{Sandboxes: 50, StartsAt: start, EndsAt: start.Add(2 * time.Hour), Vcpu: 2, Region: "us-east"})
			return err
		},
		"GET /v1/reservations":         func() error { _, err := c.Reservations.List(ctx, ListReservations{Limit: 5}); return err },
		"GET /v1/reservations/{id}":    func() error { _, err := c.Reservations.Get(ctx, fixtureID); return err },
		"DELETE /v1/reservations/{id}": func() error { _, err := c.Reservations.Cancel(ctx, fixtureID); return err },
		"POST /v1/reservations/{id}:update": func() error {
			_, err := c.Reservations.Update(ctx, fixtureID, ReservationUpdate{Sandboxes: 4})
			return err
		},
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
		"POST /v1/sandboxes/{id}/files:link": func() error {
			if _, err := sbx.Files.DownloadURL(ctx, "/workspace/a.txt", &DownloadURLOptions{Filename: "a.txt"}); err != nil {
				return err
			}
			_, err := sbx.Files.UploadURL(ctx, "/workspace/b.txt", &UploadURLOptions{MaxBytes: 1024})
			return err
		},
		"POST /v1/sandboxes/{id}/files:revoke-links": func() error {
			_, err := sbx.Files.RevokeLinks(ctx)
			return err
		},
		"POST /v1/sandboxes/{id}:pause":   func() error { return sbx.Pause(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:wake":    func() error { return sbx.Wake(ctx, time.Minute, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:restart": func() error { return sbx.Restart(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/sandboxes/{id}:stop":    func() error { return sbx.Stop(ctx, &LifecycleOptions{NoWait: true}) },
		"POST /v1/support/messages": func() error {
			_, err := c.Support.Message(ctx, SupportMessage{Message: "Go SDK fixture"})
			return err
		},
		"GET /v1/sandboxes/{id}/previews":                func() error { _, err := sbx.Previews.List(ctx); return err },
		"POST /v1/sandboxes/{id}/previews/{port}:rotate": func() error { _, err := sbx.Previews.Rotate(ctx, 3000); return err },
		"DELETE /v1/sandboxes/{id}/previews/{port}":      func() error { return sbx.Previews.Delete(ctx, 3000) },
		"POST /v1/apps": func() error {
			_, err := c.Apps.Create(ctx, CreateAppOptions{Name: "go-fixture", MaxCopies: 1, VCPU: 1, MemoryMiB: 512})
			return err
		},
		"GET /v1/apps": func() error { _, err := c.Apps.List(ctx, &AppListOptions{State: []string{"live"}}); return err },
		"GET /v1/apps/{id}": func() error {
			_, err := c.Apps.Get(ctx, imageID, &AppGetOptions{WaitFor: "live", TimeoutSeconds: 1})
			return err
		},
		"POST /v1/apps/{id}:update": func() error {
			_, err := c.Apps.Update(ctx, imageID, UpdateAppOptions{Name: &name})
			return err
		},
		"POST /v1/apps/{id}/deploys": func() error {
			_, err := c.Apps.Deploy(ctx, imageID, DeployAppOptions{Image: "web:1", Env: map[string]string{"A": "1"}})
			return err
		},
		"GET /v1/apps/{id}/deploys/{deployId}": func() error { _, err := c.Apps.GetDeploy(ctx, imageID, imageID, nil); return err },
		"POST /v1/apps/{id}/rollback":          func() error { _, err := c.Apps.Rollback(ctx, imageID, nil); return err },
		"GET /v1/apps/{id}/versions":           func() error { _, err := c.Apps.Versions(ctx, imageID); return err },
		"GET /v1/apps/{id}/copies":             func() error { _, err := c.Apps.Copies(ctx, imageID); return err },
		"GET /v1/apps/{id}/logs": func() error {
			_, err := c.Apps.Logs(ctx, imageID, &AppLogsOptions{Since: "2h", Version: 1, Stream: "stderr", Limit: 50})
			return err
		},
		"DELETE /v1/apps/{id}": func() error { _, err := c.Apps.Delete(ctx, imageID, nil); return err },
		"POST /v1/functions": func() error {
			_, err := c.Functions.Create(ctx, CreateAppOptions{MaxCopies: 1, VCPU: 1, MemoryMiB: 256})
			return err
		},
		"GET /v1/functions": func() error { _, err := c.Functions.List(ctx, nil); return err },
		"POST /v1/builds": func() error {
			_, err := c.Builds.Create(ctx, CreateBuildOptions{RepositoryID: imageID, Ref: "main", Branch: "main"})
			return err
		},
		"GET /v1/builds":              func() error { _, err := c.Builds.List(ctx, &BuildListOptions{State: []string{"failed"}}); return err },
		"GET /v1/builds/{id}":         func() error { _, err := c.Builds.Get(ctx, imageID); return err },
		"POST /v1/builds/{id}:cancel": func() error { _, err := c.Builds.Cancel(ctx, imageID); return err },
		"GET /v1/builds/{id}/logs":    func() error { _, err := c.Builds.Logs(ctx, imageID, nil); return err },
		"POST /v1/repositories": func() error {
			_, err := c.Builds.Repositories.Connect(ctx, ConnectRepositoryOptions{Provider: "github", Repository: "acme/web", Deploy: "app:web", BuildEnv: map[string]string{"NODE_ENV": "production"}})
			return err
		},
		"GET /v1/repositories": func() error { _, err := c.Builds.Repositories.List(ctx, nil); return err },
		"POST /v1/repositories/{id}:update": func() error {
			_, err := c.Builds.Repositories.Update(ctx, imageID, UpdateRepositoryOptions{Previews: &yes, Clear: []string{"buildCommand"}})
			return err
		},
		"DELETE /v1/repositories/{id}":         func() error { _, err := c.Builds.Repositories.Disconnect(ctx, imageID); return err },
		"POST /v1/repositories/github:install": func() error { _, err := c.Builds.Repositories.InstallGithub(ctx); return err },
		"POST /v1/ci/repositories": func() error {
			_, err := c.CI.Repositories.Connect(ctx, "acme/web", &ConnectCIRepositoryOptions{ProductionBranch: "main", ConfigPath: "runtime.yml", Installation: 7})
			return err
		},
		"GET /v1/ci/repositories": func() error {
			_, err := c.CI.Repositories.List(ctx, &CIRepositoryListOptions{State: []string{"connected"}})
			return err
		},
		"DELETE /v1/ci/repositories/{id}": func() error { _, err := c.CI.Repositories.Disconnect(ctx, imageID); return err },
		"GET /v1/ci/checks": func() error {
			_, err := c.CI.Checks.List(ctx, &CICheckListOptions{RepositoryID: imageID, State: []string{"failed"}, Branch: "main", PullRequest: 3})
			return err
		},
		"GET /v1/ci/checks/{id}": func() error { _, err := c.CI.Checks.Get(ctx, imageID); return err },
		"GET /v1/ci/jobs/{id}":   func() error { _, err := c.CI.Jobs.Get(ctx, imageID); return err },
		"GET /v1/ci/tests": func() error {
			quarantined := true
			_, err := c.CI.Tests.List(ctx, imageID, &CITestListOptions{Quarantined: &quarantined})
			return err
		},
		"POST /v1/containers": func() error {
			_, err := c.Containers.Create(ctx, CreateContainerOptions{Image: "nginx:1.27", VCPU: 1, MemoryMiB: 512, Command: []string{"nginx", "-g", "daemon off;"}})
			return err
		},
		"GET /v1/containers": func() error {
			_, err := c.Containers.List(ctx, &ContainerListOptions{State: []string{"running"}})
			return err
		},
		"GET /v1/containers/{id}": func() error {
			_, err := c.Containers.Get(ctx, imageID, &ContainerGetOptions{WaitFor: "running", TimeoutSeconds: 1})
			return err
		},
		"POST /v1/containers/{id}:update": func() error {
			_, err := c.Containers.Update(ctx, imageID, UpdateContainerOptions{Name: &name})
			return err
		},
		"DELETE /v1/containers/{id}": func() error { _, err := c.Containers.Delete(ctx, imageID, nil); return err },
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

func TestSecretsSetSendsGithubAppGitAndAPIAndListReturnsThem(t *testing.T) {
	answer := map[string]any{
		"name": "GITHUB_APP", "hosts": []string{"api.github.com", "github.com"},
		"placeholder": "rtsec_0123456789abcdef0123456789abcdef", "valueBytes": 32,
		"createdAt": "2026-09-23T10:00:00.000Z", "updatedAt": "2026-09-23T10:00:00.000Z",
		"rules": []any{map[string]any{"methods": []string{"GET"}, "paths": []string{"/repos/acme/*"}}},
		"only":  map[string]any{"labels": []any{map[string]any{"role": "hub"}}},
		"githubApp": map[string]any{
			"id": 12345, "installation": 67890, "repositories": []string{"runtime-cloud"},
			"permissions": map[string]any{"contents": "write"},
		},
		"git": map[string]any{"push": []string{}},
		"api": map[string]any{"callsPerHour": 500, "writesPerHour": 0, "copies": true, "fresh": 60,
			"graphql": map[string]any{"path": "/api/graphql", "mutations": []string{"createPullRequest"}}},
		"enforced": true,
	}
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if r.Method == http.MethodGet {
			writeJSON(w, 200, map[string]any{"secrets": []any{answer}})
			return
		}
		writeJSON(w, 200, answer)
	})
	c := s.client(t)
	calls, writes, fresh := 500, 0, 60
	saved, err := c.Secrets.Set(context.Background(), "GITHUB_APP", SetSecretOptions{
		Value: "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----",
		Hosts: []string{"api.github.com", "github.com"},
		Rules: []SecretRule{{Methods: []string{"GET"}, Paths: []string{"/repos/acme/*"}}},
		Only:  &SecretScope{Labels: []map[string]string{{"role": "hub"}}},
		GithubApp: &SecretGithubApp{
			ID: 12345, Installation: 67890, Repositories: []string{"runtime-cloud"},
			Permissions: map[string]string{"contents": "write"},
		},
		Git: &SecretGit{},
		API: &SecretAPIPolicy{
			CallsPerHour: &calls, WritesPerHour: &writes, Copies: true, Fresh: &fresh,
			GraphQL: &SecretGraphQL{Path: "/api/graphql", Mutations: []string{"createPullRequest"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	if err := json.Unmarshal(s.bodies[0], &sent); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"value": "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----",
		"hosts": []any{"api.github.com", "github.com"},
		"rules": []any{map[string]any{"methods": []any{"GET"}, "paths": []any{"/repos/acme/*"}}},
		"only":  map[string]any{"labels": []any{map[string]any{"role": "hub"}}},
		"githubApp": map[string]any{
			"id": float64(12345), "installation": float64(67890), "repositories": []any{"runtime-cloud"},
			"permissions": map[string]any{"contents": "write"},
		},
		// An empty list is sent as [], meaning no push at all, never null.
		"git": map[string]any{"push": []any{}},
		"api": map[string]any{"callsPerHour": float64(500), "writesPerHour": float64(0), "copies": true, "fresh": float64(60),
			"graphql": map[string]any{"path": "/api/graphql", "mutations": []any{"createPullRequest"}}},
	}
	if !reflect.DeepEqual(sent, want) {
		t.Fatalf("sent %v, want %v", sent, want)
	}
	if saved.GithubApp == nil || saved.GithubApp.Installation != 67890 || saved.Git == nil || len(saved.Git.Push) != 0 {
		t.Fatalf("the answer lost its policy: %+v", saved)
	}
	listed, err := c.Secrets.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].API == nil || listed[0].API.WritesPerHour == nil || *listed[0].API.WritesPerHour != 0 ||
		!listed[0].API.Copies || listed[0].API.Fresh == nil || *listed[0].API.Fresh != 60 || listed[0].API.GraphQL.Path != "/api/graphql" ||
		listed[0].Only == nil || listed[0].Rules[0].Paths[0] != "/repos/acme/*" {
		t.Fatalf("list lost its policy: %+v", listed)
	}
	// Nothing set: no policy field is sent at all.
	if _, err := c.Secrets.Set(context.Background(), "K", SetSecretOptions{Value: "v", Hosts: []string{"a.example.com"}}); err != nil {
		t.Fatal(err)
	}
	if string(s.bodies[2]) != `{"value":"v","hosts":["a.example.com"]}`+"\n" && string(s.bodies[2]) != `{"value":"v","hosts":["a.example.com"]}` {
		t.Fatalf("a plain secret sent %s", s.bodies[2])
	}
}
