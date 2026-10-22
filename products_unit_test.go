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
	"reflect"
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
		// A sandbox with a time limit, as an E2B-style create makes one.
		end := time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
		info["timeoutSeconds"], info["expiresAt"], info["endsAt"] = 300, end, end
		if strings.HasSuffix(r.URL.Path, ":extend") {
			extended.Add(1)
		}
		writeJSON(w, 200, info)
	})
	c := s.client(t)
	ctx := context.Background()
	sbx, err := c.Sandboxes.GetOrCreate(ctx, "ci-cache", &CreateOptions{Region: "us-east"})
	if err != nil {
		t.Fatal(err)
	}
	var created map[string]any
	_ = json.Unmarshal(s.bodies[0], &created)
	if created["name"] != "ci-cache" || created["getOrCreate"] != true || created["region"] != "us-east" {
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
	// An empty map removes every label; nil leaves them alone.
	if err := sbx.Update(ctx, SandboxSettings{Labels: map[string]string{}}, nil); err != nil {
		t.Fatal(err)
	}
	if string(s.bodies[2]) != `{"labels":{}}` {
		t.Fatalf("clearing labels sent %s", s.bodies[2])
	}
	if err := sbx.Update(ctx, SandboxSettings{AutoWake: &off}, nil); err != nil {
		t.Fatal(err)
	}
	if string(s.bodies[3]) != `{"autoWake":false}` {
		t.Fatalf("leaving labels sent %s", s.bodies[3])
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
// for it to run, then sends only the snapshot call: the server pauses and wakes it.
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
	want := "GET sbx-1,GET sbx-1?waitFor=running,POST sbx-1:snapshot"
	if strings.Join(steps, ",") != want {
		t.Fatalf("steps %v, want %s", steps, want)
	}
}

// 19 Oct 2026: a company deleted sandboxes by id while each one's KeepAlive
// ran on, and every loop read its gone sandbox once a minute until the
// process ended (442 404s an hour on 40 sandboxes). The first 404 ends it.
func TestKeepAliveEndsWhenTheSandboxIsGone(t *testing.T) {
	var reads atomic.Int32
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if n == 1 {
			writeJSON(w, 200, running)
			return
		}
		reads.Add(1)
		writeJSON(w, 404, map[string]any{"error": "Sandbox not found.", "code": "not_found", "retryable": false})
	})
	c := s.client(t)
	ctx := context.Background()
	sbx, err := c.Sandboxes.Create(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var errs atomic.Int32
	sbx.KeepAlive(ctx, &KeepAliveOptions{Every: 10 * time.Second, OnError: func(error) { errs.Add(1) }})
	deadline := time.Now().Add(5 * time.Second)
	for reads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if reads.Load() != 1 || errs.Load() != 0 {
		t.Fatalf("read the gone sandbox %d times, reported %d errors; want 1 read and no error", reads.Load(), errs.Load())
	}
}

// A sandbox with no time limit renews itself (ARCHITECTURE.md section 10, "A
// sandbox runs while it works"): KeepAlive watches it and extends nothing,
// whether the server says so with endsAt null or, older, with timeoutSeconds
// 0; once a renewal is refused, endsAt names its end and KeepAlive extends.
func TestKeepAliveLeavesASandboxWithNoTimeLimitAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]any
		want   bool
	}{
		{"endsAt null", map[string]any{"timeoutSeconds": 0, "endsAt": nil}, false},
		{"persistent", map[string]any{"timeoutSeconds": 1800, "persistent": true}, false},
		{"renewal refused", map[string]any{"timeoutSeconds": 0, "endsAt": "soon"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var extended atomic.Int32
			s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
				info := map[string]any{}
				for k, v := range running {
					info[k] = v
				}
				soon := time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
				info["expiresAt"] = soon
				for k, v := range tc.fields {
					if v == "soon" {
						v = soon
					}
					info[k] = v
				}
				if strings.HasSuffix(r.URL.Path, ":extend") {
					extended.Add(1)
				}
				writeJSON(w, 200, info)
			})
			c := s.client(t)
			ctx := context.Background()
			sbx, err := c.Sandboxes.Create(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			sbx.KeepAlive(ctx, &KeepAliveOptions{Every: 10 * time.Second, Margin: 10 * time.Minute})
			deadline := time.Now().Add(time.Second)
			for extended.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			sbx.StopKeepAlive()
			if got := extended.Load() > 0; got != tc.want {
				t.Fatalf("extended %d times; wanted an extend: %v", extended.Load(), tc.want)
			}
		})
	}
}

// CLOUD.md section 4: the old field names are the renamed services, until a
// release after their week of overlap removes them.
func TestRenamedProductFieldsAreTheSameServices(t *testing.T) {
	c, err := New(WithAPIKey("rk_renamed"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Addresses != c.IPs || c.Tunnel != c.Networks.WireGuard || c.IPs == nil || c.Networks.WireGuard == nil {
		t.Fatal("an old field is not the renamed service")
	}
}

func TestReferralsReadEveryFieldAndNoName(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		writeJSON(w, 200, map[string]any{
			"code": "k7m2q9xd", "link": "https://withruntime.com/r/k7m2q9xd", "signedUp": 2, "paid": 1,
			"payingCompanies": 1, "shareEarnedMicros": "50000000", "shareBasisPoints": 1000,
			"shareMonths": 12, "windowDays": 30,
		})
	})
	c := s.client(t)
	got, err := c.Referrals.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.PayingCompanies != 1 || got.ShareEarnedMicros != "50000000" || got.ShareBasisPoints != 1000 ||
		got.ShareMonths != 12 || got.WindowDays != 30 {
		t.Fatalf("referral fields lost: %+v", got)
	}
	// 10 October 2026: the link's page never names the company, so the
	// summary has no name and there is no switch to set.
	for _, field := range []string{"ShowName", "InviteName"} {
		if _, ok := reflect.TypeOf(ReferralSummary{}).FieldByName(field); ok {
			t.Fatalf("ReferralSummary still has %s", field)
		}
	}
	if _, ok := reflect.TypeOf(c.Referrals).MethodByName("Update"); ok {
		t.Fatal("Referrals still has Update")
	}
}

func TestPlansListAndEveryChange(t *testing.T) {
	var seen []string
	var bodies []string
	var keys []string
	now := map[string]any{
		"planId": "pro", "name": "Pro", "status": "trialing", "interval": "year",
		"periodStart": "2026-10-01T00:00:00.000Z", "periodEnd": "2026-11-01T00:00:00.000Z",
		"trialEndsAt": "2026-10-15T00:00:00.000Z", "cancelAtPeriodEnd": true, "nextPlanId": "team",
		"startedAt":   "2026-10-01T00:00:00.000Z",
		"addOns":      []any{map[string]any{"id": "extra-seats", "quantity": 2, "endsAt": nil}},
		"autoUpgrade": true,
		"planUsage":   map[string]any{"grantedMicros": "200000000", "leftMicros": "150000000", "expiresAt": "2026-11-01T00:00:00.000Z"},
	}
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		bodies = append(bodies, strings.TrimSpace(string(body)))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		switch r.URL.Path {
		case "/v1/plans":
			writeJSON(w, 200, map[string]any{"current": now, "plans": []any{map[string]any{
				"id": "pro", "name": "Pro", "card": "pro", "feeMicros": "100000000", "yearlyFeeMicros": "1000000000",
				"bonusMicros": "10000000", "trialDays": 14, "firstPeriodDouble": true, "support": "agent",
				"collection": "card", "contract": false, "position": 1,
				"included": []any{map[string]any{"product": "apps", "meter": "egress", "quantity": 100.5, "unit": "GB"}},
				"limits":   []any{map[string]any{"limit": "sandbox.count", "max": 50}, map[string]any{"limit": "sandbox.memory", "max": nil}},
				"features": []any{"sso"},
				"addOns": []any{map[string]any{"id": "extra-seats", "name": "Extra seats", "feeMicros": "20000000",
					"limits": []any{map[string]any{"limit": "member.count", "add": 10}}, "feature": nil}},
			}}})
		case "/v1/plans/choose":
			writeJSON(w, 200, map[string]any{"checkoutUrl": "https://checkout.example.test/c/1", "current": nil})
		default:
			writeJSON(w, 200, map[string]any{"checkoutUrl": nil, "current": now})
		}
	})
	c := s.client(t)
	ctx := context.Background()
	listed, err := c.Plans.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cur := listed.Current
	if cur == nil || cur.PlanID != "pro" || cur.Status != "trialing" || cur.Interval != "year" || !cur.CancelAtPeriodEnd ||
		cur.NextPlanID == nil || *cur.NextPlanID != "team" || cur.TrialEndsAt == nil || cur.TrialEndsAt.Day() != 15 ||
		cur.PeriodEnd == nil || cur.PeriodEnd.Year() != 2026 || !cur.AutoUpgrade ||
		len(cur.AddOns) != 1 || cur.AddOns[0].Quantity != 2 || cur.AddOns[0].EndsAt != nil ||
		cur.PlanUsage == nil || cur.PlanUsage.LeftMicros != "150000000" {
		t.Fatalf("current plan lost: %+v", cur)
	}
	p := listed.Plans[0]
	if len(listed.Plans) != 1 || p.FeeMicros != "100000000" || p.YearlyFeeMicros == nil || *p.YearlyFeeMicros != "1000000000" ||
		p.Card == nil || *p.Card != "pro" || p.TrialDays != 14 || !p.FirstPeriodDouble || p.Support != "agent" || p.Position != 1 ||
		p.Included[0].Quantity != 100.5 || p.Limits[0].Limit != "sandbox.count" || p.Limits[0].Max == nil || *p.Limits[0].Max != 50 ||
		p.Limits[1].Max != nil || p.Features[0] != "sso" || p.AddOns[0].Limits[0].Add != 10 || p.AddOns[0].Feature != nil {
		t.Fatalf("plans lost: %+v", listed.Plans)
	}
	chosen, err := c.Plans.Choose(ctx, "pro", nil)
	if err != nil {
		t.Fatal(err)
	}
	if chosen.CheckoutURL == nil || *chosen.CheckoutURL != "https://checkout.example.test/c/1" || chosen.Current != nil {
		t.Fatalf("choose lost: %+v", chosen)
	}
	if _, err := c.Plans.Choose(ctx, "pro", &PlanChooseOptions{Interval: "year"}); err != nil {
		t.Fatal(err)
	}
	canceled, err := c.Plans.Cancel(ctx)
	if err != nil || canceled.CheckoutURL != nil || canceled.Current == nil {
		t.Fatalf("cancel: %+v %v", canceled, err)
	}
	if _, err := c.Plans.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Plans.AddOn(ctx, "extra-seats"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Plans.RemoveAddOn(ctx, "extra-seats"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Plans.AutoUpgrade(ctx, false); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /v1/plans", "POST /v1/plans/choose", "POST /v1/plans/choose", "POST /v1/plans/cancel",
		"POST /v1/plans/resume", "POST /v1/plans/add-ons", "DELETE /v1/plans/add-ons/extra-seats", "PUT /v1/plans/auto-upgrade"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("requests %v", seen)
	}
	wantBodies := []string{"", `{"plan":"pro"}`, `{"interval":"year","plan":"pro"}`, "{}", "{}", `{"addOn":"extra-seats"}`, "", `{"on":false}`}
	if !reflect.DeepEqual(bodies, wantBodies) {
		t.Fatalf("bodies %q", bodies)
	}
	for i, key := range keys[1:] {
		if key == "" {
			t.Fatalf("change %d sent no Idempotency-Key", i+1)
		}
	}
}

func TestCreateSendsSpreadFromAndReadsPlacement(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		info := map[string]any{}
		for k, v := range running {
			info[k] = v
		}
		info["placement"] = "pl_3f9a0c1b2d4e5f60"
		writeJSON(w, 200, info)
	})
	c := s.client(t)
	sbx, err := c.Sandboxes.Create(context.Background(), &CreateOptions{SpreadFrom: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	if from, _ := sent["spreadFrom"].([]any); len(from) != 2 || from[0] != "a" || from[1] != "b" {
		t.Fatalf("create body %s", s.bodies[0])
	}
	if sbx.Info().Placement == nil || *sbx.Info().Placement != "pl_3f9a0c1b2d4e5f60" {
		t.Fatalf("placement %v", sbx.Info().Placement)
	}
}

func TestCreateAndForkSendSecretsOnlyWhenGivenAndReadThem(t *testing.T) {
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		var sent map[string]any
		_ = json.Unmarshal(body, &sent)
		info := map[string]any{}
		for k, v := range running {
			info[k] = v
		}
		info["secrets"] = sent["secrets"]
		if strings.HasSuffix(r.URL.Path, ":fork") {
			writeJSON(w, 200, map[string]any{"sandboxes": []any{info}})
			return
		}
		writeJSON(w, 200, info)
	})
	c := s.client(t)
	ctx := context.Background()
	var none []string
	for i, opts := range []*CreateOptions{
		{Secrets: &[]string{"ANTHROPIC_API_KEY"}},
		{Secrets: &[]string{}},
		{Secrets: &none},
		{},
		{Snapshot: "snap-1", Secrets: &[]string{}},
	} {
		if _, err := c.Sandboxes.Create(ctx, opts); err != nil {
			t.Fatal(i, err)
		}
	}
	sbx, err := c.Sandboxes.GetOrCreate(ctx, "dev", &CreateOptions{Secrets: &[]string{"ANTHROPIC_API_KEY"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sbx.Fork(ctx, &ForkOptions{Secrets: &[]string{}}); err != nil {
		t.Fatal(err)
	}
	copies, err := sbx.Fork(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A pointer to a nil slice still asks for none, so it is sent as [].
	want := []string{`["ANTHROPIC_API_KEY"]`, `[]`, `[]`, ``, `[]`, `["ANTHROPIC_API_KEY"]`, `[]`, ``}
	if len(s.bodies) != len(want) {
		t.Fatalf("%d requests", len(s.bodies))
	}
	for i, body := range s.bodies {
		var sent map[string]json.RawMessage
		_ = json.Unmarshal(body, &sent)
		if string(sent["secrets"]) != want[i] {
			t.Fatalf("request %d body %s", i, body)
		}
	}
	if got := sbx.Info().Secrets; got == nil || !reflect.DeepEqual(*got, []string{"ANTHROPIC_API_KEY"}) {
		t.Fatalf("secrets %v", got)
	}
	if got := copies[0].Info().Secrets; got != nil {
		t.Fatalf("a copy with every secret reads %v", *got)
	}
}
