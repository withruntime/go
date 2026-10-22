package withruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var agentAnswer = map[string]any{"id": "agt-1", "name": "helpdesk", "version": 2, "labels": map[string]string{},
	"definition": map[string]any{"command": "claude", "vcpu": 1, "sandboxClass": "future"},
	"createdAt":  "2026-10-18T10:00:00Z", "updatedAt": "2026-10-18T10:00:00Z"}

func agentSessionAnswer(state string, lastSeq int) map[string]any {
	return map[string]any{"id": "ses-1", "agentId": "agt-1", "agentVersion": 2, "key": "customer:42", "trigger": "api",
		"state": state, "reason": nil, "result": nil, "sandboxId": nil, "lastSeq": lastSeq, "labels": map[string]string{},
		"createdAt": "2026-10-18T10:00:00Z", "startedAt": nil, "endedAt": nil}
}

var triggerAnswer = map[string]any{"id": "trg-1", "agentId": "agt-1", "kind": "github",
	"config":   map[string]any{"repository": "acme/app", "installation": 12345678},
	"pausedAt": nil, "pausedWhy": nil, "createdAt": "2026-10-18T10:00:00Z"}

func TestAgentRoutesSendWhatTheAPIReads(t *testing.T) {
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
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/sessions") && r.Method == http.MethodPost:
			writeJSON(w, 201, map[string]any{"session": agentSessionAnswer("queued", 1), "joined": false, "seq": 1})
		case strings.HasSuffix(r.URL.Path, "/messages"):
			writeJSON(w, 200, map[string]any{"seq": 7})
		case strings.HasPrefix(r.URL.Path, "/v1/agents/sessions"):
			writeJSON(w, 200, agentSessionAnswer("stopped", 3))
		case strings.Contains(r.URL.Path, "/triggers") && r.Method == http.MethodGet:
			writeJSON(w, 200, map[string]any{"data": []any{triggerAnswer}})
		case strings.Contains(r.URL.Path, "/triggers"):
			writeJSON(w, 200, triggerAnswer)
		default:
			writeJSON(w, 200, agentAnswer)
		}
	})
	c := s.client(t)
	session := &AgentSession{c: c, info: AgentSessionInfo{ID: "ses-1"}}
	definition := AgentDefinition{Command: "claude", VCPU: 1}
	cases := []struct {
		name, method, path string
		run                func() error
		body               map[string]any
		key                string // "auto", "none" or the exact key
	}{
		{"create", "POST", "/v1/agents", func() error {
			agent, err := c.Agents.Create(ctx, "helpdesk", definition, &CreateAgentOptions{Labels: map[string]string{"team": "a"}})
			if err == nil && (agent.ID != "agt-1" || agent.Version != 2 || agent.Definition.Command != "claude" || agent.Warnings == nil) {
				t.Fatalf("agent %+v", agent)
			}
			return err
		}, map[string]any{"name": "helpdesk", "definition": map[string]any{"command": "claude", "vcpu": float64(1)}, "labels": map[string]any{"team": "a"}}, "auto"},
		{"create with a key", "POST", "/v1/agents", func() error {
			_, err := c.Agents.Create(ctx, "helpdesk", definition, &CreateAgentOptions{IdempotencyKey: "make-1"})
			return err
		}, map[string]any{"name": "helpdesk", "definition": map[string]any{"command": "claude", "vcpu": float64(1)}}, "make-1"},
		{"get by name", "GET", "/v1/agents/help%20desk", func() error { _, err := c.Agents.Get(ctx, "help desk"); return err }, nil, "none"},
		{"update", "PATCH", "/v1/agents/agt-1", func() error {
			_, err := c.Agents.Update(ctx, "agt-1", AgentDefinition{Command: "codex"})
			return err
		}, map[string]any{"definition": map[string]any{"command": "codex"}}, "auto"},
		{"delete", "DELETE", "/v1/agents/agt-1", func() error { return c.Agents.Delete(ctx, "agt-1") }, nil, "auto"},
		{"start", "POST", "/v1/agents/helpdesk/sessions", func() error {
			started, err := c.Agents.Start(ctx, "helpdesk", "hello", &StartAgentOptions{Key: "customer:42", Labels: map[string]string{"plan": "pro"}})
			if err == nil && (started.ID() != "ses-1" || started.Seq() != 1 || started.Joined() || started.Info().State != "queued") {
				t.Fatalf("session %+v", started.Info())
			}
			return err
		}, map[string]any{"input": "hello", "key": "customer:42", "labels": map[string]any{"plan": "pro"}}, "auto"},
		{"start sends only what is given", "POST", "/v1/agents/helpdesk/sessions", func() error {
			_, err := c.Agents.Start(ctx, "helpdesk", "hello", &StartAgentOptions{IdempotencyKey: "start-1"})
			return err
		}, map[string]any{"input": "hello"}, "start-1"},
		{"session get", "GET", "/v1/agents/sessions/ses-1", func() error {
			found, err := c.Agents.Sessions.Get(ctx, "ses-1")
			if err == nil && (found.Seq() != 3 || found.Joined() || !found.Info().Ended() || *found.Info().Key != "customer:42") {
				t.Fatalf("session %+v", found.Info())
			}
			return err
		}, nil, "none"},
		{"session delete", "DELETE", "/v1/agents/sessions/ses-1", func() error { return c.Agents.Sessions.Delete(ctx, "ses-1") }, nil, "auto"},
		{"send", "POST", "/v1/agents/sessions/ses-1/messages", func() error {
			seq, err := session.Send(ctx, "more", nil)
			if err == nil && seq != 7 {
				t.Fatalf("seq %d", seq)
			}
			return err
		}, map[string]any{"text": "more"}, "auto"},
		{"send with a key", "POST", "/v1/agents/sessions/ses-1/messages", func() error {
			_, err := session.Send(ctx, "more", &AgentSendOptions{IdempotencyKey: "word-1"})
			return err
		}, map[string]any{"text": "more"}, "word-1"},
		{"stop", "POST", "/v1/agents/sessions/ses-1/stop", func() error {
			info, err := session.Stop(ctx)
			if err == nil && (info.State != "stopped" || session.Info().State != "stopped") {
				t.Fatal("the handle did not keep the answer")
			}
			return err
		}, map[string]any{}, "auto"},
		{"trigger create", "POST", "/v1/agents/agt-1/triggers", func() error {
			trigger, err := c.Agents.Triggers.Create(ctx, "agt-1", "github", GitHubTrigger{Repository: "acme/app", Installation: 12345678})
			if err == nil && (trigger.Kind != "github" || trigger.Config["repository"] != "acme/app") {
				t.Fatalf("trigger %+v", trigger)
			}
			return err
		}, map[string]any{"config": map[string]any{"installation": float64(12345678), "repository": "acme/app"}, "kind": "github"}, "auto"},
		{"trigger create schedule", "POST", "/v1/agents/agt-1/triggers", func() error {
			_, err := c.Agents.Triggers.Create(ctx, "agt-1", "schedule", ScheduleTrigger{Cron: "0 9 * * 1-5", Input: "standup"})
			return err
		}, map[string]any{"config": map[string]any{"cron": "0 9 * * 1-5", "input": "standup"}, "kind": "schedule"}, "auto"},
		{"trigger create webhook", "POST", "/v1/agents/agt-1/triggers", func() error {
			_, err := c.Agents.Triggers.Create(ctx, "agt-1", "webhook", nil)
			return err
		}, map[string]any{"config": map[string]any{}, "kind": "webhook"}, "auto"},
		{"trigger list", "GET", "/v1/agents/agt-1/triggers", func() error {
			triggers, err := c.Agents.Triggers.List(ctx, "agt-1")
			if err == nil && (len(triggers) != 1 || triggers[0].ID != "trg-1") {
				t.Fatalf("triggers %+v", triggers)
			}
			return err
		}, nil, "none"},
		{"trigger pause", "POST", "/v1/agents/triggers/trg-1/pause", func() error {
			_, err := c.Agents.Triggers.Pause(ctx, "trg-1", "holiday")
			return err
		}, map[string]any{"why": "holiday"}, "auto"},
		{"trigger resume", "POST", "/v1/agents/triggers/trg-1/resume", func() error { _, err := c.Agents.Triggers.Resume(ctx, "trg-1"); return err }, map[string]any{}, "auto"},
		{"trigger delete", "DELETE", "/v1/agents/triggers/trg-1", func() error { return c.Agents.Triggers.Delete(ctx, "trg-1") }, nil, "auto"},
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

func TestAgentAndSessionListsFollowNextCursor(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		row := agentAnswer
		if strings.HasPrefix(r.URL.Path, "/v1/agents/sessions") {
			row = agentSessionAnswer("done", 4)
		}
		if r.URL.Query().Get("cursor") == "" {
			writeJSON(w, 200, map[string]any{"data": []any{row, row}, "nextCursor": "c2"})
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{row}, "nextCursor": nil})
	})
	c := s.client(t)
	ctx := context.Background()
	agents := 0
	for agent, err := range c.Agents.All(ctx) {
		if err != nil || agent.ID != "agt-1" {
			t.Fatal(agent, err)
		}
		agents++
	}
	sessions := 0
	for session, err := range c.Agents.Sessions.All(ctx, &AgentSessionListOptions{Agent: "helpdesk", State: "done", Limit: 2}) {
		if err != nil || session.State != "done" {
			t.Fatal(session, err)
		}
		sessions++
	}
	page, err := c.Agents.List(ctx, &AgentListOptions{Cursor: "c2", Limit: 5})
	if err != nil || page.HasMore() || len(page.Data) != 1 {
		t.Fatalf("resumed page %+v %v", page, err)
	}
	if agents != 3 || sessions != 3 {
		t.Fatalf("walked %d agents and %d sessions", agents, sessions)
	}
	var paths []string
	for _, r := range s.requests {
		paths = append(paths, r.URL.RequestURI())
	}
	want := "/v1/agents /v1/agents?cursor=c2 /v1/agents/sessions?agent=helpdesk&limit=2&state=done " +
		"/v1/agents/sessions?agent=helpdesk&cursor=c2&limit=2&state=done /v1/agents?cursor=c2&limit=5"
	if strings.Join(paths, " ") != want {
		t.Fatalf("requests %v", paths)
	}
}

func agentEvent(seq int, kind, text string) map[string]any {
	return map[string]any{"seq": seq, "kind": kind, "actor": "agent", "body": map[string]any{"text": text}, "createdAt": "2026-10-18T10:00:00Z"}
}

// The log is read page by page, after the last event read; once a page says
// the session ended, the rest is read without waiting until a page is empty.
func TestAgentEventsLongPollThenDrainTheEnd(t *testing.T) {
	pages := map[string]map[string]any{
		"after=0&wait=30":  {"data": []any{agentEvent(1, "input", "hi"), agentEvent(2, "status", "reading")}, "ended": false},
		"after=2&wait=30":  {"data": []any{}, "ended": false},
		"after=2&wait=30#": {"data": []any{agentEvent(3, "reply", "hello")}, "ended": true},
		"after=3&wait=0":   {"data": []any{agentEvent(4, "result", "done")}, "ended": true},
		"after=4&wait=0":   {"data": []any{}, "ended": true},
	}
	var mu sync.Mutex
	seen := map[string]int{}
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.URL.Path != "/v1/agents/sessions/ses-1/events" {
			t.Errorf("path %s", r.URL.Path)
		}
		mu.Lock()
		query := r.URL.RawQuery
		seen[query]++
		if query == "after=2&wait=30" && seen[query] == 2 {
			query += "#"
		}
		mu.Unlock()
		page, ok := pages[query]
		if !ok {
			t.Errorf("unexpected read %s", query)
			writeJSON(w, 500, map[string]any{})
			return
		}
		writeJSON(w, 200, page)
	})
	session := &AgentSession{c: s.client(t), info: AgentSessionInfo{ID: "ses-1"}}
	var texts []string
	for event, err := range session.Events(context.Background(), 0) {
		if err != nil {
			t.Fatal(err)
		}
		texts = append(texts, event.Kind+":"+event.Text())
	}
	if strings.Join(texts, " ") != "input:hi status:reading reply:hello result:done" {
		t.Fatalf("events %v", texts)
	}
	if len(s.requests) != 5 {
		t.Fatalf("%d reads", len(s.requests))
	}
	// Breaking out stops reading.
	before := len(s.requests)
	for range session.Events(context.Background(), 0) {
		break
	}
	if len(s.requests) != before+1 {
		t.Fatal("a break did not stop the reads")
	}
}

// A long poll holds the answer up to 30 s: its deadline is longer than a
// client's shorter timeout.
func TestAgentEventsOutlastAShortClientTimeout(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			time.Sleep(700 * time.Millisecond)
			writeJSON(w, 200, map[string]any{"data": []any{}, "ended": true})
			return
		}
		time.Sleep(700 * time.Millisecond)
		writeJSON(w, 200, agentSessionAnswer("done", 0))
	})
	c := s.client(t, WithTimeout(500*time.Millisecond), WithMaxRetries(0))
	session := &AgentSession{c: c, info: AgentSessionInfo{ID: "ses-1"}}
	if _, err := session.events(context.Background(), 0, 30); err != nil {
		t.Fatalf("the long poll kept the client's 500 ms: %v", err)
	}
	if err := session.Refresh(context.Background()); err == nil {
		t.Fatal("an ordinary read lost the client's timeout")
	}
}

func TestAgentSessionWaitReturnsTheEndedSession(t *testing.T) {
	var mu sync.Mutex
	gets := 0
	var polls []string
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/events") {
			polls = append(polls, r.URL.RawQuery)
			writeJSON(w, 200, map[string]any{"data": []any{agentEvent(gets+5, "status", "")}, "ended": gets == 2})
			return
		}
		gets++
		switch gets {
		case 1:
			writeJSON(w, 200, agentSessionAnswer("running", 5))
		case 2:
			writeJSON(w, 200, agentSessionAnswer("waiting", 6))
		default:
			failed := agentSessionAnswer("failed", 7)
			failed["reason"] = "the command exited 1"
			writeJSON(w, 200, failed)
		}
	})
	session := &AgentSession{c: s.client(t), info: AgentSessionInfo{ID: "ses-1"}}
	info, err := session.Wait(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != "failed" || *info.Reason != "the command exited 1" || session.Info().State != "failed" {
		t.Fatalf("info %+v", info)
	}
	if strings.Join(polls, " ") != "after=5&wait=30 after=6&wait=30" {
		t.Fatalf("polls %v", polls)
	}
}

func TestAgentSessionWaitTimesOut(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			if r.URL.Query().Get("wait") != "1" {
				t.Errorf("a 200 ms wait asked the server to hold %s s", r.URL.Query().Get("wait"))
			}
			<-r.Context().Done()
			return
		}
		writeJSON(w, 200, agentSessionAnswer("running", 2))
	})
	session := &AgentSession{c: s.client(t), info: AgentSessionInfo{ID: "ses-1"}}
	started := time.Now()
	_, err := session.Wait(context.Background(), 200*time.Millisecond)
	failure, ok := asError(err)
	if !ok || failure.Code != "timeout" || !strings.Contains(failure.Message, "ses-1") {
		t.Fatalf("want a timeout, got %v", err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("the timeout was not kept")
	}
}

// The API's refusals of agents are the SDK's usual typed errors, and none is
// retried but by status, as every call is.
func TestAgentErrorsAreTyped(t *testing.T) {
	cases := []struct {
		status int
		code   string
		kind   error
	}{
		{403, "product_in_beta", ErrPermissionDenied},
		{403, "permission_denied", ErrPermissionDenied},
		{404, "not_found", ErrNotFound},
		{409, "session_ended", ErrConflict},
		{409, "name_taken", ErrConflict},
		{409, "trigger_paused", ErrConflict},
		{409, "not_installed", ErrConflict},
		{402, "budget_reached", nil},
		{409, "session_open", ErrConflict},
		{429, "quota_exceeded", ErrRateLimited},
		{400, "invalid_request", ErrInvalidRequest},
		{401, "unauthorized", ErrAuthentication},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
				apiError(w, tc.status, tc.code, map[string]any{"message": "trigger paused: holiday"})
			})
			c := s.client(t, WithMaxRetries(0))
			_, err := c.Agents.Start(context.Background(), "helpdesk", "hi", nil)
			failure, ok := asError(err)
			if !ok || failure.Code != tc.code || failure.Status != tc.status || (tc.kind != nil && !errors.Is(err, tc.kind)) {
				t.Fatalf("got %#v", err)
			}
			if failure.Message != "trigger paused: holiday" {
				t.Fatalf("the why was lost: %q", failure.Message)
			}
			if len(s.requests) != 1 {
				t.Fatalf("%d requests", len(s.requests))
			}
		})
	}
	// With retries on, a refusal that is not a passing state is sent once.
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		apiError(w, 409, "session_ended", nil)
	})
	session := &AgentSession{c: s.client(t), info: AgentSessionInfo{ID: "ses-1"}}
	if _, err := session.Send(context.Background(), "late", nil); !errors.Is(err, ErrConflict) || len(s.requests) != 1 {
		t.Fatalf("%d requests: %v", len(s.requests), err)
	}
}

func TestAgentDefinitionKeepsFieldsNewerThanTheSDK(t *testing.T) {
	var agent Agent
	encoded, _ := json.Marshal(agentAnswer)
	if err := json.Unmarshal(encoded, &agent); err != nil {
		t.Fatal(err)
	}
	if agent.Definition.Extra["sandboxClass"] != "future" || agent.Warnings == nil || len(agent.Warnings) != 0 {
		t.Fatalf("agent %+v", agent)
	}
	back, err := json.Marshal(agent.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if string(back) != `{"command":"claude","sandboxClass":"future","vcpu":1}` {
		t.Fatalf("written back as %s", back)
	}
	full := AgentDefinition{
		Command: "claude", From: &AgentFrom{Image: "runtime:latest"}, Env: map[string]string{"A": "1"},
		MCP: []AgentMCPServer{{Name: "docs", URL: "https://docs.example.com/mcp"}}, Volumes: []AgentVolume{{Volume: "v", Path: "/data"}},
		MaxSessionSeconds: 3600,
	}
	back, _ = json.Marshal(full)
	if string(back) != `{"command":"claude","from":{"image":"runtime:latest"},"env":{"A":"1"},"mcp":[{"name":"docs","url":"https://docs.example.com/mcp"}],"volumes":[{"volume":"v","path":"/data"}],"maxSessionSeconds":3600}` {
		t.Fatalf("full definition %s", back)
	}
	var warned Agent
	_ = json.Unmarshal([]byte(`{"id":"a","warnings":["subscription_for_customers"]}`), &warned)
	if len(warned.Warnings) != 1 || warned.Warnings[0] != "subscription_for_customers" {
		t.Fatalf("warnings %v", warned.Warnings)
	}
}

// The templates are the guide's, value for value and in its order
// (packages/cloud-guide/agent-templates.json); skipped when the SDK is on its
// own.
func TestAgentTemplatesAreTheGuides(t *testing.T) {
	raw, err := os.ReadFile("../../packages/cloud-guide/agent-templates.json")
	if err != nil {
		t.Skip("the guide's templates are not beside this module")
	}
	var want any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	c, err := New(WithAPIKey("rk_templates"))
	if err != nil {
		t.Fatal(err)
	}
	templates := map[string]AgentTemplate{}
	var order []string
	for _, template := range c.Agents.Templates.List() {
		templates[template.Key] = template
		order = append(order, template.Key)
	}
	uses := map[string]AgentUse{}
	var useOrder []string
	for _, use := range c.Agents.Templates.Uses() {
		uses[use.Key] = use
		useOrder = append(useOrder, use.Key)
	}
	encoded, _ := json.Marshal(map[string]any{"templates": templates, "uses": uses})
	var got any
	_ = json.Unmarshal(encoded, &got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the templates differ from the guide's:\n%s\nwant\n%s", encoded, raw)
	}
	wantOrder, wantUseOrder := jsonKeys(t, raw, "templates"), jsonKeys(t, raw, "uses")
	if strings.Join(order, ",") != strings.Join(wantOrder, ",") || strings.Join(useOrder, ",") != strings.Join(wantUseOrder, ",") {
		t.Fatalf("order %v %v want %v %v", order, useOrder, wantOrder, wantUseOrder)
	}
}

// jsonKeys is the keys of the object at top-level field name, in file order.
func jsonKeys(t *testing.T, raw []byte, name string) []string {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(top[name]))
	if _, err := decoder.Token(); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, token.(string))
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestAgentTemplateDefinitionMergesAUseOverAHarness(t *testing.T) {
	templates := &AgentTemplateService{}
	support, err := templates.Definition("claude-code", "support")
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(support)
	want := `{"command":"npx -y @anthropic-ai/claude-code --dangerously-skip-permissions --mcp-config ~/.runtime-agent/mcp.json -p \"$(cat ~/.runtime-agent/start.md)\"",` +
		`"vcpu":1,"memoryMiB":1024,"secrets":["ANTHROPIC_API_KEY"],"idlePauseSeconds":10,"maxConcurrent":1000,"template":"support"}`
	if string(encoded) != want {
		t.Fatalf("merged %s", encoded)
	}
	alone, err := templates.Definition("codex", "")
	if err != nil || alone.Template != "" || alone.VCPU != 0 || alone.Secrets[0] != "OPENAI_API_KEY" {
		t.Fatalf("alone %+v %v", alone, err)
	}
	team, _ := templates.Definition("gemini-cli", "team")
	if team.Template != "team" || team.VCPU != 4 || team.MemoryMiB != 8192 || team.Secrets[0] != "GEMINI_API_KEY" {
		t.Fatalf("team %+v", team)
	}
	// A new definition each time: changing one changes no other.
	support.Secrets[0] = "CHANGED"
	if again, _ := templates.Definition("claude-code", "support"); again.Secrets[0] != "ANTHROPIC_API_KEY" {
		t.Fatal("the template was changed through a definition")
	}
	if template, _ := templates.Get("claude-code"); template.Definition.Secrets[0] != "ANTHROPIC_API_KEY" {
		t.Fatal("the template was changed through a definition")
	}
	for _, args := range [][2]string{{"claude", ""}, {"claude-code", "customers"}} {
		_, err := templates.Definition(args[0], args[1])
		failure, ok := asError(err)
		if !ok || failure.Code != "invalid_request" || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%v: got %v", args, err)
		}
	}
	if _, ok := templates.Use("support"); !ok {
		t.Fatal("no support use")
	}
	if _, ok := templates.Get("support"); ok {
		t.Fatal("a use is not a template")
	}
}

func TestAgentSessionTreeCostAndStorageReadAsSent(t *testing.T) {
	var child AgentSessionInfo
	body := `{"id":"ses-2","state":"done","trigger":"spawn","reason":"budget","stopWhy":"budget",` +
		`"parentSessionId":"ses-1","rootSessionId":"ses-1","depth":1,` +
		`"cost":{"dollars":0.25,"vcpuSeconds":120,"gibSeconds":240},"treeCost":{"dollars":0.5,"vcpuSeconds":240,"gibSeconds":480},` +
		`"historyBytes":2048,"attachmentBytes":4096,"deletedAt":null,"createdAt":"2026-10-18T10:00:00Z"}`
	if err := json.Unmarshal([]byte(body), &child); err != nil {
		t.Fatal(err)
	}
	if child.Trigger != "spawn" || *child.Reason != "budget" || *child.StopWhy != "budget" || *child.ParentSessionID != "ses-1" ||
		child.RootSessionID != "ses-1" || child.Depth != 1 || child.Cost.Dollars != 0.25 || child.Cost.VCPUSeconds != 120 ||
		child.TreeCost.GiBSeconds != 480 || child.HistoryBytes != 2048 || child.AttachmentBytes != 4096 || child.DeletedAt != nil {
		t.Fatalf("child %+v", child)
	}
	var top AgentSessionInfo
	if err := json.Unmarshal([]byte(`{"id":"ses-1","rootSessionId":"ses-1","depth":0,"parentSessionId":null,"stopWhy":null}`), &top); err != nil {
		t.Fatal(err)
	}
	if top.ParentSessionID != nil || top.RootSessionID != top.ID || top.Depth != 0 || top.StopWhy != nil {
		t.Fatalf("top %+v", top)
	}
	// A deleted session is kept as a record: its time is parsed, its cost kept.
	var deleted AgentSessionInfo
	body = `{"id":"ses-3","state":"done","reason":"deleted","key":null,"result":null,"deletedAt":"2026-10-19T08:30:00Z",` +
		`"cost":{"dollars":1.5,"vcpuSeconds":600,"gibSeconds":1200}}`
	if err := json.Unmarshal([]byte(body), &deleted); err != nil {
		t.Fatal(err)
	}
	if deleted.DeletedAt == nil || !deleted.DeletedAt.Equal(time.Date(2026, 10, 19, 8, 30, 0, 0, time.UTC)) ||
		*deleted.Reason != "deleted" || deleted.Key != nil || deleted.Cost.Dollars != 1.5 {
		t.Fatalf("deleted %+v", deleted)
	}
}

func TestAgentChildrenAndTreeListsReadByParentRootAndDeleted(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		row := agentSessionAnswer("done", 4)
		row["trigger"], row["parentSessionId"], row["depth"] = "spawn", "ses-1", 1
		if r.URL.Query().Get("cursor") == "" {
			writeJSON(w, 200, map[string]any{"data": []any{row}, "nextCursor": "c2"})
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{row}, "nextCursor": nil})
	})
	c := s.client(t)
	ctx := context.Background()
	session := &AgentSession{c: c, info: AgentSessionInfo{ID: "ses 1"}}
	page, err := session.Children(ctx)
	if err != nil || !page.HasMore() || page.Data[0].Trigger != "spawn" || *page.Data[0].ParentSessionID != "ses-1" || page.Data[0].Depth != 1 {
		t.Fatalf("children %+v %v", page, err)
	}
	children := 0
	for child, err := range page.All(ctx) {
		if err != nil || child.Trigger != "spawn" {
			t.Fatal(child, err)
		}
		children++
	}
	tree := 0
	for _, err := range c.Agents.Sessions.All(ctx, &AgentSessionListOptions{Root: "ses-1", Deleted: true}) {
		if err != nil {
			t.Fatal(err)
		}
		tree++
	}
	if _, err := c.Agents.Sessions.List(ctx, &AgentSessionListOptions{Parent: "ses-1", State: "done", Limit: 5}); err != nil {
		t.Fatal(err)
	}
	if children != 2 || tree != 2 {
		t.Fatalf("walked %d children and %d of the tree", children, tree)
	}
	var paths []string
	for _, r := range s.requests {
		paths = append(paths, r.URL.RequestURI())
	}
	want := "/v1/agents/sessions?parent=ses+1 /v1/agents/sessions?cursor=c2&parent=ses+1 " +
		"/v1/agents/sessions?deleted=true&root=ses-1 /v1/agents/sessions?cursor=c2&deleted=true&root=ses-1 " +
		"/v1/agents/sessions?limit=5&parent=ses-1&state=done"
	if strings.Join(paths, " ") != want {
		t.Fatalf("requests %v", paths)
	}
}
