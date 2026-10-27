package withruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
)

func workflowAt(version int64, state string, result any) map[string]any {
	reason := any(nil)
	if state != "running" {
		reason = "done"
	}
	return map[string]any{"id": "w1", "name": "triage", "state": state, "reason": reason, "result": result,
		"parentSessionId": nil, "spec": map[string]any{}, "maxSessions": 100, "maxConcurrent": 20, "maxDollars": 10,
		"started": 2, "version": version, "labels": map[string]any{},
		"phases": []any{map[string]any{"name": "fix", "agentId": "a", "total": 2, "ready": true, "pending": 0,
			"running": 0, "done": 2, "failed": 0}},
		"cost":      map[string]any{"dollars": 0.01, "vcpuSeconds": 1, "gibSeconds": 2},
		"createdAt": "2026-10-26T00:00:00Z", "updatedAt": "2026-10-26T00:00:00Z", "endedAt": nil}
}

func TestAgentWorkflowsRunFollowWaitStepsAndStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	var seen []string
	var sent map[string]any
	version := int64(1)
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/agents/workflows":
			_ = json.Unmarshal(body, &sent)
			writeJSON(w, 201, workflowAt(1, "running", nil))
		case r.URL.Path == "/v1/agents/workflows":
			writeJSON(w, 200, map[string]any{"data": []any{workflowAt(1, "running", nil)}, "nextCursor": nil})
		case r.URL.Path == "/v1/agents/workflows/w1/steps":
			writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"phase": "fix", "item": 0, "itemText": "a.ts",
				"state": "done", "reason": "done", "result": "ok", "sessionId": "s1", "createdAt": "2026-10-26T00:00:00Z",
				"startedAt": nil, "endedAt": nil}}, "nextCursor": nil})
		case r.URL.Path == "/v1/agents/workflows/w1/stop":
			writeJSON(w, 200, workflowAt(9, "stopped", nil))
		case r.URL.Query().Get("wait") == "":
			writeJSON(w, 200, workflowAt(version, "running", nil))
		default:
			// Each long poll returns the next version; the third has ended.
			version++
			if version >= 3 {
				writeJSON(w, 200, workflowAt(version, "done", `["X","Y"]`))
			} else {
				writeJSON(w, 200, workflowAt(version, "running", nil))
			}
		}
	})
	c := s.client(t)
	spec := AgentWorkflowSpec{Agent: "dev", MaxConcurrent: 50, Phases: []AgentWorkflowPhase{
		{Name: "fix", Input: "Fix {{item}}", Each: []string{"a.ts", "b.ts"}},
		{Name: "check", Input: "Check {{item}}", Each: AgentWorkflowSource{From: "fix", Split: "lines"}, Pipeline: true},
	}}
	flow, err := c.Agents.Workflows.Run(ctx, spec, nil)
	if err != nil || flow.ID() != "w1" {
		t.Fatalf("run %v %v", flow, err)
	}
	var versions []int64
	for now, err := range flow.Follow(ctx, 0) {
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, now.Version)
	}
	if !reflect.DeepEqual(versions, []int64{2, 3}) || *flow.Info().Result != `["X","Y"]` {
		t.Fatalf("followed %v %+v", versions, flow.Info())
	}
	mu.Lock()
	version = 1
	mu.Unlock()
	got, err := c.Agents.Workflows.Get(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	ended, err := got.Wait(ctx)
	if err != nil || ended.State != "done" || ended.Phases[0].Done != 2 {
		t.Fatalf("wait %+v %v", ended, err)
	}
	steps, err := flow.Steps(ctx, "done")
	if err != nil || *steps.Data[0].ItemText != "a.ts" {
		t.Fatalf("steps %+v %v", steps, err)
	}
	list, err := c.Agents.Workflows.List(ctx, &AgentWorkflowListOptions{States: []string{"running"}})
	if err != nil || len(list.Data) != 1 {
		t.Fatalf("list %+v %v", list, err)
	}
	stopped, err := flow.Stop(ctx)
	if err != nil || stopped.State != "stopped" {
		t.Fatalf("stop %+v %v", stopped, err)
	}
	wantSpec := map[string]any{"agent": "dev", "maxConcurrent": float64(50), "phases": []any{
		map[string]any{"name": "fix", "input": "Fix {{item}}", "each": []any{"a.ts", "b.ts"}},
		map[string]any{"name": "check", "input": "Check {{item}}", "each": map[string]any{"from": "fix", "split": "lines"},
			"pipeline": true},
	}}
	if !reflect.DeepEqual(sent, wantSpec) {
		t.Fatalf("spec sent %v", sent)
	}
	want := []string{
		"POST /v1/agents/workflows",
		"GET /v1/agents/workflows/w1?after=0&wait=30",
		"GET /v1/agents/workflows/w1?after=2&wait=30",
		"GET /v1/agents/workflows/w1",
		"GET /v1/agents/workflows/w1",
		"GET /v1/agents/workflows/w1?after=1&wait=30",
		"GET /v1/agents/workflows/w1?after=2&wait=30",
		"GET /v1/agents/workflows/w1/steps?state=done",
		"GET /v1/agents/workflows?state=running",
		"POST /v1/agents/workflows/w1/stop",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("routes\n got %q\nwant %q", seen, want)
	}
}

func TestAgentDefinitionKeepsMaxChildrenAndTheAdvisor(t *testing.T) {
	t.Parallel()
	var d AgentDefinition
	in := `{"command":"run","maxChildren":300,"advisor":{"model":"claude-opus-5-5","maxCalls":5},"later":1}`
	if err := json.Unmarshal([]byte(in), &d); err != nil {
		t.Fatal(err)
	}
	if d.MaxChildren != 300 || d.Advisor.Model != "claude-opus-5-5" || d.Advisor.MaxCalls != 5 || d.Extra["later"] != float64(1) || d.Extra["advisor"] != nil {
		t.Fatalf("read %+v", d)
	}
	out, _ := json.Marshal(d)
	var back, want map[string]any
	_ = json.Unmarshal(out, &back)
	_ = json.Unmarshal([]byte(in), &want)
	if !reflect.DeepEqual(back, want) {
		t.Fatalf("written %s", out)
	}
}
