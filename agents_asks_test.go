package withruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
)

var inboxAsk = map[string]any{"id": "ask-1", "agentId": "a", "step": "bucket-cap", "question": "Raise the cap?",
	"why": nil, "options": []any{map[string]any{"label": "Raise", "detail": "$40 a month"}, map[string]any{"label": "Keep"}},
	"pick": "Raise", "labels": map[string]any{"team": "infra"}, "state": "open", "answer": nil, "answeredBy": nil,
	"sessions":  []any{map[string]any{"sessionId": "s1", "state": "asking", "seq": 2, "answerSeq": nil}},
	"expiresAt": nil, "createdAt": "2026-10-24T10:00:00Z", "updatedAt": "2026-10-24T10:00:00Z", "closedAt": nil}

func TestAgentAsksListByStateAndLabelsGetAndAnswer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	var seen []string
	var bodies []map[string]any
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		var received map[string]any
		if len(body) > 0 {
			_ = json.Unmarshal(body, &received)
		}
		bodies = append(bodies, received)
		mu.Unlock()
		switch {
		case r.URL.Path == "/v1/agents/asks":
			writeJSON(w, 200, map[string]any{"data": []any{inboxAsk}, "nextCursor": nil})
		case r.Method == http.MethodPost:
			answered := map[string]any{}
			for k, v := range inboxAsk {
				answered[k] = v
			}
			answered["state"], answered["answer"] = "answered", received
			writeJSON(w, 200, answered)
		default:
			writeJSON(w, 200, inboxAsk)
		}
	})
	c := s.client(t)
	page, err := c.Agents.Asks.List(ctx, &AgentAskListOptions{States: []string{"open", "expired"},
		Labels: map[string]string{"team": "infra", "for": "marc"}})
	if err != nil || len(page.Data) != 1 || *page.Data[0].Step != "bucket-cap" || page.Data[0].Options[0].Detail != "$40 a month" {
		t.Fatalf("list %+v %v", page, err)
	}
	one, err := c.Agents.Asks.Get(ctx, "ask-1")
	if err != nil || *one.Pick != "Raise" || one.Sessions[0].Seq != 2 || one.Sessions[0].AnswerSeq != nil {
		t.Fatalf("get %+v %v", one, err)
	}
	answered, err := c.Agents.Asks.Answer(ctx, "ask-1", "Raise", &AgentAnswerOptions{Text: "Done."})
	if err != nil || answered.State != "answered" || answered.Answer.Option != "Raise" || answered.Answer.Text != "Done." {
		t.Fatalf("answer %+v %v", answered, err)
	}
	if _, err := c.Agents.Asks.Answer(ctx, "ask-1", "", nil); err == nil {
		t.Fatal("an empty answer was sent")
	}
	want := []string{
		"GET /v1/agents/asks?label=for%3Amarc&label=team%3Ainfra&state=open&state=expired",
		"GET /v1/agents/asks/ask-1",
		"POST /v1/agents/asks/ask-1/answer",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("routes\n got %q\nwant %q", seen, want)
	}
	if !reflect.DeepEqual(bodies[2], map[string]any{"option": "Raise", "text": "Done."}) {
		t.Fatalf("answer body %v", bodies[2])
	}
}

func TestAgentAsksApproveAndDenyAHeldToolCall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	var bodies []map[string]any
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		var received map[string]any
		_ = json.Unmarshal(body, &received)
		mu.Lock()
		bodies = append(bodies, received)
		mu.Unlock()
		held := map[string]any{}
		for k, v := range inboxAsk {
			held[k] = v
		}
		held["state"], held["answer"] = "answered", received
		held["toolCall"] = map[string]any{"tool": "Bash", "input": map[string]any{"command": "rm -rf build"}, "seq": 4, "policy": "always_ask"}
		writeJSON(w, 200, held)
	})
	c := s.client(t)
	allowed, err := c.Agents.Asks.Approve(ctx, "ask-1")
	if err != nil || allowed.ToolCall == nil || allowed.ToolCall.Tool != "Bash" || allowed.ToolCall.Seq != 4 || allowed.ToolCall.Input["command"] != "rm -rf build" {
		t.Fatalf("approve %+v %v", allowed, err)
	}
	if _, err := c.Agents.Asks.Deny(ctx, "ask-1", "Clean dist/ only."); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Agents.Asks.Deny(ctx, "ask-1", ""); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"option": "Allow"}, {"option": "Deny", "text": "Clean dist/ only."}, {"option": "Deny"}}
	if !reflect.DeepEqual(bodies, want) {
		t.Fatalf("bodies %v", bodies)
	}
}

func TestAgentDefinitionPermissionsRoundTrip(t *testing.T) {
	d := AgentDefinition{Command: "claude", Permissions: &AgentPermissions{Default: "auto", Tools: map[string]string{"shell": "always_ask"}}}
	body, err := json.Marshal(d)
	if err != nil || string(body) != `{"command":"claude","permissions":{"default":"auto","tools":{"shell":"always_ask"}}}` {
		t.Fatalf("%s %v", body, err)
	}
	var back AgentDefinition
	if err := json.Unmarshal(body, &back); err != nil || back.Permissions.Tools["shell"] != "always_ask" || back.Extra != nil {
		t.Fatalf("%+v %v", back, err)
	}
}
