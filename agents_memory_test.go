package withruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
)

var memoryEntry = map[string]any{"scope": "", "path": "notes/tea.md", "bytes": 21,
	"updatedAt": "2026-10-18T10:00:00Z", "updatedBy": "key_1"}

var memoryUsage = map[string]any{"bytes": 42, "files": 2, "limitBytes": 1073741824, "limitFiles": 100000}

func TestAgentMemoryRoutesSendTheScopeInTheQuery(t *testing.T) {
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
		q := r.URL.Query()
		switch {
		case r.URL.Path == "/v1/agents/dev/memory" && q.Has("q"):
			hit := map[string]any{"snippet": "**tea**", "rank": 0.5}
			for k, v := range memoryEntry {
				hit[k] = v
			}
			writeJSON(w, 200, map[string]any{"data": []any{hit}, "nextCursor": nil})
		case r.URL.Path == "/v1/agents/dev/memory" && q.Has("cursor"):
			second := map[string]any{}
			for k, v := range memoryEntry {
				second[k] = v
			}
			second["scope"] = "customer:42"
			writeJSON(w, 200, map[string]any{"data": []any{second}, "nextCursor": nil, "usage": memoryUsage})
		case r.URL.Path == "/v1/agents/dev/memory":
			writeJSON(w, 200, map[string]any{"data": []any{memoryEntry}, "nextCursor": "page-2", "usage": memoryUsage})
		case r.Method == http.MethodPut:
			writeJSON(w, 200, map[string]any{"scope": q.Get("scope"), "path": q.Get("path"), "bytes": 9,
				"created": true, "updatedAt": "2026-10-18T10:00:00Z"})
		case r.Method == http.MethodDelete:
			writeJSON(w, 200, map[string]any{"deleted": true})
		default:
			file := map[string]any{"content": "tea, no sugar"}
			for k, v := range memoryEntry {
				file[k] = v
			}
			writeJSON(w, 200, file)
		}
	})
	c := s.client(t)
	page, err := c.Agents.Memory.List(ctx, "dev", &AgentMemoryListOptions{Prefix: "notes/", Limit: 1})
	if err != nil || page.Usage.Files != 2 || page.Usage.LimitBytes != 1073741824 {
		t.Fatalf("list %+v %v", page, err)
	}
	var scopes []string
	for f, err := range page.All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		scopes = append(scopes, f.Scope)
	}
	if !reflect.DeepEqual(scopes, []string{"", "customer:42"}) {
		t.Fatalf("scopes %q", scopes)
	}
	shared := ""
	hits, err := c.Agents.Memory.Search(ctx, "dev", "tea", &AgentMemorySearchOptions{Scope: &shared})
	if err != nil || len(hits) != 1 || hits[0].Snippet != "**tea**" || hits[0].Rank != 0.5 {
		t.Fatalf("search %+v %v", hits, err)
	}
	file, err := c.Agents.Memory.Read(ctx, "dev", "notes/tea.md", nil)
	if err != nil || file.Content != "tea, no sugar" || file.Path != "notes/tea.md" || file.UpdatedBy != "key_1" {
		t.Fatalf("read %+v %v", file, err)
	}
	written, err := c.Agents.Memory.Write(ctx, "dev", "tone.md", "Formal.", &AgentMemoryFileOptions{Scope: "customer:42"})
	if err != nil || !written.Created || written.Scope != "customer:42" {
		t.Fatalf("write %+v %v", written, err)
	}
	deleted, err := c.Agents.Memory.Delete(ctx, "dev", "notes/tea.md", nil)
	if err != nil || !deleted {
		t.Fatalf("delete %v %v", deleted, err)
	}
	want := []string{
		"GET /v1/agents/dev/memory?limit=1&prefix=notes%2F",
		"GET /v1/agents/dev/memory?cursor=page-2&limit=1&prefix=notes%2F",
		"GET /v1/agents/dev/memory?q=tea&scope=",
		"GET /v1/agents/dev/memory/content?path=notes%2Ftea.md&scope=",
		"PUT /v1/agents/dev/memory/content?path=tone.md&scope=customer%3A42",
		"DELETE /v1/agents/dev/memory/content?path=notes%2Ftea.md&scope=",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("routes\n got %q\nwant %q", seen, want)
	}
	if !reflect.DeepEqual(bodies[4], map[string]any{"content": "Formal."}) {
		t.Fatalf("write body %v", bodies[4])
	}
}
