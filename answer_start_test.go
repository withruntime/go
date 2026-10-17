package withruntime

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// The controller failover drill, 4 October 2026: a call in flight when the
// controller died waited on a silent connection until its deadline. An
// attempt whose answer has not started by answerStart is sent again under
// the same key.
func TestAnAttemptWhoseAnswerNeverStartsIsSentAgainUnderItsKey(t *testing.T) {
	previous := answerStart
	answerStart = 300 * time.Millisecond
	t.Cleanup(func() { answerStart = previous })
	release := make(chan struct{})
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if n == 1 {
			select { // a server that is gone: the connection stays, silent
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	t.Cleanup(func() { close(release) })
	c := s.client(t, WithTimeout(5*time.Second))
	started := time.Now()
	var out map[string]any
	if err := c.Request(context.Background(), http.MethodPost, "/v1/feedback", &RequestOptions{}, &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true {
		t.Fatalf("answer %v", out)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) != 2 {
		t.Fatalf("%d requests, want 2", len(s.requests))
	}
	first, second := s.requests[0].Header.Get("Idempotency-Key"), s.requests[1].Header.Get("Idempotency-Key")
	if first == "" || first != second {
		t.Fatalf("keys %q and %q", first, second)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("took %v", took)
	}
}
