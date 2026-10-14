package withruntime

import (
	"context"
	"net/http"
	"testing"
)

func TestVolumeResizeWaitsKeepsItsKeyAndReadsProgress(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, 200, map[string]any{"id": "v", "sizeMiB": 1024, "resize": map[string]any{
			"id": "r", "operationId": "op", "sizeMiB": 2048, "state": "running", "generation": 1, "error": nil,
		}})
	})
	c := s.client(t)
	ctx := context.Background()
	v, err := c.Volumes.Resize(ctx, "v/1", ResizeVolumeOptions{SizeMiB: 2048, IdempotencyKey: "grow-1"})
	if err != nil {
		t.Fatal(err)
	}
	if v.SizeMiB != 1024 || v.Resize == nil || v.Resize.State != "running" || v.Resize.SizeMiB != 2048 {
		t.Fatalf("progress lost: %+v", v.Resize)
	}
	zero := 0
	if _, err := c.Volumes.Resize(ctx, "v/1", ResizeVolumeOptions{SizeMiB: 2048, Wait: &zero}); err != nil {
		t.Fatal(err)
	}
	first, second := s.requests[0], s.requests[1]
	if first.URL.EscapedPath() != "/v1/volumes/v%2F1:resize" || string(s.bodies[0]) != `{"sizeMiB":2048}` {
		t.Fatalf("got %s %s", first.URL.EscapedPath(), s.bodies[0])
	}
	if first.Header.Get("Prefer") != "wait=10" || first.Header.Get("Idempotency-Key") != "grow-1" {
		t.Fatalf("headers %v", first.Header)
	}
	if second.Header.Get("Prefer") == "wait=10" || second.Header.Get("Idempotency-Key") == "" {
		t.Fatalf("wait override or generated key lost: %v", second.Header)
	}
}
