package withruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

const bookedJSON = `{"id":"b1","status":"declined","reason":"room","message":"No room.","region":"r1","sandboxes":500,` +
	`"vcpu":2,"memoryMiB":4096,"diskMiB":4096,"startsAt":"2026-10-18T10:00:00.000Z","endsAt":"2026-10-18T12:00:00.000Z",` +
	`"heldUntil":null,"phase":null,"largestSandboxes":120,"nearestWindow":{"startsAt":"2026-10-18T11:00:00.000Z","endsAt":"2026-10-18T13:00:00.000Z"},` +
	`"estimateMicros":"12500000","unusedRateMicros":"15000","unusedMaxMicros":"3000000","unusedChargedMicros":"0","inUse":0,"peakInUse":0,"releasedPercent":0,"createdAt":"2026-10-17T10:00:00.000Z","cancelledAt":null}`

func TestReservationsCreateSendsTheWindowAndAKeyAndADeclineIsNotAnError(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(bookedJSON))
	})
	c := s.client(t)
	start := time.Date(2026, 10, 18, 4, 0, 0, 0, time.FixedZone("MDT", -6*3600))
	booking, err := c.Reservations.Create(context.Background(), ReservationCreate{
		Sandboxes: 500, StartsAt: start, EndsAt: start.Add(2 * time.Hour), MemoryMiB: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	if booking.Status != "declined" || booking.Reason == nil || *booking.Reason != "room" || booking.LargestSandboxes != 120 ||
		booking.NearestWindow == nil || booking.NearestWindow.StartsAt != "2026-10-18T11:00:00.000Z" ||
		booking.EstimateMicros != "12500000" || booking.HeldUntil != nil || booking.Phase != nil {
		t.Fatalf("booking = %+v", booking)
	}
	request := s.requests[0]
	if request.Method != "POST" || request.URL.Path != "/v1/reservations" || request.Header.Get("Idempotency-Key") == "" {
		t.Fatalf("request = %s %s key %q", request.Method, request.URL.Path, request.Header.Get("Idempotency-Key"))
	}
	var sent map[string]any
	_ = json.Unmarshal(s.bodies[0], &sent)
	want := map[string]any{"sandboxes": float64(500), "startsAt": "2026-10-18T10:00:00Z", "endsAt": "2026-10-18T12:00:00Z", "memoryMiB": float64(8192)}
	if len(sent) != len(want) {
		t.Fatalf("sent = %v", sent)
	}
	for k, v := range want {
		if sent[k] != v {
			t.Fatalf("sent[%s] = %v, want %v", k, sent[k], v)
		}
	}
}

func TestReservationsListGetAndCancel(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/reservations" {
			_, _ = w.Write([]byte(`{"items":[` + bookedJSON + `],"nextCursor":"c2"}`))
			return
		}
		_, _ = w.Write([]byte(bookedJSON))
	})
	c := s.client(t)
	ctx := context.Background()
	page, err := c.Reservations.List(ctx, ListReservations{Limit: 5, Cursor: "c1"})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != "b1" || page.NextCursor == nil || *page.NextCursor != "c2" {
		t.Fatalf("page = %+v, %v", page, err)
	}
	if got := s.requests[0].URL.RawQuery; got != "cursor=c1&limit=5" {
		t.Fatalf("query = %q", got)
	}
	if _, err := c.Reservations.Get(ctx, "b 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reservations.Cancel(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if got := s.requests[1].Method + " " + s.requests[1].URL.EscapedPath(); got != "GET /v1/reservations/b%201" {
		t.Fatalf("get = %q", got)
	}
	if got := s.requests[2].Method + " " + s.requests[2].URL.Path; got != "DELETE /v1/reservations/b1" {
		t.Fatalf("cancel = %q", got)
	}
}

func TestReservationsKeepHoldsStandingRoomAndUpdateChangesIt(t *testing.T) {
	t.Parallel()
	standing := `{"id":"b1","status":"confirmed","standing":true,"snapshots":["s1"],"forKey":true,"endsAt":null,` +
		`"heldUntil":null,"unusedMaxMicros":null,"unusedPerHourMicros":"75000","sandboxes":40,"inUse":36}`
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(standing))
	})
	c := s.client(t)
	ctx := context.Background()
	booking, err := c.Reservations.Keep(ctx, ReservationKeep{Sandboxes: 40, Snapshots: []string{"s1"}, ForKey: true})
	if err != nil {
		t.Fatal(err)
	}
	if !booking.Standing || !booking.ForKey || len(booking.Snapshots) != 1 || booking.EndsAt != "" || booking.HeldUntil != nil ||
		booking.UnusedPerHourMicros != "75000" || booking.InUse != 36 {
		t.Fatalf("booking = %+v", booking)
	}
	if _, err := c.Reservations.Update(ctx, "b1", ReservationUpdate{Snapshots: []string{"s2"}}); err != nil {
		t.Fatal(err)
	}
	var kept, changed map[string]any
	_ = json.Unmarshal(s.bodies[0], &kept)
	_ = json.Unmarshal(s.bodies[1], &changed)
	if kept["standing"] != true || kept["sandboxes"] != float64(40) || kept["forKey"] != true || len(kept["snapshots"].([]any)) != 1 {
		t.Fatalf("kept = %v", kept)
	}
	if got := s.requests[1].Method + " " + s.requests[1].URL.Path; got != "POST /v1/reservations/b1:update" {
		t.Fatalf("update = %q", got)
	}
	if len(changed) != 1 || len(changed["snapshots"].([]any)) != 1 || s.requests[0].Header.Get("Idempotency-Key") == "" {
		t.Fatalf("changed = %v", changed)
	}
}
