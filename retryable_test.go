package withruntime

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// The answer's own `retryable` decides the general retry; a wait the server
// names past a minute is returned at once; an answer that ran nothing is sent
// again past maxRetries until the call's deadline.

func TestAServerSaysNotRetryableOnA503AndItIsNotRetried(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		apiError(w, 503, "host_unavailable", map[string]any{"retryable": false})
	})
	_, err := s.client(t).Sandboxes.Create(context.Background(), nil)
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "host_unavailable" || failure.Retryable() ||
		failure.ServerRetryable == nil || len(s.requests) != 1 {
		t.Fatalf("want one try and a non-retryable error, got %v after %d", err, len(s.requests))
	}
}

func TestAServerSaysRetryableOnA409AndItIsSentAgainAfterRetryAfterUnderItsKey(t *testing.T) {
	var first time.Time
	var gap time.Duration
	var s *server
	s = newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if n == 1 {
			first = s.now()
			w.Header().Set("Retry-After", "0.2")
			apiError(w, 409, "snapshot_not_ready", map[string]any{"retryable": true})
			return
		}
		gap = s.now().Sub(first)
		writeJSON(w, 200, running)
	})
	if _, err := s.client(t).Sandboxes.Create(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	key := s.requests[0].Header.Get("Idempotency-Key")
	if len(s.requests) != 2 || key == "" || s.requests[1].Header.Get("Idempotency-Key") != key {
		t.Fatalf("want two tries under one key, got %d", len(s.requests))
	}
	if gap < 170*time.Millisecond {
		t.Fatalf("sent again after %v, before Retry-After", gap)
	}
}

func TestADailyCapsHourIsReturnedAtOnce(t *testing.T) {
	for _, header := range []bool{true, false} {
		s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
			extra := map[string]any{"retryable": true}
			if header {
				w.Header().Set("Retry-After", "3600")
			} else {
				extra["retryAfterMs"] = 3_600_000
			}
			apiError(w, 429, "no_credit_daily_limit", extra)
		})
		started := time.Now()
		_, err := s.client(t).Sandboxes.Get(context.Background(), "sbx-1")
		var failure *Error
		if !errors.As(err, &failure) || !failure.Retryable() || failure.RetryAfter != time.Hour ||
			len(s.requests) != 1 || time.Since(started) > 5*time.Second {
			t.Fatalf("header %v: want the cap at once, got %v after %d", header, err, len(s.requests))
		}
	}
	// A refusal that ran nothing, with a long wait, is not slept on either.
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Retry-After", "120")
		apiError(w, 429, "rate_limited", nil)
	})
	_, err := s.client(t).Sandboxes.Get(context.Background(), "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.RetryAfter != 2*time.Minute || len(s.requests) != 1 {
		t.Fatalf("want rate_limited at once, got %v after %d", err, len(s.requests))
	}
}

func TestWithoutRetryableTheStatusRuleStandsAndNewSwitchedOffCodesFailAtOnce(t *testing.T) {
	t.Parallel()
	for _, code := range []string{"feature_unavailable", "delete_unavailable", "switch_image_unavailable",
		"switching_paused", "schema_migration_required", "storage_unavailable"} {
		s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
			apiError(w, 503, code, nil)
		})
		_, err := s.client(t).Sandboxes.Get(context.Background(), "sbx-1")
		var failure *Error
		if !errors.As(err, &failure) || failure.Retryable() || len(s.requests) != 1 {
			t.Fatalf("%s: want one try, got %d", code, len(s.requests))
		}
	}
	// No retryable (or one that is not a boolean): a 502 is still retried.
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		if n == 1 {
			apiError(w, 502, "bad_gateway", map[string]any{"retryable": "yes", "retryAfterMs": 5})
			return
		}
		writeJSON(w, 200, running)
	})
	if _, err := s.client(t).Sandboxes.Get(context.Background(), "sbx-1"); err != nil || len(s.requests) != 2 {
		t.Fatalf("want a second try, got %v after %d", err, len(s.requests))
	}
}

func TestNothingRanIsSentAgainPastMaxRetriesUntilTheDeadline(t *testing.T) {
	for _, refusal := range []struct {
		status int
		code   string
	}{{429, "guest_busy"}, {503, "busy"}, {429, "rate_limited"}} {
		s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
			if n <= 6 {
				apiError(w, refusal.status, refusal.code, map[string]any{"retryAfterMs": 1, "retryable": true})
				return
			}
			writeJSON(w, 200, running)
		})
		if _, err := s.client(t, WithMaxRetries(0)).Sandboxes.Create(context.Background(), &CreateOptions{IdempotencyKey: "k-1"}); err != nil {
			t.Fatal(refusal.code, err)
		}
		if len(s.requests) != 7 || s.requests[6].Header.Get("Idempotency-Key") != "k-1" {
			t.Fatalf("%s: want 7 tries under one key, got %d", refusal.code, len(s.requests))
		}
	}
	// Until the deadline: then the refusal stands, not a timeout.
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		apiError(w, 429, "guest_busy", map[string]any{"retryAfterMs": 20})
	})
	started := time.Now()
	_, err := s.client(t, WithMaxRetries(0), WithTimeout(400*time.Millisecond)).Sandboxes.Get(context.Background(), "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "guest_busy" || len(s.requests) < 3 || time.Since(started) > 2*time.Second {
		t.Fatalf("want guest_busy at the deadline, got %v after %d", err, len(s.requests))
	}
}

func TestALateBusyIsNotSentAgainButALateGuestBusyIs(t *testing.T) {
	t.Parallel()
	c, rt := lateClient(t, `{"error":{"code":"busy","status":503,"message":"x","retryAfterMs":1,"retryable":false}}`)
	if _, err := c.Sandboxes.Get(context.Background(), "sbx-1"); err == nil || len(rt.keys) != 1 {
		t.Fatalf("want one try for a late busy, got %d", len(rt.keys))
	}
	c, rt = lateClient(t, `{"error":{"code":"guest_busy","status":429,"message":"x","retryAfterMs":1}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := c.Sandboxes.Get(ctx, "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "guest_busy" || len(rt.keys) < 3 {
		t.Fatalf("want a late guest_busy sent again, got %v after %d", err, len(rt.keys))
	}
}
