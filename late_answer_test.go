package withruntime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// An answer the API sent after holding its headers for 90 s comes as a 200
// marked runtime-late-answer, then its JSON: the answer, or {"error": ...}
// when the work failed after the status went. A failure is an error, never a
// success with an empty result.

func lateClient(t *testing.T, body string) (*Client, *tries) {
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{
			"Content-Type": {"application/json"}, "Runtime-Late-Answer": {"true"},
		}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	}}
	return neverSentClient(t, rt), rt
}

func TestALateFailureIsAnError(t *testing.T) {
	c, _ := lateClient(t, "\n\n"+`{"error":{"code":"sandbox_lost","status":409,"message":"The sandbox's host was lost."}}`)
	sandbox, err := c.Sandboxes.Get(context.Background(), "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "sandbox_lost" || failure.Status != 409 {
		t.Fatalf("want the late sandbox_lost as an error, got %+v, %v", sandbox, err)
	}
}

func TestALateFailureWithoutAStatusIs500(t *testing.T) {
	c, _ := lateClient(t, `{"error":{"code":"internal","message":"x"}}`)
	_, err := c.Sandboxes.Get(context.Background(), "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.Status != 500 || failure.Code != "internal" {
		t.Fatalf("got %v", err)
	}
}

func TestALateRetryableFailureIsSentAgainUnderItsKey(t *testing.T) {
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if n > 1 {
			return nil, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Runtime-Late-Answer": {"true"}},
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"bad_gateway","status":502,"message":"x"}}`)), Request: r}, nil
	}}
	c := neverSentClient(t, rt)
	if _, err := c.Sandboxes.Create(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(rt.keys) != 2 || rt.keys[0] == "" || rt.keys[0] != rt.keys[1] {
		t.Fatalf("want two tries under one key, got %q", rt.keys)
	}
}

func TestALateBodyThatIsNotJSONIsAnError(t *testing.T) {
	c, _ := lateClient(t, "\n\n")
	_, err := c.Sandboxes.Get(context.Background(), "sbx-1")
	var failure *Error
	if !errors.As(err, &failure) || failure.Status != 502 {
		t.Fatalf("got %v", err)
	}
}

func TestALateAnswerIsTheAnswer(t *testing.T) {
	// An answer may carry an error of its own beside other fields.
	c, _ := lateClient(t, "\n"+`{"id":"sbx-1","kind":"sandbox","state":"running","error":{"code":"x"}}`)
	sandbox, err := c.Sandboxes.Get(context.Background(), "sbx-1")
	if err != nil || sandbox.ID() != "sbx-1" {
		t.Fatalf("got %+v, %v", sandbox, err)
	}
}
