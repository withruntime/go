package withruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// A request whose connection never opened is sent again at once; one that
// may have reached a server keeps the backoff (11 October 2026). Each attempt
// goes through a RoundTripper that fails the first tries as asked, on real
// loopback sockets, and answers ok after.

type tries struct {
	mu   sync.Mutex
	at   []time.Time
	keys []string
	fail func(n int, r *http.Request) (*http.Response, error)
}

func (t *tries) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.at = append(t.at, time.Now())
	t.keys = append(t.keys, r.Header.Get("Idempotency-Key"))
	n := len(t.at)
	t.mu.Unlock()
	if response, err := t.fail(n, r); response != nil || err != nil {
		return response, err
	}
	body := `{"id":"sbx-1","kind":"sandbox","state":"running","status":"active","vcpu":2}`
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func (t *tries) gap(i int) time.Duration { return t.at[i].Sub(t.at[i-1]) }

func closedAddress(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}

// realDial sends the request to address with the default transport.
func realDial(address string, r *http.Request) (*http.Response, error) {
	moved := r.Clone(r.Context())
	moved.URL.Host = address
	moved.Host = address
	return http.DefaultTransport.RoundTrip(moved)
}

func neverSentClient(t *testing.T, rt http.RoundTripper) *Client {
	c, err := New(WithAPIKey(testKey), WithBaseURL("http://localhost"), WithHTTPClient(&http.Client{Transport: rt}))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestARefusedConnectionIsSentAgainAtOnceWithItsKey(t *testing.T) {
	closed := closedAddress(t)
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if n == 1 {
			return realDial(closed, r)
		}
		return nil, nil
	}}
	_, err := neverSentClient(t, rt).Sandboxes.Create(context.Background(), &CreateOptions{Funding: "trial"})
	if err != nil {
		t.Fatal(err)
	}
	// Before 11 October 2026 the first pause was backoff(0), 125 to 375 ms.
	if len(rt.at) != 2 || rt.gap(1) > 100*time.Millisecond {
		t.Fatalf("want a second try within 100 ms, got %d tries, gap %v", len(rt.at), rt.gap(1))
	}
	if rt.keys[0] == "" || rt.keys[0] != rt.keys[1] {
		t.Fatalf("keys %v", rt.keys)
	}
}

func TestASecondRefusalInARowWaitsTheBackoff(t *testing.T) {
	closed := closedAddress(t)
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if n <= 2 {
			return realDial(closed, r)
		}
		return nil, nil
	}}
	if _, err := neverSentClient(t, rt).Sandboxes.Create(context.Background(), &CreateOptions{Funding: "trial"}); err != nil {
		t.Fatal(err)
	}
	if rt.gap(1) > 100*time.Millisecond || rt.gap(2) < 240*time.Millisecond {
		t.Fatalf("gaps %v then %v", rt.gap(1), rt.gap(2))
	}
}

func TestAResetAfterTheRequestWasSentKeepsTheBackoff(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			buffer := make([]byte, 65536)
			_, _ = conn.Read(buffer)
			conn.(*net.TCPConn).SetLinger(0)
			conn.Close()
		}
	}()
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if n == 1 {
			return realDial(l.Addr().String(), r)
		}
		return nil, nil
	}}
	if _, err := neverSentClient(t, rt).Sandboxes.Create(context.Background(), &CreateOptions{Funding: "trial"}); err != nil {
		t.Fatal(err)
	}
	if rt.gap(1) < 120*time.Millisecond || rt.keys[0] != rt.keys[1] {
		t.Fatalf("gap %v keys %v", rt.gap(1), rt.keys)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestATimeoutKeepsTheBackoff(t *testing.T) {
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if n == 1 {
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}}
		}
		return nil, nil
	}}
	if _, err := neverSentClient(t, rt).Sandboxes.Create(context.Background(), &CreateOptions{Funding: "trial"}); err != nil {
		t.Fatal(err)
	}
	if rt.gap(1) < 120*time.Millisecond {
		t.Fatalf("gap %v", rt.gap(1))
	}
	if neverSent(&net.OpError{Op: "read", Err: timeoutError{}}) || neverSent(errors.New("ECONNREFUSED")) {
		t.Fatal("a timeout or a bare message is not a connection that never opened")
	}
}
