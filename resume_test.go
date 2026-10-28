package withruntime

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

/* The API's hand-off (packages/cloud/src/api/tunnels.ts, "Hand-off"), played
   by a fake server: restart, the client's answer, resume with a token and
   replay, close 1012; then the reconnect with ?resume=<token>. What the caller
   reads and writes continues across it, nothing lost or repeated. */

type handoffServer struct {
	t       *testing.T
	mu      sync.Mutex
	opened  []string // each upgrade's query
	got     []string // what the second socket received, in order
	refuse  int      // answer the resume with this status instead
	noToken bool     // close 1012 without a resume message
	silent  bool     // accept the resume and never answer
}

func (h *handoffServer) serve(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.opened = append(h.opened, r.URL.RawQuery)
	refuse := h.refuse
	h.mu.Unlock()
	resume := r.URL.Query().Get("resume")
	if resume != "" && refuse != 0 {
		http.Error(w, `{"error":{"code":"resume_refused","message":"no"}}`, refuse)
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
	socket := &websocket{conn: conn, reader: bufio.NewReader(conn)}
	if resume == "" {
		h.first(conn, socket, strings.HasSuffix(r.URL.Path, "/terminal"))
		return
	}
	if h.silent {
		_, _ = io.Copy(io.Discard, conn)
		return
	}
	if resume != "token-1" {
		h.t.Errorf("resumed with %q", resume)
		return
	}
	_ = serverFrame(conn, opText, []byte(`{"type":"ready","resumed":true}`))
	for {
		op, payload, err := socket.read()
		if err != nil {
			return
		}
		h.mu.Lock()
		h.got = append(h.got, fmt.Sprintf("%d %s", op, payload))
		h.mu.Unlock()
		if op == opBinary && string(payload) == "after" && strings.HasSuffix(r.URL.Path, "/terminal") {
			_ = serverFrame(conn, opBinary, []byte("output 2"))
		}
	}
}

// first serves the socket the restart hands off: the restart, output and,
// once the client answered, the token and close 1012.
func (h *handoffServer) first(conn net.Conn, socket *websocket, terminal bool) {
	_ = serverFrame(conn, opText, []byte(`{"type":"ready","processId":"proc-1"}`))
	// Sent after the restart, so a caller that read it is held from then on.
	_ = serverFrame(conn, opText, []byte(`{"type":"restart"}`))
	if terminal {
		_ = serverFrame(conn, opBinary, []byte("output 1"))
	}
	for {
		op, payload, err := socket.read()
		if err != nil {
			return
		}
		if op == opText && string(payload) == `{"type":"restart"}` {
			break
		}
		h.mu.Lock()
		h.got = append(h.got, fmt.Sprintf("first %d %s", op, payload))
		h.mu.Unlock()
	}
	if !h.noToken {
		_ = serverFrame(conn, opText, []byte(`{"type":"resume","token":"token-1","replay":"dW5zZW50"}`))
	}
	_ = serverFrame(conn, opClose, binary.BigEndian.AppendUint16(nil, serviceRestart))
}

func handoffClient(t *testing.T, h *handoffServer) *Sandbox {
	h.t = t
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	c, err := New(WithAPIKey(testKey), WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return newSandbox(c, SandboxInfo{ID: "sandbox"})
}

func readString(t *testing.T, terminal *Terminal, want string) {
	t.Helper()
	buffer := make([]byte, len(want))
	if _, err := io.ReadFull(terminal, buffer); err != nil || string(buffer) != want {
		t.Fatalf("read %q, %v; want %q", buffer, err, want)
	}
}

func TestTerminalResumesAcrossARestart(t *testing.T) {
	t.Parallel()
	h := &handoffServer{}
	terminal, err := handoffClient(t, h).Terminal(context.Background(), &TerminalOptions{Cols: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	readString(t, terminal, "output 1")
	// Typed while the socket is handed off: held, then sent after the replay.
	if _, err := terminal.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	readString(t, terminal, "output 2")
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.opened) != 2 || !strings.Contains(h.opened[0], "cols=100") || h.opened[1] != "resume=token-1" {
		t.Fatalf("opened %q", h.opened)
	}
	want := []string{`1 {"data":"dW5zZW50","type":"replay"}`, "2 after"}
	if strings.Join(h.got, "|") != strings.Join(want, "|") {
		t.Fatalf("the resumed socket got %q, want %q", h.got, want)
	}
	terminal.socket.mu.Lock()
	resumes, replays := terminal.socket.resumes, terminal.socket.replays
	terminal.socket.mu.Unlock()
	if terminal.ProcessID() != "proc-1" || resumes != 1 || replays != 1 {
		t.Fatalf("process %q, resumes %d, replays %d", terminal.ProcessID(), resumes, replays)
	}
}

func TestTunnelResumesAcrossARestart(t *testing.T) {
	t.Parallel()
	h := &handoffServer{}
	tunnel, err := handoffClient(t, h).OpenTunnel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tunnel.Close()
	// Once the restart reached the tunnel: held while paused, else sent after
	// the replay; the same either way.
	deadline := time.Now().Add(5 * time.Second)
	for {
		tunnel.socket.mu.Lock()
		handed := tunnel.socket.paused || tunnel.socket.resumes > 0
		tunnel.socket.mu.Unlock()
		if handed || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := tunnel.socket.write(opBinary, []byte("after")); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		h.mu.Lock()
		got := strings.Join(h.got, "|")
		h.mu.Unlock()
		if got == `1 {"data":"dW5zZW50","type":"replay"}|2 after` {
			select {
			case <-tunnel.done:
				t.Fatalf("the tunnel ended: %v", tunnel.err)
			default:
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the resumed tunnel got %q", h.got)
}

func TestARestartWithoutATokenEndsTheTerminalAsAClose(t *testing.T) {
	t.Parallel()
	h := &handoffServer{noToken: true}
	terminal, err := handoffClient(t, h).Terminal(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	readString(t, terminal, "output 1")
	if _, err := terminal.Read(make([]byte, 8)); err != io.EOF {
		t.Fatalf("read after the close: %v, want io.EOF", err)
	}
	if len(h.opened) != 1 {
		t.Fatalf("reconnected without a token: %q", h.opened)
	}
}

func TestARefusedResumeIsNotRetried(t *testing.T) {
	t.Parallel()
	h := &handoffServer{refuse: http.StatusConflict}
	terminal, err := handoffClient(t, h).Terminal(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	readString(t, terminal, "output 1")
	started := time.Now()
	if _, err := terminal.Read(make([]byte, 8)); err != io.EOF {
		t.Fatalf("read after the refusal: %v, want io.EOF", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.opened) != 2 || time.Since(started) > 2*time.Second {
		t.Fatalf("opened %q in %s", h.opened, time.Since(started))
	}
	// Ended for good: a write fails rather than wait for a resume.
	if _, err := terminal.Write([]byte("late")); err == nil {
		t.Fatal("a write after the session ended succeeded")
	}
}

func TestCloseDuringAResumeEndsIt(t *testing.T) {
	t.Parallel()
	h := &handoffServer{silent: true}
	terminal, err := handoffClient(t, h).Terminal(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	readString(t, terminal, "output 1")
	read := make(chan error, 1)
	go func() {
		_, err := terminal.Read(make([]byte, 8))
		read <- err
	}()
	// Once the reconnect is open and waiting for an answer that never comes.
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		opened := len(h.opened)
		h.mu.Unlock()
		if opened == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	_ = terminal.Close()
	select {
	case err := <-read:
		if err != io.EOF {
			t.Fatalf("read after Close: %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close left the resume waiting")
	}
}
