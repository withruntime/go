package withruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
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

func TestParityWatchResumeNoticesPollingAndStop(t *testing.T) {
	reads := 0
	stops := 0
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		switch {
		case r.Method == "POST":
			writeJSON(w, 200, map[string]any{"id": "watch", "path": "/workspace", "cursor": 10})
		case r.Method == "DELETE":
			stops++
			writeJSON(w, 200, map[string]any{"stopped": true})
		case r.URL.Query().Get("follow") == "true":
			reads++
			w.Header().Set("Content-Type", "application/x-ndjson")
			if reads == 1 {
				if r.URL.Query().Get("cursor") != "10" {
					t.Error("initial cursor")
				}
				fmt.Fprintln(w, `{"k":"events","events":[{"type":"write","path":"/workspace/a","isDir":false}],"cursor":20}`)
				fmt.Fprintln(w, `{"k":"overflow","dropped":2,"reason":"rate","cursor":25}`)
				fmt.Fprintln(w, `{"k":"continue","cursor":30}`)
			} else if reads == 2 {
				if r.URL.Query().Get("cursor") != "30" {
					t.Error("resume cursor")
				}
				fmt.Fprintln(w, `{"k":"paused","cursor":40}`)
			} else {
				if r.URL.Query().Get("cursor") != "40" {
					t.Error("pause cursor")
				}
				fmt.Fprintln(w, `{"k":"end","reason":"timeout","cursor":50}`)
			}
		default:
			writeJSON(w, 200, map[string]any{"events": []any{}, "nextCursor": 51, "ended": true})
		}
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sandbox"})
	watch, err := sbx.Files.Watch(context.Background(), "/workspace", &WatchOptions{Recursive: true, Exclude: []string{"node_modules"}})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for event, err := range watch.Events(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, event.Kind)
	}
	if strings.Join(kinds, ",") != "events,overflow,paused" || watch.Cursor() != 40 {
		t.Fatalf("events %v cursor %d", kinds, watch.Cursor())
	}
	for event, err := range watch.Events(context.Background()) {
		if err != nil || event.Kind != "end" {
			t.Fatalf("resumed %v %v", event, err)
		}
	}
	page, err := sbx.Files.Watches.Read(context.Background(), watch.ID, watch.Cursor(), 0)
	if err != nil || page.NextCursor != 51 || !page.Ended {
		t.Fatalf("poll %v %v", page, err)
	}
	if err := watch.Stop(context.Background()); err != nil || stops != 1 {
		t.Fatalf("stop %v", err)
	}
}

func TestParityWatchStopClosesTheActiveResponse(t *testing.T) {
	reading := make(chan struct{})
	ended := make(chan struct{})
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		switch r.Method {
		case "POST":
			writeJSON(w, 200, map[string]any{"id": "watch", "path": "/workspace"})
		case "DELETE":
			writeJSON(w, 200, map[string]any{"stopped": true})
		default:
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			close(reading)
			<-r.Context().Done()
			close(ended)
		}
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sandbox"})
	watch, err := sbx.Files.Watch(context.Background(), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		for range watch.Events(context.Background()) {
		}
		close(finished)
	}()
	<-reading
	if err := watch.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("response leaked")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("reader leaked")
	}
}

func serverFrame(conn net.Conn, opcode byte, data []byte) error {
	head := []byte{0x80 | opcode}
	switch {
	case len(data) < 126:
		head = append(head, byte(len(data)))
	case len(data) < 65536:
		head = append(head, 126, byte(len(data)>>8), byte(len(data)))
	default:
		head = append(head, 127)
		head = binary.BigEndian.AppendUint64(head, uint64(len(data)))
	}
	_, err := conn.Write(append(head, data...))
	return err
}
func tunnelServer(t *testing.T, mode string) (*Client, <-chan struct{}) {
	t.Helper()
	closed := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testKey || !strings.HasSuffix(r.URL.Path, "/tunnel") {
			t.Error("bad tunnel authorization or path")
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { conn.Close(); closed <- struct{}{} }()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
		if mode == "quiet" {
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		if mode == "refuse" {
			_ = serverFrame(conn, opText, []byte(`{"type":"error","error":{"code":"forbidden","message":"no grant"}}`))
			return
		}
		_ = serverFrame(conn, opText, []byte(`{"type":"ready"}`))
		socket := &websocket{conn: conn, reader: bufio.NewReader(conn)}
		var credited uint64
		for {
			op, frame, err := socket.read()
			if err != nil {
				return
			}
			if op != opBinary || len(frame) < 5 {
				return
			}
			kind, id, data := frame[0], binary.BigEndian.Uint32(frame[1:]), frame[5:]
			switch kind {
			case 'o':
				if string(data) != "tcp 8080" {
					_ = serverFrame(conn, opBinary, tunnelFrame('c', id, []byte("closed")))
				} else {
					_ = serverFrame(conn, opBinary, tunnelFrame('o', id, nil))
				}
			case 'd':
				credited += uint64(len(data))
				if mode != "no-credit" {
					_ = serverFrame(conn, opBinary, tunnelFrame('a', 0, binary.BigEndian.AppendUint64(nil, credited)))
					_ = serverFrame(conn, opBinary, tunnelFrame('d', id, data))
				}
			case 'e':
				_ = serverFrame(conn, opBinary, tunnelFrame('e', id, nil))
				_ = serverFrame(conn, opBinary, tunnelFrame('c', id, []byte("done")))
			case 'c':
			}
		}
	}))
	t.Cleanup(server.Close)
	c, err := New(WithAPIKey(testKey), WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return c, closed
}

func TestParityTunnelMultiplexFlowControlHalfCloseAndRefusal(t *testing.T) {
	c, _ := tunnelServer(t, "echo")
	sbx := newSandbox(c, SandboxInfo{ID: "sandbox"})
	tunnel, err := sbx.OpenTunnel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tunnel.Close()
	bad, err := tunnel.Connect(context.Background(), 1)
	if bad != nil || err == nil {
		t.Fatal("closed port accepted")
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		stream, err := tunnel.Connect(context.Background(), 8080)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer stream.Close()
			// The v1 protocol has no per-stream receive window, so a stream
			// whose unread echo passes tunnelWindow fails by design whenever
			// the reader is descheduled. Each stream stays under one window,
			// and the two together pass it, so the writers still wait for
			// credit on the shared send window.
			payload := bytes.Repeat([]byte("abcdef"), (tunnelWindow-4096)/6)
			if 2*len(payload) <= tunnelWindow {
				t.Error("the streams no longer exceed the send window together")
			}
			writing := make(chan error, 1)
			go func() {
				_, e := stream.Write(payload)
				if e == nil {
					e = stream.CloseWrite()
				}
				writing <- e
			}()
			got, e := io.ReadAll(stream)
			if e != nil || !bytes.Equal(got, payload) {
				t.Errorf("stream lost bytes %d/%d %v", len(got), len(payload), e)
			}
			if e := <-writing; e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
}
func TestParityTunnelCancellationClosesQuietHandshakeAndBlockedWriter(t *testing.T) {
	for _, mode := range []string{"quiet", "no-credit"} {
		t.Run(mode, func(t *testing.T) {
			c, closed := tunnelServer(t, mode)
			sbx := newSandbox(c, SandboxInfo{ID: "sandbox"})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				tun, e := sbx.OpenTunnel(ctx)
				if e != nil {
					finished <- e
					return
				}
				defer tun.Close()
				stream, e := tun.Connect(ctx, 8080)
				if e != nil {
					finished <- e
					return
				}
				_, e = stream.Write(make([]byte, 2*tunnelWindow))
				finished <- e
			}()
			time.Sleep(30 * time.Millisecond)
			cancel()
			select {
			case e := <-finished:
				if e == nil {
					t.Fatal("cancellation accepted whole blocked write")
				}
			case <-time.After(time.Second):
				t.Fatal("cancel hung")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("socket leaked")
			}
		})
	}
}
func TestParityPortForwardPreservesResponseAfterClientHalfClose(t *testing.T) {
	c, closed := tunnelServer(t, "echo")
	sbx := newSandbox(c, SandboxInfo{ID: "sandbox"})
	forward, err := sbx.PortForward(context.Background(), 8080, "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", forward.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("hello"), 20000)
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = conn.(*net.TCPConn).CloseWrite()
	got, err := io.ReadAll(conn)
	conn.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("forward: %d bytes %v", len(got), err)
	}
	if err := forward.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("forward tunnel leaked")
	}
	if conn, err := net.DialTimeout("tcp", forward.Addr().String(), 50*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("listener still open")
	}
}
func TestParityTunnelPreservesStructuredRefusal(t *testing.T) {
	c, _ := tunnelServer(t, "refuse")
	_, err := newSandbox(c, SandboxInfo{ID: "sandbox"}).OpenTunnel(context.Background())
	failure, ok := asError(err)
	if !ok || failure.Code != "forbidden" {
		t.Fatalf("lost error %v", err)
	}
}

func TestParityWatchCannotResumeWhileStopIsInFlight(t *testing.T) {
	deleting := make(chan struct{})
	release := make(chan struct{})
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == "POST" {
			writeJSON(w, 200, map[string]any{"id": "watch", "path": "/workspace"})
			return
		}
		if r.Method == "DELETE" {
			close(deleting)
			<-release
			writeJSON(w, 200, map[string]any{"stopped": true})
			return
		}
		t.Error("watch resumed during stop")
		fmt.Fprintln(w, `{"k":"end","cursor":1}`)
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sandbox"})
	watch, err := sbx.Files.Watch(context.Background(), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- watch.Stop(context.Background()) }()
	<-deleting
	for _, err := range watch.Events(context.Background()) {
		if err == nil {
			t.Error("reader accepted while stopping")
		}
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := watch.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestParitySocketCloseUnblocksAnInFlightWrite(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	socket := &websocket{conn: local, reader: bufio.NewReader(local)}
	writing := make(chan error, 1)
	go func() { writing <- socket.write(opBinary, []byte("blocked")) }()
	time.Sleep(10 * time.Millisecond)
	closed := make(chan struct{})
	go func() { socket.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited on blocked write")
	}
	select {
	case err := <-writing:
		if err == nil {
			t.Fatal("blocked writer succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("write leaked")
	}
}

func TestParityUnreadTunnelStreamDoesNotBlockControlOrCancellation(t *testing.T) {
	c, _ := tunnelServer(t, "echo")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := newSandbox(c, SandboxInfo{ID: "sandbox"}).OpenTunnel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	stream, err := tun.Connect(ctx, 8080)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err = stream.Write(make([]byte, 10*tunnelChunk)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(stream.in) < 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(stream.in) < 10 {
		t.Fatal("unread response dispatch stalled")
	}
	openCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	second, err := tun.Connect(openCtx, 8080)
	if err != nil {
		t.Fatalf("unread response blocked another open: %v", err)
	}
	defer second.Close()
	cancel()
	select {
	case <-tun.done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("unread response blocked cancellation")
	}
}

func TestParityTunnelUnreadOverflowFailsOnlyItsStream(t *testing.T) {
	c, _ := tunnelServer(t, "echo")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tun, err := newSandbox(c, SandboxInfo{ID: "sandbox"}).OpenTunnel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	stream, err := tun.Connect(ctx, 8080)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, _ = stream.Write(make([]byte, 2*tunnelWindow))
	select {
	case <-stream.done:
	case <-ctx.Done():
		t.Fatal("unread overflow did not stop its stream")
	}
	_, err = io.ReadAll(stream)
	var sdkErr *Error
	if !errors.As(err, &sdkErr) || sdkErr.Code != "tunnel_receive_overflow" {
		t.Fatalf("lost explicit overflow: %v", err)
	}
	stream.receiveMu.Lock()
	queued := stream.receiveBytes
	stream.receiveMu.Unlock()
	if queued != 0 {
		t.Fatalf("buffer not drained: %d", queued)
	}
	second, err := tun.Connect(ctx, 8080)
	if err != nil {
		t.Fatalf("overflow broke another stream: %v", err)
	}
	defer second.Close()
	if _, err = second.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err = io.ReadFull(second, reply); err != nil || string(reply) != "ok" {
		t.Fatalf("second stream %q %v", reply, err)
	}
}
