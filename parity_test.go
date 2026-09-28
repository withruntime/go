package withruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParityProductRequestsAndTypedAnswers(t *testing.T) {
	ctx := context.Background()
	var method, path string
	var received map[string]any
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
		method, path = r.Method, r.URL.RequestURI()
		received = map[string]any{}
		_ = json.Unmarshal(body, &received)
		writeJSON(w, 200, map[string]any{"id": "resource", "data": []any{}, "funded": false, "fundedUntil": "2026-09-24T12:00:00Z", "rateMicros": 5000000, "rateUnit": "unit_month", "config": "private key draft", "configReady": false, "hint": "Wait for gateway key", "backups": map[string]any{"daily": false, "retentionDays": 7}, "restoredFrom": "backup"})
	})
	c := s.client(t)
	sbx := newSandbox(c, SandboxInfo{ID: "sandbox"})
	cases := []struct {
		name, method, path string
		run                func() error
		body               map[string]any
	}{
		{"domain add", "POST", "/v1/domains", func() error {
			_, e := c.Domains.Add(ctx, AddDomainOptions{Hostname: "app.example.com", SandboxID: "sandbox", Port: 3000})
			return e
		}, map[string]any{"hostname": "app.example.com", "sandboxId": "sandbox", "port": float64(3000)}},
		{"domain verify", "POST", "/v1/domains/app.example.com:verify", func() error { _, e := c.Domains.Verify(ctx, "app.example.com"); return e }, nil},
		{"domain get", "GET", "/v1/domains/app.example.com", func() error { _, e := c.Domains.Get(ctx, "app.example.com"); return e }, nil},
		{"domain list", "GET", "/v1/domains", func() error { _, e := c.Domains.List(ctx); return e }, nil},
		{"domain remove", "DELETE", "/v1/domains/app.example.com", func() error { return c.Domains.Remove(ctx, "app.example.com") }, nil},
		{"port open", "POST", "/v1/ports", func() error { _, e := c.Ports.Open(ctx, OpenPortOptions{SandboxID: "sandbox", Port: 5432}); return e }, nil},
		{"port list", "GET", "/v1/ports?sandboxId=sandbox", func() error { _, e := c.Ports.List(ctx, "sandbox"); return e }, nil},
		{"port close", "DELETE", "/v1/ports/port", func() error { return c.Ports.Close(ctx, "port") }, nil},
		{"address reserve", "POST", "/v1/addresses", func() error {
			a, e := c.Addresses.Reserve(ctx, 4)
			if e == nil && (a.Funded || a.FundedUntil == nil || a.RateMicros != 5000000) {
				t.Fatalf("funding lost: %+v", a)
			}
			return e
		}, map[string]any{"family": float64(4)}},
		{"address list", "GET", "/v1/addresses", func() error { _, e := c.Addresses.List(ctx); return e }, nil},
		{"address release", "DELETE", "/v1/addresses/address", func() error { return c.Addresses.Release(ctx, "address") }, nil},
		{"tunnel create", "POST", "/v1/tunnel", func() error { _, e := c.Tunnel.Create(ctx, "10.66.0.0/24"); return e }, nil},
		{"tunnel get", "GET", "/v1/tunnel", func() error { _, e := c.Tunnel.Get(ctx); return e }, nil},
		{"peer add", "POST", "/v1/tunnel/peers", func() error {
			out, e := c.Tunnel.AddPeer(ctx, AddTunnelPeerOptions{Name: "office"})
			if e == nil && (out.Config == nil || out.ConfigReady || out.Hint == "") {
				t.Fatal("draft private key lost")
			}
			return e
		}, nil},
		{"peer rotate", "POST", "/v1/tunnel/peers/peer:rotate", func() error { _, e := c.Tunnel.RotatePeer(ctx, "peer", ""); return e }, nil},
		{"peer remove", "DELETE", "/v1/tunnel/peers/peer", func() error { _, e := c.Tunnel.RemovePeer(ctx, "peer"); return e }, nil},
		{"tunnel delete", "DELETE", "/v1/tunnel", func() error { return c.Tunnel.Delete(ctx) }, nil},
		{"sso", "GET", "/v1/sso", func() error { _, e := c.SSO.Get(ctx); return e }, nil},
		{"mcp catalog", "GET", "/v1/mcp/catalog", func() error { _, e := c.MCP.Catalog(ctx); return e }, nil},
		{"mcp start", "POST", "/v1/sandboxes/sandbox/mcp", func() error {
			_, e := sbx.MCP.Start(ctx, MCPStartOptions{Servers: []MCPServerRequest{{ID: "github", Secrets: map[string]string{"TOKEN": "SECRET"}}}})
			return e
		}, nil},
		{"mcp get", "GET", "/v1/sandboxes/sandbox/mcp", func() error { _, e := sbx.MCP.Get(ctx); return e }, nil},
		{"mcp stop", "DELETE", "/v1/sandboxes/sandbox/mcp", func() error { return sbx.MCP.Stop(ctx) }, nil},
		{"mount add", "POST", "/v1/sandboxes/sandbox/mounts", func() error {
			_, e := sbx.Mounts.Add(ctx, BucketMountOptions{Provider: "r2", Bucket: "data", Path: "/data", Secret: "BUCKET"})
			return e
		}, nil},
		{"mount list", "GET", "/v1/sandboxes/sandbox/mounts", func() error { _, e := sbx.Mounts.List(ctx); return e }, nil},
		{"mount remove", "POST", "/v1/sandboxes/sandbox/mounts:unmount", func() error { return sbx.Mounts.Remove(ctx, "/data") }, map[string]any{"path": "/data"}},
		{"record start", "POST", "/v1/sandboxes/sandbox/desktop/recordings", func() error { _, e := sbx.Desktop.Recordings.Start(ctx, &RecordOptions{FPS: 15}); return e }, map[string]any{"fps": float64(15)}},
		{"record get", "GET", "/v1/sandboxes/sandbox/desktop/recordings/rec", func() error { _, e := sbx.Desktop.Recordings.Get(ctx, "rec"); return e }, nil},
		{"record stop", "POST", "/v1/sandboxes/sandbox/desktop/recordings/rec:stop", func() error { _, e := sbx.Desktop.Recordings.Stop(ctx, "rec"); return e }, nil},
		{"record list", "GET", "/v1/sandboxes/sandbox/desktop/recordings", func() error { _, e := sbx.Desktop.Recordings.List(ctx); return e }, nil},
		{"record video", "GET", "/v1/sandboxes/sandbox/desktop/recordings/rec/video", func() error { _, e := sbx.Desktop.Recordings.Download(ctx, "rec"); return e }, nil},
		{"record delete", "DELETE", "/v1/sandboxes/sandbox/desktop/recordings/rec", func() error { return sbx.Desktop.Recordings.Delete(ctx, "rec") }, nil},
		{"backup", "POST", "/v1/volumes/volume:backup", func() error { _, e := c.Volumes.Backup(ctx, "volume", nil); return e }, nil},
		{"backup policy", "POST", "/v1/volumes/volume:backup-policy", func() error {
			off := false
			v, e := c.Volumes.SetBackupPolicy(ctx, "volume", SetBackupPolicyOptions{Daily: &off})
			if e == nil && v.Backups.Daily {
				t.Fatal("false omitted")
			}
			return e
		}, map[string]any{"daily": false}},
		{"backup list", "GET", "/v1/volume-backups?volumeId=volume", func() error { _, e := c.Volumes.Backups(ctx, &BackupListOptions{VolumeID: "volume"}); return e }, nil},
		{"backup get", "GET", "/v1/volume-backups/backup", func() error { _, e := c.Volumes.GetBackup(ctx, "backup"); return e }, nil},
		{"backup delete", "POST", "/v1/volume-backups/backup:delete", func() error { _, e := c.Volumes.DeleteBackup(ctx, "backup"); return e }, nil},
		{"restore", "POST", "/v1/volumes", func() error {
			v, e := c.Volumes.Restore(ctx, "backup", nil)
			if e == nil && (v.RestoredFrom == nil || *v.RestoredFrom != "backup") {
				t.Fatal("restore metadata lost")
			}
			return e
		}, map[string]any{"fromBackup": "backup"}},
		{"chmod", "POST", "/v1/sandboxes/sandbox/files:chmod", func() error { return sbx.Files.Chmod(ctx, "/workspace/run", 0o755) }, map[string]any{"path": "/workspace/run", "mode": "755"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err != nil {
				t.Fatal(err)
			}
			if method != tc.method || path != tc.path {
				t.Fatalf("got %s %s want %s %s", method, path, tc.method, tc.path)
			}
			if tc.body != nil {
				a, _ := json.Marshal(received)
				b, _ := json.Marshal(tc.body)
				if !bytes.Equal(a, b) {
					t.Fatalf("body %s want %s", a, b)
				}
			}
		})
	}
}

func TestParityGeneratedPeerKeysNeverAutomaticallyRetry(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, _ *http.Request, _ []byte) {
		apiError(w, 502, "upstream_failure", nil)
	})
	c := s.client(t, WithMaxRetries(3))
	_, err := c.Tunnel.AddPeer(context.Background(), AddTunnelPeerOptions{Name: "office"})
	if err == nil || len(s.requests) != 1 {
		t.Fatalf("unexpected retry: %d %v", len(s.requests), err)
	}
	_, err = c.Tunnel.RotatePeer(context.Background(), "peer", "")
	if err == nil || len(s.requests) != 2 {
		t.Fatalf("unexpected rotate retry: %d %v", len(s.requests), err)
	}
}

func TestParityFileModePhaseKeysAndReplay(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			var mu sync.Mutex
			journal := map[string]string{}
			committed := false
			chunks := 0
			chmods := 0
			begins := map[string]bool{}
			s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, body []byte) {
				mu.Lock()
				defer mu.Unlock()
				var input map[string]any
				_ = json.Unmarshal(body, &input)
				key := r.Header.Get("Idempotency-Key")
				if strings.HasSuffix(r.URL.Path, "/uploads") || strings.HasSuffix(r.URL.Path, ":commit") {
					value := r.URL.Path + string(body)
					if previous, ok := journal[key]; ok && previous != value {
						apiError(w, 409, "operation_conflict", nil)
						return
					}
					journal[key] = value
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/uploads"):
					if legacy && input["mode"] != nil {
						apiError(w, 409, "guest_upgrade_required", nil)
						return
					}
					reply := map[string]any{"uploadId": "u-1", "chunkBytes": chunkBytes, "replayed": begins[key]}
					if !legacy {
						reply["mode"] = input["mode"]
					}
					begins[key] = true
					writeJSON(w, 200, reply)
				case r.Method == "PUT":
					if committed {
						apiError(w, 404, "transfer_not_found", nil)
						return
					}
					chunks++
					writeJSON(w, 200, map[string]any{})
				case strings.HasSuffix(r.URL.Path, ":commit"):
					committed = true
					writeJSON(w, 200, map[string]any{})
				case strings.HasSuffix(r.URL.Path, ":chmod"):
					chmods++
					writeJSON(w, 200, map[string]any{})
				default:
					t.Errorf("unexpected %s", r.URL.Path)
					writeJSON(w, 500, map[string]any{})
				}
			})
			c := s.client(t, WithMaxRetries(0))
			sbx := newSandbox(c, SandboxInfo{ID: "sandbox"})
			mode := uint32(0o755)
			opts := &FileWriteOptions{Mode: &mode, IdempotencyKey: "repeat"}
			for range 2 {
				if err := sbx.Files.Write(context.Background(), "/workspace/run", make([]byte, chunkBytes+1), opts); err != nil {
					t.Fatal(err)
				}
			}
			if chunks != 2 {
				t.Fatalf("replayed chunks: %d", chunks)
			}
			if legacy && chmods != 2 || !legacy && chmods != 0 {
				t.Fatalf("chmods %d", chmods)
			}
		})
	}
}

func TestParityMCPReadyCancelsAndModeZeroSurvives(t *testing.T) {
	s := newServer(t, func(_ int, w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "/mcp") {
			writeJSON(w, 200, map[string]any{"running": true, "servers": []any{map[string]any{"status": "installing"}}})
			return
		}
		if r.URL.Query().Get("mode") != "000" {
			t.Error("zero mode lost")
		}
		writeJSON(w, 200, map[string]any{})
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sandbox"})
	zero := uint32(0)
	if err := sbx.Files.Write(context.Background(), "/workspace/zero", []byte("x"), &FileWriteOptions{Mode: &zero}); err != nil {
		t.Fatal(err)
	}
	_, err := sbx.MCP.Ready(context.Background(), 5*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ready cancellation %v", err)
	}
}

// The repository fixture serves the real API router and validation. Product
// backends are simulated; an unavailable DB is acceptable, a missing route or
// rejected request schema is not.
func TestFixtureParityRoutes(t *testing.T) {
	fixture(t)
	c, err := New(WithAPIKey("rk_test"), WithBaseURL(fixtureURL), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sbx := newSandbox(c, SandboxInfo{ID: fixtureID})
	id := "22222222-3333-4444-8555-666666666666"
	calls := map[string]func() error{
		"domains.add": func() error {
			_, e := c.Domains.Add(ctx, AddDomainOptions{Hostname: "go.example.com", SandboxID: fixtureID, Port: 3000})
			return e
		},
		"domains.verify":    func() error { _, e := c.Domains.Verify(ctx, "go.example.com"); return e },
		"domains.get":       func() error { _, e := c.Domains.Get(ctx, "go.example.com"); return e },
		"domains.list":      func() error { _, e := c.Domains.List(ctx); return e },
		"domains.remove":    func() error { return c.Domains.Remove(ctx, "go.example.com") },
		"ports.open":        func() error { _, e := c.Ports.Open(ctx, OpenPortOptions{SandboxID: fixtureID, Port: 5432}); return e },
		"ports.list":        func() error { _, e := c.Ports.List(ctx, fixtureID); return e },
		"ports.close":       func() error { return c.Ports.Close(ctx, id) },
		"addresses.reserve": func() error { _, e := c.Addresses.Reserve(ctx, 4); return e },
		"addresses.list":    func() error { _, e := c.Addresses.List(ctx); return e },
		"addresses.release": func() error { return c.Addresses.Release(ctx, id) },
		"tunnel.create":     func() error { _, e := c.Tunnel.Create(ctx, ""); return e },
		"tunnel.get":        func() error { _, e := c.Tunnel.Get(ctx); return e },
		"tunnel.delete":     func() error { return c.Tunnel.Delete(ctx) },
		"tunnel.addPeer": func() error {
			_, e := c.Tunnel.AddPeer(ctx, AddTunnelPeerOptions{Name: "office", PublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="})
			return e
		},
		"tunnel.rotatePeer": func() error {
			_, e := c.Tunnel.RotatePeer(ctx, id, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
			return e
		},
		"tunnel.removePeer": func() error { _, e := c.Tunnel.RemovePeer(ctx, id); return e },
		"sso.get":           func() error { _, e := c.SSO.Get(ctx); return e },
		"mcp.catalog":       func() error { _, e := c.MCP.Catalog(ctx); return e },
		"mcp.start": func() error {
			_, e := sbx.MCP.Start(ctx, MCPStartOptions{Servers: []MCPServerRequest{{ID: "fetch"}}})
			return e
		},
		"mcp.get":  func() error { _, e := sbx.MCP.Get(ctx); return e },
		"mcp.stop": func() error { return sbx.MCP.Stop(ctx) },
		"mount.add": func() error {
			_, e := sbx.Mounts.Add(ctx, BucketMountOptions{Provider: "s3", Bucket: "example-bucket", Path: "/data", Region: "us-east-1", Secret: "BUCKET"})
			return e
		},
		"mount.list":    func() error { _, e := sbx.Mounts.List(ctx); return e },
		"mount.remove":  func() error { return sbx.Mounts.Remove(ctx, "/data") },
		"record.start":  func() error { _, e := sbx.Desktop.Recordings.Start(ctx, nil); return e },
		"record.list":   func() error { _, e := sbx.Desktop.Recordings.List(ctx); return e },
		"record.get":    func() error { _, e := sbx.Desktop.Recordings.Get(ctx, "rec-abcdefgh"); return e },
		"record.stop":   func() error { _, e := sbx.Desktop.Recordings.Stop(ctx, "rec-abcdefgh"); return e },
		"record.delete": func() error { return sbx.Desktop.Recordings.Delete(ctx, "rec-abcdefgh") },
		"watch.start":   func() error { _, e := sbx.Files.Watch(ctx, "/workspace", nil); return e },
		"watch.list":    func() error { _, e := sbx.Files.Watches.List(ctx); return e },
		"watch.read":    func() error { _, e := sbx.Files.Watches.Read(ctx, "watch", 0, 0); return e },
		"watch.stop":    func() error { return sbx.Files.Watches.Stop(ctx, "watch") },
		"files.chmod":   func() error { return sbx.Files.Chmod(ctx, "/workspace/run", 0o755) },
		"volume.backup": func() error { _, e := c.Volumes.Backup(ctx, id, nil); return e },
		"volume.policy": func() error {
			off := false
			_, e := c.Volumes.SetBackupPolicy(ctx, id, SetBackupPolicyOptions{Daily: &off})
			return e
		},
		"volume.backups":      func() error { _, e := c.Volumes.Backups(ctx, &BackupListOptions{VolumeID: id}); return e },
		"volume.getBackup":    func() error { _, e := c.Volumes.GetBackup(ctx, id); return e },
		"volume.deleteBackup": func() error { _, e := c.Volumes.DeleteBackup(ctx, id); return e },
		"volume.restore":      func() error { _, e := c.Volumes.Restore(ctx, id, nil); return e },
	}
	for name, run := range calls {
		t.Run(name, func(t *testing.T) { reached(t, name, run()) })
	}
}
