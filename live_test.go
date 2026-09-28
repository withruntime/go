package withruntime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

/*
Against the real API, on the free trial. Set RUNTIME_LIVE_TEST=1; the key is

	RUNTIME_API_KEY or this machine's saved connection (npx withruntime login).
	Every sandbox it starts carries a run label and is stopped at the end,
	whatever failed. It spends trial time only: funding is "trial", which never
	falls back to paid credit.
*/
func TestLiveTrial(t *testing.T) {
	if os.Getenv("RUNTIME_LIVE_TEST") != "1" {
		t.Skip("set RUNTIME_LIVE_TEST=1 to run against the real API on the free trial")
	}
	c, err := New()
	if err != nil {
		t.Fatalf("could not authenticate: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	run := "go-live-" + newKey()[:8]
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		for sbx, err := range c.Sandboxes.All(cleanup, &ListOptions{Labels: map[string]string{"run": run}}) {
			if err != nil {
				t.Errorf("listing for cleanup: %v", err)
				return
			}
			if err := sbx.Stop(cleanup, nil); err != nil {
				t.Errorf("stopping %s: %v", sbx.ID(), err)
			} else {
				t.Logf("stopped %s: %s", sbx.ID(), sbx.State())
			}
		}
	})
	me, err := c.Me(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("org %s, API %s", me.OrgID, me.APIVersion)

	started := time.Now()
	sbx, err := c.Sandboxes.Create(ctx, &CreateOptions{
		Funding:        "trial",
		Labels:         map[string]string{"run": run, "sdk": "go"},
		TimeoutSeconds: 600,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created %s in %s: %s, %d vCPU", sbx.ID(), time.Since(started).Round(time.Millisecond), sbx.State(), sbx.Info().VCPU)

	result, err := sbx.Exec(ctx, "python3 -c 'print(6 * 7)'", &ExecOptions{Check: true})
	if err != nil || strings.TrimSpace(result.Stdout) != "42" {
		t.Fatalf("exec: %+v %v", result, err)
	}
	_, err = sbx.Exec(ctx, "exit 4", &ExecOptions{Check: true})
	var failed *CommandError
	if !errors.As(err, &failed) || *failed.ExitCode != 4 {
		t.Fatalf("check: %v", err)
	}

	var lines []string
	streamed, err := sbx.Exec(ctx, "for i in 1 2 3; do echo line $i; sleep 0.2; done", &ExecOptions{
		OnStdout: func(text string) { lines = append(lines, text) },
	})
	if err != nil || streamed.Stdout != "line 1\nline 2\nline 3\n" || len(lines) == 0 {
		t.Fatalf("streamed exec: %+v %v", streamed, err)
	}

	if err := sbx.Files.Write(ctx, "/workspace/go/hello.txt", []byte("from go")); err != nil {
		t.Fatal(err)
	}
	if text, err := sbx.Files.ReadText(ctx, "/workspace/go/hello.txt"); err != nil || text != "from go" {
		t.Fatalf("read back %q %v", text, err)
	}
	big := make([]byte, 2<<20+5)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := sbx.Files.Write(ctx, "/workspace/go/big.bin", big); err != nil {
		t.Fatal(err)
	}
	if back, err := sbx.Files.Read(ctx, "/workspace/go/big.bin"); err != nil || string(back) != string(big) {
		t.Fatalf("large file: %d bytes, %v", len(back), err)
	}

	proc, err := sbx.Spawn(ctx, "read name; echo hello $name", &SpawnOptions{PipeStdin: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Write(ctx, []byte("go\n"), true); err != nil {
		t.Fatal(err)
	}
	if waited, err := proc.Wait(ctx); err != nil || waited.Stdout != "hello go\n" {
		t.Fatalf("process: %+v %v", waited, err)
	}

	if err := sbx.Pause(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Logf("paused: %s", sbx.State())
	if err := sbx.Wake(ctx, 10*time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	if text, err := sbx.Files.ReadText(ctx, "/workspace/go/hello.txt"); err != nil || text != "from go" {
		t.Fatalf("after wake %q %v", text, err)
	}

	usage, err := c.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Trial != nil {
		t.Logf("trial: %d ms of %d used", usage.Trial.UsedMs, usage.Trial.TotalMs)
	}
	limits, err := c.Limits.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("limits: window %s", limits.Daily.Window)

	// The products beside the sandbox: each call answers from the real API.
	policy, err := sbx.Network.Set(ctx, Network{Internet: true, Allow: []string{"pypi.org"}})
	if err != nil || !policy.Internet || len(policy.Allow) != 1 {
		t.Fatalf("network set: %+v %v", policy, err)
	}
	if policy, err = sbx.Network.On(ctx); err != nil || len(policy.Allow) != 0 {
		t.Fatalf("network on: %+v %v", policy, err)
	}
	if _, err := sbx.Spawn(ctx, "python3 -m http.server 8000", nil); err != nil {
		t.Fatal(err)
	}
	preview, err := sbx.Previews.Create(ctx, 8000, nil)
	if err != nil || preview.Token == nil || !strings.Contains(preview.URL, "runtimehost.com") {
		t.Fatalf("preview: %+v %v", preview, err)
	}
	if rotated, err := sbx.Previews.Rotate(ctx, 8000); err != nil || rotated.Token == nil || *rotated.Token == *preview.Token {
		t.Fatalf("rotate: %v", err)
	}
	if err := sbx.Previews.Delete(ctx, 8000); err != nil {
		t.Fatalf("preview delete: %v", err)
	}
	cell, err := sbx.Interpreter.Run(ctx, "x = 20\nx + 22", nil)
	if err != nil || cell.Status != "ok" || len(cell.Results) == 0 || cell.Results[0].Data["text/plain"] != "42" {
		t.Fatalf("interpreter: %+v %v", cell, err)
	}
	if metrics, err := sbx.Metrics(ctx, "15m"); err != nil || metrics.SandboxID != sbx.ID() {
		t.Fatalf("metrics: %+v %v", metrics, err)
	}
	term, err := sbx.Terminal(ctx, &TerminalOptions{Cols: 100, Rows: 30})
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if _, err := term.Write([]byte("echo term-$((40 + 2)); exit\n")); err != nil {
		t.Fatal(err)
	}
	var printed strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, err := term.Read(buffer)
		printed.Write(buffer[:n])
		if err != nil {
			break
		}
	}
	if !strings.Contains(printed.String(), "term-42") {
		t.Fatalf("terminal printed %q", printed.String())
	}
	for name, read := range map[string]func() error{
		"volumes":   func() error { _, err := c.Volumes.List(ctx, nil); return err },
		"images":    func() error { _, err := c.Images.List(ctx, nil); return err },
		"secrets":   func() error { _, err := c.Secrets.List(ctx); return err },
		"events":    func() error { _, err := c.Events.List(ctx, &EventListOptions{ResourceID: sbx.ID()}); return err },
		"snapshots": func() error { _, err := c.Snapshots.List(ctx, nil); return err },
		"referrals": func() error { _, err := c.Referrals.Get(ctx); return err },
		"compare":   func() error { _, err := c.Switching.Compare(ctx, "e2b", 7); return err },
	} {
		if err := read(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	copies, err := sbx.Fork(ctx, &ForkOptions{Funding: "trial", Labels: map[string]string{"run": run, "sdk": "go"}})
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if text, err := copies[0].Files.ReadText(ctx, "/workspace/go/hello.txt"); err != nil || text != "from go" {
		t.Fatalf("the fork lost the file: %q %v", text, err)
	}
	if err := copies[0].Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if err := sbx.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if sbx.State() != "stopped" {
		t.Fatalf("stop left it %s", sbx.State())
	}
	_, err = sbx.Exec(ctx, "true", nil)
	var failure *Error
	if !errors.As(err, &failure) || failure.RequestID == "" {
		t.Fatalf("exec on a stopped sandbox: %v", err)
	}
	t.Logf("exec after stop: %s (%d) %s", failure.Code, failure.Status, failure.RequestID)
}
