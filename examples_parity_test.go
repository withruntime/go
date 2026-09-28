package withruntime_test

import (
	"context"
	"io"
	runtime "withruntime.com/go"
)

// Compile the public TCP stream shape without opening a live service.
func ExampleSandbox_OpenTunnel() {
	ctx := context.Background()
	client, _ := runtime.New()
	sandbox, _ := client.Sandboxes.Get(ctx, "11111111-2222-4333-8444-555555555555")
	tunnel, _ := sandbox.OpenTunnel(ctx)
	defer tunnel.Close()
	stream, _ := tunnel.Connect(ctx, 5432)
	defer stream.Close()
	var connection io.ReadWriteCloser = stream
	_ = connection
}
