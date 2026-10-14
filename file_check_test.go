package withruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// A file's bytes come with the length and SHA-256 the API promised; a broken
// stream can arrive as one that ended, short (files.ts readBytes). Such a read
// is tried again, then refused: never returned short as the file.

func fileReads(t *testing.T, bodies ...string) (*Sandbox, *tries) {
	whole := "0123456789"
	sum := sha256.Sum256([]byte(whole))
	rt := &tries{fail: func(n int, r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/files/content") {
			return nil, nil
		}
		body := bodies[min(n-2, len(bodies)-1)]
		return &http.Response{StatusCode: 200, Header: http.Header{
			"Content-Type": {"application/octet-stream"}, "X-Content-Length": {"10"},
			"X-Content-Sha256": {hex.EncodeToString(sum[:])},
		}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	}}
	c := neverSentClient(t, rt)
	sandbox, err := c.Sandboxes.Get(context.Background(), "sbx-1")
	if err != nil {
		t.Fatal(err)
	}
	return sandbox, rt
}

func TestAFileReadShortOfItsLengthIsRefused(t *testing.T) {
	sandbox, rt := fileReads(t, "01234")
	data, err := sandbox.Files.Read(context.Background(), "/a")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "download_incomplete" {
		t.Fatalf("want download_incomplete, got %q, %v", data, err)
	}
	if reads := len(rt.at) - 1; reads != 3 {
		t.Fatalf("want three reads, got %d", reads)
	}
}

func TestAFileReadThatDiffersFromItsDigestIsRefused(t *testing.T) {
	sandbox, _ := fileReads(t, "0123456780")
	if _, err := sandbox.Files.Read(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("got %v", err)
	}
}

func TestAShortFileReadIsReadAgain(t *testing.T) {
	sandbox, _ := fileReads(t, "01234", "0123456789")
	data, err := sandbox.Files.Read(context.Background(), "/a")
	if err != nil || string(data) != "0123456789" {
		t.Fatalf("got %q, %v", data, err)
	}
}
