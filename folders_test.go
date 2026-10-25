package withruntime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

/* A folder moves through the API's folder routes (ARCHITECTURE.md section
   10, "Folders over HTTP"), as it does in the JS, Python and Ruby SDKs: a
   broken archive is download_incomplete before anything is written, and a
   large upload goes in parts. */

// folderArchive is a gzipped tar of one file, whole unless cut is set: then
// it stops mid-entry, as tar in the sandbox does when it fails part way.
func folderArchive(t *testing.T, trailer bool) []byte {
	t.Helper()
	var plain bytes.Buffer
	archive := tar.NewWriter(&plain)
	data := []byte("print(1)\n")
	_ = archive.WriteHeader(&tar.Header{Name: "./src/", Mode: 0o755, Typeflag: tar.TypeDir})
	_ = archive.WriteHeader(&tar.Header{Name: "./src/main.py", Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg})
	_, _ = archive.Write(data)
	_ = archive.Flush()
	if trailer {
		_ = archive.Close()
	}
	var zipped bytes.Buffer
	writer := gzip.NewWriter(&zipped)
	_, _ = writer.Write(plain.Bytes())
	_ = writer.Close()
	return zipped.Bytes()
}

// folderServer answers stat as a directory and the folder route with body.
func folderServer(t *testing.T, body []byte) *server {
	return newServer(t, func(n int, w http.ResponseWriter, r *http.Request, _ []byte) {
		switch r.URL.Path {
		case "/v1/sandboxes/sbx-1/files/stat":
			writeJSON(w, 200, map[string]any{"exists": true, "path": "/workspace/project", "type": "directory"})
		case "/v1/sandboxes/sbx-1/files/archive":
			if r.URL.Query().Get("gzip") != "true" || r.URL.Query().Get("path") != "/workspace/project" {
				apiError(w, 400, "invalid_request", nil)
				return
			}
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(body)
		default:
			apiError(w, 404, "not_found", nil)
		}
	})
}

func leftovers(t *testing.T, dir string) []string {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestAFolderDownloadsThroughTheFolderRoute(t *testing.T) {
	t.Parallel()
	// bsdtar pads its compressed output to a whole record with zeros.
	padded := append(folderArchive(t, true), make([]byte, 700)...)
	if err := unpackArchive(padded, filepath.Join(t.TempDir(), "padded")); err != nil {
		t.Fatalf("a padded archive: %v", err)
	}
	s := folderServer(t, folderArchive(t, true))
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	parent := t.TempDir()
	out := filepath.Join(parent, "copy")
	if err := sbx.Files.Download(context.Background(), "/workspace/project", out); err != nil {
		t.Fatal(err)
	}
	if text, _ := os.ReadFile(filepath.Join(out, "src", "main.py")); string(text) != "print(1)\n" {
		t.Fatalf("got %q", text)
	}
	if names := leftovers(t, parent); len(names) != 1 || names[0] != "copy" {
		t.Fatalf("left beside it: %v", names)
	}
	for _, r := range s.requests {
		if strings.Contains(r.URL.Path, ":exec") || strings.Contains(r.URL.Path, "/processes") {
			t.Fatalf("ran a command to move a folder: %s", r.URL.Path)
		}
	}
}

func TestAFolderCutShortIsDownloadIncompleteAndWritesNothing(t *testing.T) {
	t.Parallel()
	whole := folderArchive(t, true)
	for name, body := range map[string][]byte{
		// gzip stopped part way: its checksum never arrives.
		"gzip cut": whole[:len(whole)-12],
		// A whole gzip stream around a tar that stopped between entries.
		"no end blocks": folderArchive(t, false),
		"empty":         {},
	} {
		t.Run(name, func(t *testing.T) {
			s := folderServer(t, body)
			sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
			parent := t.TempDir()
			err := sbx.Files.Download(context.Background(), "/workspace/project", filepath.Join(parent, "copy"))
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != "download_incomplete" || failure.Hint == "" {
				t.Fatalf("want download_incomplete, got %v", err)
			}
			if names := leftovers(t, parent); len(names) != 0 {
				t.Fatalf("a broken archive left %v", names)
			}
		})
	}
}

func TestAFolderMergesIntoOneThereButNeverThroughALink(t *testing.T) {
	t.Parallel()
	s := folderServer(t, folderArchive(t, true))
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	out := filepath.Join(t.TempDir(), "copy")
	_ = os.MkdirAll(filepath.Join(out, "src"), 0o755)
	_ = os.WriteFile(filepath.Join(out, "keep.txt"), []byte("mine"), 0o644)
	_ = os.WriteFile(filepath.Join(out, "src", "main.py"), []byte("old"), 0o644)
	if err := sbx.Files.Download(context.Background(), "/workspace/project", out); err != nil {
		t.Fatal(err)
	}
	if text, _ := os.ReadFile(filepath.Join(out, "keep.txt")); string(text) != "mine" {
		t.Fatal("the merge lost a file that was there")
	}
	if text, _ := os.ReadFile(filepath.Join(out, "src", "main.py")); string(text) != "print(1)\n" {
		t.Fatal("the merge kept the old file")
	}
	// src is now a link out of the folder: nothing may be written through it.
	elsewhere := t.TempDir()
	_ = os.RemoveAll(filepath.Join(out, "src"))
	if err := os.Symlink(elsewhere, filepath.Join(out, "src")); err != nil {
		t.Skip("no symlinks here")
	}
	if err := sbx.Files.Download(context.Background(), "/workspace/project", out); err == nil || !strings.Contains(err.Error(), "link") {
		t.Fatalf("want a refusal, got %v", err)
	}
	if names := leftovers(t, elsewhere); len(names) != 0 {
		t.Fatalf("wrote through the link: %v", names)
	}
}

func TestALargeFolderUploadsInPartsInOrder(t *testing.T) {
	t.Parallel()
	var parts []string
	var begin, small bool
	var joined bytes.Buffer
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch r.URL.Path {
		case "/v1/sandboxes/sbx-1/files/archive":
			small = r.Method == http.MethodPut && r.URL.Query().Get("path") == "/workspace/small"
			writeJSON(w, 200, map[string]any{"path": "/workspace/small", "received": len(body)})
		case "/v1/sandboxes/sbx-1/files/archive/uploads":
			begin = strings.Contains(string(body), `"gzip":true`) && strings.Contains(string(body), `"/workspace/large"`)
			writeJSON(w, 200, map[string]any{"uploadId": "a1", "chunkBytes": 1 << 20})
		case "/v1/sandboxes/sbx-1/files/archive/uploads/a1":
			parts = append(parts, r.URL.Query().Get("offset"))
			joined.Write(body)
			writeJSON(w, 200, map[string]any{"received": joined.Len()})
		case "/v1/sandboxes/sbx-1/files/archive/uploads/a1:commit":
			writeJSON(w, 200, map[string]any{"path": "/workspace/large", "received": joined.Len()})
		default:
			apiError(w, 404, "not_found", nil)
		}
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	local := t.TempDir()
	_ = os.WriteFile(filepath.Join(local, "a.txt"), []byte("a"), 0o644)
	if err := sbx.Files.Upload(context.Background(), local, "/workspace/small"); err != nil || !small {
		t.Fatalf("a small folder: %v, one PUT %v", err, small)
	}
	noise := make([]byte, 2<<20+5)
	state := uint32(1)
	for i := range noise {
		state = state*1664525 + 1013904223
		noise[i] = byte(state >> 24)
	}
	_ = os.WriteFile(filepath.Join(local, "noise.bin"), noise, 0o644)
	if err := sbx.Files.Upload(context.Background(), local, "/workspace/large"); err != nil {
		t.Fatal(err)
	}
	if !begin || strings.Join(parts, ",") != "0,1048576,2097152" {
		t.Fatalf("begin %v, parts %v", begin, parts)
	}
	back := filepath.Join(t.TempDir(), "back")
	if err := unpackArchive(joined.Bytes(), back); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(back, "noise.bin")); !bytes.Equal(got, noise) {
		t.Fatal("the parts do not reassemble the folder")
	}
	if last := s.requests[len(s.requests)-1].URL.Path; last != "/v1/sandboxes/sbx-1/files/archive/uploads/a1:commit" {
		t.Fatalf("last call %s", last)
	}
}

func TestAFailedFolderUploadIsAborted(t *testing.T) {
	t.Parallel()
	s := newServer(t, func(n int, w http.ResponseWriter, r *http.Request, body []byte) {
		switch r.URL.Path {
		case "/v1/sandboxes/sbx-1/files/archive/uploads":
			writeJSON(w, 200, map[string]any{"uploadId": "a1", "chunkBytes": 1 << 20})
		case "/v1/sandboxes/sbx-1/files/archive/uploads/a1":
			apiError(w, 507, "no_space", nil)
		default:
			writeJSON(w, 200, map[string]any{})
		}
	})
	sbx := newSandbox(s.client(t), SandboxInfo{ID: "sbx-1"})
	local := t.TempDir()
	noise := make([]byte, 2<<20)
	state := uint32(7)
	for i := range noise {
		state = state*1664525 + 1013904223
		noise[i] = byte(state >> 24)
	}
	_ = os.WriteFile(filepath.Join(local, "noise.bin"), noise, 0o644)
	err := sbx.Files.Upload(context.Background(), local, "/workspace/large")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "no_space" {
		t.Fatalf("got %v", err)
	}
	if last := s.requests[len(s.requests)-1].URL.Path; last != "/v1/sandboxes/sbx-1/files/archive/uploads/a1:abort" {
		t.Fatalf("last call %s", last)
	}
}

// tarEntry is one header and its body.
type tarEntry struct {
	header tar.Header
	body   string
}

// gzipTar packs entries as one gzipped tar.
func gzipTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var plain bytes.Buffer
	archive := tar.NewWriter(&plain)
	for _, entry := range entries {
		header := entry.header
		header.Size = int64(len(entry.body))
		if err := archive.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		_, _ = archive.Write([]byte(entry.body))
	}
	_ = archive.Close()
	var zipped bytes.Buffer
	writer := gzip.NewWriter(&zipped)
	_, _ = writer.Write(plain.Bytes())
	_ = writer.Close()
	return zipped.Bytes()
}

// tar writes a file's first name as a file and every other as a hard link to
// it: a folder holding one file twice (1 October 2026 audit).
func TestHardLinkedFilesLandWithTheirContent(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "out")
	err := unpackArchive(gzipTar(t,
		tarEntry{tar.Header{Name: "./a.txt", Mode: 0o644, Typeflag: tar.TypeReg}, "first"},
		tarEntry{tar.Header{Name: "./sub/b.txt", Linkname: "./a.txt", Typeflag: tar.TypeLink}, ""},
		tarEntry{tar.Header{Name: "./a.txt", Mode: 0o644, Typeflag: tar.TypeReg}, "second"},
	), out)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"a.txt": "second", "sub/b.txt": "first"} {
		got, err := os.ReadFile(filepath.Join(out, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
	for _, link := range []string{"later.txt", "../outside/secret.txt", "/etc/hosts", "d"} {
		err := unpackArchive(gzipTar(t,
			tarEntry{tar.Header{Name: "./d/", Mode: 0o755, Typeflag: tar.TypeDir}, ""},
			tarEntry{tar.Header{Name: "./x", Linkname: link, Typeflag: tar.TypeLink}, ""},
			tarEntry{tar.Header{Name: "./later.txt", Mode: 0o644, Typeflag: tar.TypeReg}, "a"},
		), filepath.Join(t.TempDir(), "refused"))
		if err == nil || !strings.Contains(err.Error(), "does not carry") {
			t.Fatalf("hard link to %s: %v", link, err)
		}
	}
}
