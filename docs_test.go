package withruntime

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every Go sample in the Go guide (sdks/go/GUIDE.md)
// compiles and passes go vet against this module, so a sample that names a
// method, a field or an option that does not exist fails here.
func TestTheGuidesSamplesCompile(t *testing.T) {
	guide, err := os.ReadFile("GUIDE.md")
	if err != nil {
		t.Skip("the guide is not beside this module")
	}
	samples := regexp.MustCompile("(?ms)^```go\n(.*?)^```$").FindAllStringSubmatch(string(guide), -1)
	if len(samples) < 8 {
		t.Fatalf("found only %d Go samples", len(samples))
	}
	here, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	module := "module samples\n\ngo 1.23\n\nrequire withruntime.com/go v0.0.0\n\nreplace withruntime.com/go => " + here + "\n"
	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte(module), 0o644); err != nil {
		t.Fatal(err)
	}
	for i, sample := range samples {
		if !strings.HasPrefix(sample[1], "package main\n") {
			t.Fatalf("sample %d is not a whole program", i+1)
		}
		directory := filepath.Join(work, fmt.Sprintf("sample%02d", i+1))
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte(sample[1]), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	vet := exec.Command("go", "vet", "./...")
	vet.Dir = work
	vet.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off")
	if out, err := vet.CombinedOutput(); err != nil {
		t.Fatalf("the guide's samples do not compile:\n%s", out)
	}
	for i, sample := range samples {
		formatted := exec.Command("gofmt", "-l", filepath.Join(work, fmt.Sprintf("sample%02d", i+1), "main.go"))
		if out, _ := formatted.Output(); len(out) > 0 {
			t.Errorf("sample %d is not gofmt-formatted:\n%s", i+1, sample[1])
		}
	}
}
