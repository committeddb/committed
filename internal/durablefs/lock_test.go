package durablefs

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func lockPlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("unsupported lock platform")
	}
}

func TestDirectoryLockOwnership(t *testing.T) {
	lockPlatform(t)
	dir := t.TempDir()
	owner, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if _, err := Lock(dir); !errors.Is(err, ErrLocked) {
		t.Fatal("second instance acquired lock", err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(alias); !errors.Is(err, ErrLocked) {
		t.Fatal("alias bypassed lock", err)
	}
	other, err := Lock(t.TempDir())
	if err != nil {
		t.Fatal("independent directory blocked", err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("locking created files", entries, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Lock(dir)
	if err != nil {
		t.Fatal("close leaked lock", err)
	}
	defer next.Close()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(dir); !errors.Is(err, ErrLocked) {
		t.Fatal("double close released another owner", err)
	}
}

func TestDirectoryLockInvalidPath(t *testing.T) {
	lockPlatform(t)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(path); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := Lock(path + "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestDirectoryLockHelper(t *testing.T) {
	if os.Getenv("SEGMENTLOG_LOCK_HELPER") != "1" {
		return
	}
	lock, err := Lock(os.Getenv("SEGMENTLOG_LOCK_DIR"))
	if errors.Is(err, ErrLocked) {
		os.Exit(23)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if os.Getenv("SEGMENTLOG_LOCK_HOLD") == "1" {
		if _, err := os.Stdout.WriteString("ready\n"); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
	}
}

func helperCommand(ctx context.Context, dir string, hold bool) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDirectoryLockHelper$")
	value := "0"
	if hold {
		value = "1"
	}
	cmd.Env = append(os.Environ(), "SEGMENTLOG_LOCK_HELPER=1", "SEGMENTLOG_LOCK_DIR="+dir, "SEGMENTLOG_LOCK_HOLD="+value)
	return cmd
}

func TestDirectoryLockAcrossProcesses(t *testing.T) {
	lockPlatform(t)
	dir := t.TempDir()
	owner, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := helperCommand(ctx, dir, false)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		_ = owner.Close()
		t.Fatalf("child bypassed parent lock: %v %s", err, out)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	// Reverse the direction: a child owns the directory until forcibly killed.
	cmd = helperCommand(ctx, dir, true)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "ready\n" {
		t.Fatal("child readiness", line, err)
	}
	if _, err := Lock(dir); !errors.Is(err, ErrLocked) {
		t.Fatal("parent bypassed child lock", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err == nil {
		t.Fatal("expected killed process")
	}
	recovered, err := Lock(dir)
	if err != nil {
		t.Fatal("process death left stale ownership", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}
