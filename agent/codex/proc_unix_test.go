//go:build unix

package codex

import (
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// The app-server is often reached through a wrapper that spawns the real
// process, so stopping it has to take the whole group down: an orphan would keep
// holding the thread writer locks of every conversation it served.
func TestGracefulStopCmdStopsTheWholeProcessGroup(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 120 & wait")
	prepareCmdForKill(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() { _ = forceKillCmd(cmd) })

	if err := gracefulStopCmd(cmd); err != nil {
		t.Fatalf("gracefulStopCmd() error = %v", err)
	}

	waited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the helper ignored SIGTERM")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := syscall.Kill(-cmd.Process.Pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group survived the shutdown signal: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
