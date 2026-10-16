//go:build unix

package codex

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Closing the shared app-server has to take its whole process group with it.
// The `codex` on PATH is a wrapper that spawns the real app-server, so killing
// only the direct child leaves the app-server running as an orphan that holds
// the writer lock of every conversation it served — and those threads stay
// unresumable until it finally exits.
//
// Run with: CC_CODEX_SHARED_APPSERVER_TEST=1 go test ./agent/codex/ -run TestIntegration_SharedAppServerLeavesNoOrphan -v
func TestIntegration_SharedAppServerLeavesNoOrphan(t *testing.T) {
	if os.Getenv("CC_CODEX_SHARED_APPSERVER_TEST") == "" {
		t.Skip("CC_CODEX_SHARED_APPSERVER_TEST not set, skipping integration test")
	}
	bin, err := resolveCodexExecutable("")
	if err != nil {
		t.Skip("codex CLI not in PATH, skipping integration test")
	}

	shutdownSharedAppServers()
	t.Cleanup(shutdownSharedAppServers)

	// A scratch CODEX_HOME: the child needs to write its sqlite state there.
	base := appServerConnBase{
		cliBin:    bin,
		url:       "stdio://",
		codexHome: t.TempDir(),
		workDir:   t.TempDir(),
		scope:     appServerScopeShared,
	}
	conn, err := sharedAppServerPool.acquire(base)
	if err != nil {
		t.Fatalf("acquire shared app-server: %v", err)
	}
	group := conn.pid()
	if group == 0 {
		t.Fatal("shared app-server has no process id")
	}

	started := time.Now()
	if err := conn.Close(); err != nil {
		t.Fatalf("close shared app-server: %v", err)
	}
	if elapsed := time.Since(started); elapsed > appServerStopGrace+3*time.Second {
		t.Fatalf("close took %s, want the child to exit on SIGTERM", elapsed)
	}

	// Setpgid made the child its own group leader, so the negative pid is the
	// group: nothing may be left behind in it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(-group, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			leftovers := processGroupMembers(group)
			t.Fatalf("app-server process group %d survived Close: kill err=%v, members=%v", group, err, leftovers)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// processGroupMembers lists the pids still in a process group, for the failure
// message. It reads /proc rather than shelling out so it cannot itself be
// mistaken for a leftover.
func processGroupMembers(group int) []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var members []string
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}
		// pid (comm) state ppid pgrp ... — comm can contain spaces, so take the
		// fields after the closing parenthesis.
		after, _, ok := strings.Cut(string(stat), ") ")
		if !ok {
			continue
		}
		fields := strings.Fields(after)
		if len(fields) < 3 {
			continue
		}
		if pgrp, err := strconv.Atoi(fields[2]); err == nil && pgrp == group {
			cmdline, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
			members = append(members, strings.TrimSpace(strings.ReplaceAll(string(cmdline), "\x00", " ")))
		}
	}
	return members
}
