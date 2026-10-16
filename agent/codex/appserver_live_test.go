package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// TestLive_SharedAppServerRunsTwoProjectsAtOnce is the end-to-end check for the
// multiplex shape: one app-server, two threads, two working directories, two
// concurrent real turns that each report their own cwd. Everything else about
// the shared pool is unit-tested; this one needs a model, so it is skipped
// unless CC_CODEX_LIVE_HOME points at a CODEX_HOME with working credentials
// (a copy of ~/.codex is fine — the child needs to write sqlite state there).
//
// Run with: CC_CODEX_LIVE_HOME=/tmp/ccx_real go test ./agent/codex/ -run TestLive_ -v -timeout 8m
func TestLive_SharedAppServerRunsTwoProjectsAtOnce(t *testing.T) {
	home := os.Getenv("CC_CODEX_LIVE_HOME")
	if home == "" {
		t.Skip("CC_CODEX_LIVE_HOME not set")
	}
	bin, err := resolveCodexExecutable("")
	if err != nil {
		t.Skip("codex CLI not in PATH")
	}
	shutdownSharedAppServers()
	t.Cleanup(shutdownSharedAppServers)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dirA := t.TempDir()
	dirB := t.TempDir()
	for _, dir := range []string{dirA, dirB} {
		if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("hello\n"), 0o600); err != nil {
			t.Fatalf("seed workdir: %v", err)
		}
	}

	base := appServerConnBase{cliBin: bin, url: "stdio://", codexHome: home, scope: appServerScopeShared}
	baseA, baseB := base, base
	baseA.workDir, baseB.workDir = dirA, dirB

	sessionA, err := newAppServerSession(ctx, baseA, dirA, "", "", "yolo", "", "", "", "", "")
	if err != nil {
		t.Fatalf("session A: %v", err)
	}
	defer func() { _ = sessionA.Close() }()
	sessionB, err := newAppServerSession(ctx, baseB, dirB, "", "", "yolo", "", "", "", "", "")
	if err != nil {
		t.Fatalf("session B: %v", err)
	}
	defer func() { _ = sessionB.Close() }()

	if sessionA.currentConn() != sessionB.currentConn() {
		t.Fatal("the two projects did not land on one app-server")
	}
	t.Logf("shared pid=%d threads=%s / %s",
		sessionA.currentConn().pid(), sessionA.CurrentSessionID(), sessionB.CurrentSessionID())

	prompt := "Use the shell to run `pwd` in this workspace, then answer with that absolute path and nothing else."
	type result struct {
		name  string
		text  string
		start time.Time
		dur   time.Duration
	}
	results := make(chan result, 2)
	for _, item := range []struct {
		name string
		s    *appServerSession
		dir  string
	}{{"A", sessionA, dirA}, {"B", sessionB, dirB}} {
		go func(name string, s *appServerSession, dir string) {
			start := time.Now()
			if err := s.Send(prompt, "", nil, nil); err != nil {
				results <- result{name: name, text: "SEND ERROR: " + err.Error()}
				return
			}
			text, err := collectTurn(s.events, 4*time.Minute)
			if err != nil {
				results <- result{name: name, text: "TURN ERROR: " + err.Error()}
				return
			}
			results <- result{name: name, text: text, start: start, dur: time.Since(start)}
		}(item.name, item.s, item.dir)
	}

	got := map[string]string{}
	for i := 0; i < 2; i++ {
		r := <-results
		t.Logf("turn %s finished in %s: %.200q", r.name, r.dur, r.text)
		got[r.name] = r.text
	}
	if !strings.Contains(got["A"], dirA) {
		t.Errorf("turn A did not report its own cwd %q: %q", dirA, got["A"])
	}
	if !strings.Contains(got["B"], dirB) {
		t.Errorf("turn B did not report its own cwd %q: %q", dirB, got["B"])
	}
}

func collectTurn(events <-chan core.Event, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	var last string
	for {
		select {
		case event := <-events:
			switch event.Type {
			case core.EventResult:
				if strings.TrimSpace(event.Content) != "" {
					last = event.Content
				}
				if last != "" {
					return last, nil
				}
			case core.EventError:
				if event.Error != nil {
					return "", event.Error
				}
				return "", errors.New(event.Content)
			case core.EventText, core.EventThinking:
				if strings.TrimSpace(event.Content) != "" {
					last = event.Content
				}
			}
		case <-deadline:
			return "", errors.New("timeout waiting for the turn")
		}
	}
}
