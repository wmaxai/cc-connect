package codex

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestIntegration_CodexProviderFlow verifies the full provider config flow:
// 1. ensureCodexProviderConfig writes correct config.toml
// 2. ensureCodexAuth writes correct auth.json
// 3. Codex CLI can authenticate and respond using the written config
//
// Requires: SHENGSUANYUN_API_KEY env var and `codex` CLI in PATH.
// Skip with: go test ./agent/codex/ -run TestIntegration -v
func TestIntegration_CodexProviderFlow(t *testing.T) {
	apiKey := os.Getenv("SHENGSUANYUN_API_KEY")
	if apiKey == "" {
		t.Skip("SHENGSUANYUN_API_KEY not set, skipping integration test")
	}
	if _, err := exec.LookPath("codex"); err != nil {
		t.Skip("codex CLI not in PATH, skipping integration test")
	}

	home := filepath.Join(t.TempDir(), ".codex")

	// Step 1: write config.toml via our function
	err := ensureCodexProviderConfig(home, "shengsuanyun-codex",
		"https://router.shengsuanyun.com/api/v1", "responses", nil)
	if err != nil {
		t.Fatalf("ensureCodexProviderConfig: %v", err)
	}

	// Verify config.toml content
	cfgData, err := os.ReadFile(filepath.Join(home, "config.toml"))
	if err != nil {
		t.Fatalf("read config.toml: %v", err)
	}
	cfgContent := string(cfgData)
	t.Logf("Generated config.toml:\n%s", cfgContent)

	for _, want := range []string{
		`[model_providers.shengsuanyun-codex]`,
		`base_url = "https://router.shengsuanyun.com/api/v1"`,
		`wire_api = "responses"`,
		`env_key = "OPENAI_API_KEY"`,
	} {
		if !strings.Contains(cfgContent, want) {
			t.Errorf("config.toml missing: %s", want)
		}
	}

	// Step 2: write auth.json via our function
	err = ensureCodexAuth(home, apiKey)
	if err != nil {
		t.Fatalf("ensureCodexAuth: %v", err)
	}

	// Verify auth.json content
	authData, err := os.ReadFile(filepath.Join(home, "auth.json"))
	if err != nil {
		t.Fatalf("read auth.json: %v", err)
	}
	var authMap map[string]any
	if err := json.Unmarshal(authData, &authMap); err != nil {
		t.Fatalf("parse auth.json: %v", err)
	}
	if authMap["auth_mode"] != "apikey" {
		t.Errorf("auth_mode = %v, want apikey", authMap["auth_mode"])
	}
	if authMap["OPENAI_API_KEY"] != apiKey {
		t.Errorf("OPENAI_API_KEY not set correctly in auth.json")
	}

	// Step 3: run codex exec with the generated config
	workDir := filepath.Join(t.TempDir(), "repo")
	os.MkdirAll(workDir, 0o755)
	gitInit := exec.Command("git", "init", "-q")
	gitInit.Dir = workDir
	if err := gitInit.Run(); err != nil {
		t.Fatalf("git init: %v", err)
	}

	cmd := exec.Command("codex", "exec",
		"--model", "openai/gpt-5.3-codex",
		"-c", `model_provider="shengsuanyun-codex"`,
		"-c", `openai_base_url="https://router.shengsuanyun.com/api/v1"`,
		"--sandbox", "workspace-write",
		"-c", `approval_policy="never"`,
	)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"CODEX_HOME="+home,
		"OPENAI_API_KEY="+apiKey,
	)
	cmd.Stdin = strings.NewReader("reply with exactly 'integration-ok' and nothing else")

	output, err := cmd.CombinedOutput()
	t.Logf("Codex output:\n%s", string(output))

	if err != nil {
		t.Fatalf("codex exec failed: %v\noutput: %s", err, output)
	}
	if !strings.Contains(string(output), "integration-ok") {
		t.Errorf("expected 'integration-ok' in output, got:\n%s", output)
	}
}

// TestIntegration_SharedAppServerServesTwoProjects pins down the multiplex
// shape: two sessions from two projects share one `codex app-server` child, and
// each thread keeps its own working directory.
//
// Requires the codex CLI in PATH and a working ~/.codex login. It starts real
// processes but runs no turn, so it costs no model tokens.
// Run with: CC_CODEX_SHARED_APPSERVER_TEST=1 go test ./agent/codex/ -run TestIntegration_SharedAppServer -v
func TestIntegration_SharedAppServerServesTwoProjects(t *testing.T) {
	if os.Getenv("CC_CODEX_SHARED_APPSERVER_TEST") == "" {
		t.Skip("CC_CODEX_SHARED_APPSERVER_TEST not set, skipping integration test")
	}
	bin, err := resolveCodexExecutable("")
	if err != nil {
		t.Skip("codex CLI not in PATH, skipping integration test")
	}

	// Clear the pool so this test observes only the children it spawns.
	shutdownSharedAppServers()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A scratch CODEX_HOME keeps the test out of the user's ~/.codex: the child
	// writes sqlite state there, and thread/start needs no auth.
	codexHome := t.TempDir()
	dirA := t.TempDir()
	dirB := t.TempDir()
	shared := appServerConnBase{cliBin: bin, url: "stdio://", codexHome: codexHome, scope: appServerScopeShared}

	baseA := shared
	baseA.workDir = dirA
	baseB := shared
	baseB.workDir = dirB

	sessionA, err := newAppServerSession(ctx, baseA, dirA, "", "", "yolo", "", "", "", "", "")
	if err != nil {
		t.Fatalf("start session A: %v", err)
	}
	defer func() { _ = sessionA.Close() }()
	sessionB, err := newAppServerSession(ctx, baseB, dirB, "", "", "yolo", "", "", "", "", "")
	if err != nil {
		t.Fatalf("start session B: %v", err)
	}
	defer func() { _ = sessionB.Close() }()

	if sessionA.CurrentSessionID() == "" || sessionB.CurrentSessionID() == "" {
		t.Fatalf("thread ids = %q / %q, want both set", sessionA.CurrentSessionID(), sessionB.CurrentSessionID())
	}
	if sessionA.CurrentSessionID() == sessionB.CurrentSessionID() {
		t.Fatalf("both sessions resumed the same thread %q", sessionA.CurrentSessionID())
	}
	connA, connB := sessionA.currentConn(), sessionB.currentConn()
	if connA != connB {
		t.Fatalf("sessions landed on two different app-server processes (%p vs %p)", connA, connB)
	}
	t.Logf("shared app-server pid=%d generation=%d threads=%s / %s",
		connA.pid(), connA.Generation(), sessionA.CurrentSessionID(), sessionB.CurrentSessionID())

	if got := sessionA.GetWorkDir(); got != dirA {
		t.Errorf("session A work dir = %q, want %q", got, dirA)
	}
	if got := sessionB.GetWorkDir(); got != dirB {
		t.Errorf("session B work dir = %q, want %q", got, dirB)
	}

	// A private child per session has to stay available for callers that ask
	// for it, and it must not be the pooled one.
	privateBase := shared
	privateBase.scope = appServerScopeProcess
	privateBase.workDir = dirA
	privateSession, err := newAppServerSession(ctx, privateBase, dirA, "", "", "yolo", "", "", "", "", "")
	if err != nil {
		t.Fatalf("start private session: %v", err)
	}
	defer func() { _ = privateSession.Close() }()
	if privateSession.currentConn() == connA {
		t.Fatal("scope=process reused the shared app-server instead of spawning its own")
	}
	if got := privateSession.GetWorkDir(); got != dirA {
		t.Errorf("private session work dir = %q, want %q", got, dirA)
	}
	if err := privateSession.Close(); err != nil {
		t.Fatalf("close private session: %v", err)
	}
	if privateSession.currentConn().Alive() {
		t.Error("private process outlived its session")
	}

	// The shared process survives one of its sessions closing.
	if err := sessionB.Close(); err != nil {
		t.Fatalf("close session B: %v", err)
	}
	if !connA.Alive() {
		t.Error("closing one session killed the shared app-server")
	}

	shutdownSharedAppServers()
	if connA.Alive() {
		t.Error("shared app-server survived shutdown")
	}
}
