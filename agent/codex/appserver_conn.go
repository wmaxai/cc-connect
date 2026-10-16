package codex

// appserver_conn.go — shared `codex app-server` processes.
//
// Upstream cc-connect spawns a private app-server child for every live agent
// session, so N 入口 = N children (and `--listen ws://127.0.0.1:3845` made them
// fight over one port). hermes' codex-sdk plugin shows the shape we want: one
// long-lived app-server, N concurrent Codex threads multiplexed on that single
// stdio stream, each thread pinned to its own cwd/provider through
// `thread/start` / `thread/resume` / `turn/start` params.
//
// Both shapes stay available, mirroring hermes' `multiplex_profiles`:
//
//   - app_server_scope = "shared"  (default) one app-server for the process
//   - app_server_scope = "process" a private app-server per agent session
//
// This file owns the shared side: the pool, the child process, the JSON-RPC
// transport and the threadId routing. Sessions keep their per-conversation
// state and call into a conn for every request.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// appServerScopes
const (
	appServerScopeShared  = "shared"
	appServerScopeProcess = "process"
)

func normalizeAppServerScope(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", appServerScopeShared, "one", "multiplex":
		return appServerScopeShared
	case appServerScopeProcess, "private", "dedicated", "per_session", "per-session":
		return appServerScopeProcess
	default:
		return appServerScopeShared
	}
}

// appServerProviderDef is one `[model_providers.<name>]` the app-server must
// know about. The API key travels in the child's environment (env_key), exactly
// like hermes' codex-sdk plugin: provider registration can only happen when the
// child starts, so the pool registers every known provider up front and each
// thread then picks one by name.
type appServerProviderDef struct {
	Name    string
	BaseURL string
	WireAPI string
	APIKey  string
	Headers map[string]string
	EnvKey  string // env var the child reads this provider's key from
}

// appServerProviderEnvKey maps a provider name to the env var carrying its key
// inside a shared child, e.g. "shengsuanyun" -> CC_CODEX_KEY_SHENGSUANYUN.
func appServerProviderEnvKey(name string) string {
	var sb strings.Builder
	sb.WriteString("CC_CODEX_KEY_")
	for _, r := range strings.ToUpper(strings.TrimSpace(name)) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			sb.WriteRune(r)
		default:
			sb.WriteRune('_')
		}
	}
	return sb.String()
}

var (
	appServerProvidersMu sync.Mutex
	appServerProviders   = map[string]appServerProviderDef{}
)

// registerAppServerProviders records provider definitions so the next app-server
// spawn registers them all. Called whenever a codex agent's provider list is
// (re)wired: at startup for every project, and on runtime provider edits.
func registerAppServerProviders(defs []appServerProviderDef) {
	if len(defs) == 0 {
		return
	}
	added := false
	appServerProvidersMu.Lock()
	for _, def := range defs {
		name := strings.TrimSpace(def.Name)
		baseURL := strings.TrimSpace(def.BaseURL)
		if name == "" || baseURL == "" {
			continue
		}
		def.Name = name
		def.BaseURL = baseURL
		def.WireAPI = strings.TrimSpace(def.WireAPI)
		def.EnvKey = appServerProviderEnvKey(name)
		if existing, ok := appServerProviders[name]; ok {
			if existing.BaseURL != baseURL {
				slog.Warn("codex app-server: provider label reused with a different base_url; keeping the first",
					"provider", name, "kept", existing.BaseURL, "ignored", baseURL)
			}
			continue
		}
		appServerProviders[name] = def
		added = true
	}
	appServerProvidersMu.Unlock()

	if added {
		// A provider can only be registered on the child at spawn time, so a new
		// one takes effect on the next process; retry the acquire then.
		recycleSharedAppServers("provider set changed")
	}
}

func registeredAppServerProviders() []appServerProviderDef {
	appServerProvidersMu.Lock()
	out := make([]appServerProviderDef, 0, len(appServerProviders))
	for _, def := range appServerProviders {
		out = append(out, def)
	}
	appServerProvidersMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func appServerProviderFingerprint(defs []appServerProviderDef) string {
	parts := make([]string, 0, len(defs))
	for _, def := range defs {
		parts = append(parts, def.Name+"|"+def.BaseURL+"|"+def.WireAPI+"|"+def.APIKey)
	}
	return shortHash(strings.Join(parts, "\n"))
}

func shortHash(in string) string {
	sum := sha256.Sum256([]byte(in))
	return hex.EncodeToString(sum[:8])
}

// appServerConnBase describes everything about the child that cannot be
// overridden per thread: which binary, which transport, which CODEX_HOME, and
// whether the caller wants to share it at all.
//
// Provider keys and per-project env are intentionally NOT part of this base:
// they come from the process-wide provider registry plus the sessions that
// requested the conn, and only pin the identity when they genuinely differ.
type appServerConnBase struct {
	cliBin       string
	cliExtraArgs []string
	url          string
	codexHome    string
	workDir      string
	env          []string
	scope        string
}

// appServerIdentityIgnoredEnv are the env vars that carry per-project provider
// credentials. They must not split a shared process: each provider's key is
// injected under its own CC_CODEX_KEY_* name instead.
var appServerIdentityIgnoredEnv = map[string]bool{
	"OPENAI_API_KEY":  true,
	"OPENAI_BASE_URL": true,
	"CODEX_HOME":      true,
}

func appServerEnvFingerprint(env []string) string {
	parts := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if appServerIdentityIgnoredEnv[strings.ToUpper(strings.TrimSpace(name))] {
			continue
		}
		parts = append(parts, kv)
	}
	sort.Strings(parts)
	return shortHash(strings.Join(parts, "\n"))
}

// appServerEnvForScope decides which env the child may inherit.
//
// Credentials cannot be per-project inside a shared child, so a project with an
// active provider drops its OPENAI_API_KEY / OPENAI_BASE_URL: those keys travel
// per thread instead — the registry injects each provider's key under its own
// CC_CODEX_KEY_* name and thread/start picks one by modelProvider. A project
// with no provider keeps them, so the child still sees what that project set.
func appServerEnvForScope(env []string, scope, provider string) []string {
	if scope != appServerScopeShared || strings.TrimSpace(provider) == "" {
		return env
	}
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if appServerIdentityIgnoredEnv[strings.ToUpper(strings.TrimSpace(name))] {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// appServerProviderDefsFromCore converts the agent's provider list into the
// child's `model_providers.*` registrations.
func appServerProviderDefsFromCore(providers []core.ProviderConfig) []appServerProviderDef {
	defs := make([]appServerProviderDef, 0, len(providers))
	for _, p := range providers {
		name := strings.TrimSpace(p.Name)
		baseURL := strings.TrimSpace(p.BaseURL)
		if name == "" || baseURL == "" {
			continue
		}
		defs = append(defs, appServerProviderDef{
			Name:    name,
			BaseURL: baseURL,
			WireAPI: p.CodexWireAPI,
			APIKey:  p.APIKey,
			Headers: p.CodexHTTPHeaders,
			EnvKey:  appServerProviderEnvKey(name),
		})
	}
	return defs
}

// appServerConnIdentity is the pool key. Two projects whose base, env and
// provider set match share one child; anything that would change the child's own
// configuration — and therefore cannot be expressed per thread — splits them
// into separate children instead of silently leaking config across projects.
type appServerConnIdentity struct {
	base      appServerConnBase
	providers []appServerProviderDef
	envPrint  string
	provPrint string
}

func (id appServerConnIdentity) key() string {
	return strings.Join([]string{
		id.base.cliBin,
		strings.Join(id.base.cliExtraArgs, "\x00"),
		id.base.url,
		id.base.codexHome,
		id.workDirKey(),
		id.provPrint,
		id.envPrint,
	}, "\x1f")
}

// workDirKey keeps per-process scope from collapsing into one child: a private
// app-server belongs to exactly one session, and its cwd is part of that.
func (id appServerConnIdentity) workDirKey() string {
	if id.base.scope == appServerScopeProcess {
		return id.base.workDir
	}
	return ""
}

func (id appServerConnIdentity) describe() string {
	return fmt.Sprintf("scope=%s bin=%s url=%s codex_home=%s providers=%d env=%s",
		id.base.scope, id.base.cliBin, id.base.url, id.base.codexHome, len(id.providers), id.envPrint)
}

type appServerConn struct {
	id  appServerConnIdentity
	gen int64

	ctx    context.Context
	cancel context.CancelFunc

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	procMu sync.Mutex

	writeMu sync.Mutex

	nextID atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]chan rpcResponseEnvelope

	alive atomic.Bool

	sessionsMu sync.RWMutex
	sessions   map[*appServerSession]struct{}
	routes     map[string]*appServerSession

	wg        sync.WaitGroup
	closeOnce sync.Once

	draining atomic.Bool
	lastUsed atomic.Int64
	pooled   bool
}

const appServerRequestTimeout = 120 * time.Second

// appServerStopGrace is how long a closing child gets to exit on SIGTERM before
// it is killed. codex exits in well under a second; the margin is for a turn
// that is mid-tool-call.
const appServerStopGrace = 5 * time.Second

func (c *appServerConn) Generation() int64 { return c.gen }
func (c *appServerConn) Alive() bool       { return c.alive.Load() }
func (c *appServerConn) Draining() bool    { return c.draining.Load() }

// pid reports the shared child's process id (0 before it is spawned), for logs.
func (c *appServerConn) pid() int {
	c.procMu.Lock()
	defer c.procMu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		return c.cmd.Process.Pid
	}
	return 0
}

func (c *appServerConn) touch() { c.lastUsed.Store(time.Now().UnixNano()) }

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

type appServerPool struct {
	mu    sync.Mutex
	conns map[string]*appServerConn
	gen   int64
}

var sharedAppServerPool = &appServerPool{conns: map[string]*appServerConn{}}

// acquire returns the app-server for base, spawning it on first use (and
// respawning it after a crash). Provider list and env are snapshotted here,
// which is what maps one cc-connect onto one app-server in steady state.
//
// scope=process bypasses the pool entirely: the caller owns the child and closes
// it with its session, which is the pre-fork behaviour.
func (p *appServerPool) acquire(base appServerConnBase) (*appServerConn, error) {
	providers := registeredAppServerProviders()
	id := appServerConnIdentity{
		base:      base,
		providers: providers,
		envPrint:  appServerEnvFingerprint(base.env),
		provPrint: appServerProviderFingerprint(providers),
	}
	if id.base.scope == appServerScopeProcess {
		p.mu.Lock()
		p.gen++
		gen := p.gen
		p.mu.Unlock()
		return spawnAppServerConn(id, gen, false)
	}
	key := id.key()

	p.mu.Lock()
	conn := p.conns[key]
	if conn != nil && conn.Alive() && !conn.Draining() {
		p.mu.Unlock()
		conn.touch()
		return conn, nil
	}
	if conn != nil {
		delete(p.conns, key)
	}
	p.gen++
	gen := p.gen
	live := 0
	for _, existing := range p.conns {
		if existing.Alive() {
			live++
		}
	}
	p.mu.Unlock()

	if conn != nil {
		slog.Warn("codex app-server: previous shared process is gone, respawning", "identity", id.describe())
		go func() { _ = conn.Close() }()
	}
	if live > 0 {
		// Another project needed a different child configuration (different env
		// or a different provider set). That is the one case where "one
		// cc-connect = one app-server" cannot hold without leaking config across
		// projects; restarting cc-connect after a config edit collapses it back
		// to a single child.
		slog.Warn("codex app-server: config differs from the running shared process, starting another one",
			"identity", id.describe(), "running", live)
	}

	spawned, err := spawnAppServerConn(id, gen, true)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.conns[key] = spawned
	p.mu.Unlock()

	go p.reapIdleOnce()
	return spawned, nil
}

// release drops a drained conn once its last session goes away.
func (p *appServerPool) release(c *appServerConn) {
	if !c.pooled {
		return
	}
	p.mu.Lock()
	key := c.id.key()
	if p.conns[key] == c {
		delete(p.conns, key)
	}
	p.mu.Unlock()
}

const appServerDrainGrace = 10 * time.Minute

func (p *appServerPool) reapIdleOnce() {
	time.Sleep(appServerDrainGrace)
	p.mu.Lock()
	var doomed []*appServerConn
	for key, conn := range p.conns {
		if !conn.Draining() || conn.hasSessions() {
			continue
		}
		delete(p.conns, key)
		doomed = append(doomed, conn)
	}
	p.mu.Unlock()
	for _, conn := range doomed {
		slog.Info("codex app-server: draining shared process closed", "identity", conn.id.describe())
		_ = conn.Close()
	}
}

// recycleSharedAppServers retires every pooled child so that the next acquire
// spawns one carrying the current configuration. Sessions on a retired child
// keep their thread ids and re-resume through the crash-recovery path.
func recycleSharedAppServers(reason string) {
	sharedAppServerPool.mu.Lock()
	conns := make([]*appServerConn, 0, len(sharedAppServerPool.conns))
	for _, conn := range sharedAppServerPool.conns {
		conns = append(conns, conn)
	}
	sharedAppServerPool.conns = map[string]*appServerConn{}
	sharedAppServerPool.mu.Unlock()
	if len(conns) == 0 {
		return
	}
	slog.Info("codex app-server: retiring shared process", "reason", reason, "processes", len(conns))
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// shutdownSharedAppServers closes every pooled child on process shutdown.
func shutdownSharedAppServers() {
	recycleSharedAppServers("cc-connect shutting down")
}

// ---------------------------------------------------------------------------
// spawn
// ---------------------------------------------------------------------------

func spawnAppServerConn(id appServerConnIdentity, gen int64, pooled bool) (*appServerConn, error) {
	base := id.base
	ctx, cancel := context.WithCancel(context.Background())
	c := &appServerConn{
		id:       id,
		gen:      gen,
		ctx:      ctx,
		cancel:   cancel,
		pending:  map[int64]chan rpcResponseEnvelope{},
		sessions: map[*appServerSession]struct{}{},
		routes:   map[string]*appServerSession{},
		pooled:   pooled,
	}
	c.touch()

	args := append([]string(nil), base.cliExtraArgs...)
	args = append(args, "app-server")
	if listenURL := appServerListenURL(base.url); listenURL != "" {
		args = append(args, "--listen", listenURL)
	}
	// Register every known provider on the child, then pick one per thread via
	// thread/start's modelProvider. This is the multiplex shape hermes uses.
	for _, def := range id.providers {
		args = append(args, "-c", fmt.Sprintf("model_providers.%s.name=%q", def.Name, def.Name))
		args = append(args, "-c", fmt.Sprintf("model_providers.%s.base_url=%q", def.Name, def.BaseURL))
		args = append(args, "-c", fmt.Sprintf("model_providers.%s.env_key=%q", def.Name, def.EnvKey))
		if def.WireAPI != "" {
			args = append(args, "-c", fmt.Sprintf("model_providers.%s.wire_api=%q", def.Name, def.WireAPI))
		}
	}

	bin, err := resolveCodexExecutable(base.cliBin)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex app-server resolve CLI: %w", err)
	}

	workDir := strings.TrimSpace(base.workDir)
	if workDir == "" {
		workDir = "."
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = workDir
	// Own process group plus a polite Cancel: closing the conn SIGTERMs the
	// whole group (codex releases its thread writer locks on a clean exit) and
	// only falls back to a kill if the group ignores it.
	prepareCmdForKill(cmd)
	cmd.Cancel = func() error { return gracefulStopCmd(cmd) }
	cmd.WaitDelay = appServerStopGrace

	env := append([]string(nil), base.env...)
	for _, def := range id.providers {
		if def.APIKey != "" {
			env = append(env, def.EnvKey+"="+def.APIKey)
		}
	}
	if base.codexHome != "" {
		env = append(env, "CODEX_HOME="+base.codexHome)
	}
	if len(env) > 0 {
		cmd.Env = core.MergeEnv(os.Environ(), env)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex app-server stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex app-server stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex app-server stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("codex app-server start: %w", err)
	}

	c.cmd = cmd
	c.stdin = stdin
	c.alive.Store(true)

	slog.Info("codex app-server process started",
		"transport", "stdio", "pid", cmd.Process.Pid, "generation", gen,
		"identity", id.describe(), "work_dir", workDir, "providers", len(id.providers))

	c.wg.Add(3)
	go c.readLoop(stdout)
	go c.stderrLoop(stderr)
	go c.waitLoop()

	if err := c.initialize(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *appServerConn) initialize() error {
	params := map[string]any{
		"clientInfo": map[string]any{
			"name":    "cc-connect-codex-agent",
			"title":   "CC Connect Codex Agent",
			"version": "0.1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi": true,
			"optOutNotificationMethods": []string{
				"command/exec/outputDelta",
				"item/agentMessage/delta",
				"item/plan/delta",
				"item/fileChange/outputDelta",
				"item/reasoning/summaryTextDelta",
				"item/reasoning/textDelta",
			},
		},
	}

	var resp initResponse
	if err := c.request("initialize", params, &resp); err != nil {
		return fmt.Errorf("codex app-server initialize: %w", err)
	}
	if err := c.notify("initialized", nil); err != nil {
		return fmt.Errorf("codex app-server initialized notify: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// session registry / routing
// ---------------------------------------------------------------------------

func (c *appServerConn) attach(s *appServerSession) {
	c.sessionsMu.Lock()
	c.sessions[s] = struct{}{}
	c.sessionsMu.Unlock()
	c.touch()
}

func (c *appServerConn) detach(s *appServerSession) {
	c.sessionsMu.Lock()
	delete(c.sessions, s)
	for threadID, owner := range c.routes {
		if owner == s {
			delete(c.routes, threadID)
		}
	}
	empty := len(c.sessions) == 0
	c.sessionsMu.Unlock()
	c.touch()
	if empty && c.Draining() {
		sharedAppServerPool.release(c)
	}
}

func (c *appServerConn) bindThread(threadID string, s *appServerSession) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return
	}
	c.sessionsMu.Lock()
	c.routes[threadID] = s
	c.sessionsMu.Unlock()
}

func (c *appServerConn) unbindThread(threadID string) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return
	}
	c.sessionsMu.Lock()
	delete(c.routes, threadID)
	c.sessionsMu.Unlock()
}

func (c *appServerConn) hasSessions() bool {
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	return len(c.sessions) > 0
}

func (c *appServerConn) sessionForThread(threadID string) *appServerSession {
	if threadID == "" {
		return nil
	}
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	return c.routes[threadID]
}

func (c *appServerConn) allSessions() []*appServerSession {
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	out := make([]*appServerSession, 0, len(c.sessions))
	for s := range c.sessions {
		out = append(out, s)
	}
	return out
}

func (c *appServerConn) forgetSession(s *appServerSession) {
	c.sessionsMu.Lock()
	delete(c.sessions, s)
	c.sessionsMu.Unlock()
}

// ---------------------------------------------------------------------------
// transport
// ---------------------------------------------------------------------------

func (c *appServerConn) readLoop(r io.Reader) {
	defer c.wg.Done()
	scanner := bufio.NewScanner(r)
	scanBuf := make([]byte, 0, 64*1024)
	const maxLineSize = 10 * 1024 * 1024 // 10MB
	scanner.Buffer(scanBuf, maxLineSize)

	for scanner.Scan() {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		data := scanner.Bytes()

		var probe map[string]json.RawMessage
		if err := json.Unmarshal(data, &probe); err != nil {
			slog.Debug("codex app-server: invalid JSON", "error", err)
			continue
		}

		_, hasID := probe["id"]
		_, hasMethod := probe["method"]

		switch {
		case hasID && !hasMethod:
			var resp rpcResponseEnvelope
			if err := json.Unmarshal(data, &resp); err != nil {
				slog.Debug("codex app-server: bad response envelope", "error", err)
				continue
			}
			c.handleResponse(resp)

		case hasID && hasMethod:
			c.routeServerRequest(probe)

		default:
			var notif rpcNotificationEnvelope
			if err := json.Unmarshal(data, &notif); err != nil {
				slog.Debug("codex app-server: bad notification envelope", "error", err)
				continue
			}
			c.routeNotification(notif.Method, notif.Params)
		}
	}

	err := scanner.Err()
	if err != nil {
		if c.ctx.Err() == nil && !errors.Is(err, io.EOF) {
			slog.Warn("codex app-server read failed", "error", err)
			if errors.Is(err, bufio.ErrTooLong) {
				c.failAll(fmt.Errorf("codex app-server line exceeds max size (%d bytes): %w", maxLineSize, err))
			} else {
				c.failAll(fmt.Errorf("codex app-server connection closed: %w", err))
			}
		}
	}
	c.shutdown(io.EOF)
}

func (c *appServerConn) stderrLoop(r io.Reader) {
	defer c.wg.Done()
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		slog.Debug("codex app-server stderr", "line", line)
	}
	if err := scanner.Err(); err != nil && c.ctx.Err() == nil {
		slog.Debug("codex app-server stderr read failed", "error", err)
	}
}

func (c *appServerConn) waitLoop() {
	defer c.wg.Done()

	c.procMu.Lock()
	cmd := c.cmd
	c.procMu.Unlock()
	if cmd == nil {
		return
	}

	err := cmd.Wait()
	if c.ctx.Err() == nil && err != nil {
		slog.Warn("codex app-server exited unexpectedly", "error", err, "identity", c.id.describe())
		c.failAll(fmt.Errorf("codex app-server exited: %w", err))
	}
	c.shutdown(err)
}

// shutdown marks the transport dead, wakes pending requests and tells every
// attached session. Sessions keep their thread ids and re-resume on their next
// turn, which spawns a fresh process through the pool.
func (c *appServerConn) shutdown(err error) {
	if !c.alive.Swap(false) {
		return
	}
	if err == nil {
		err = io.EOF
	}
	c.rejectPending(err)
	for _, s := range c.allSessions() {
		s.transportLost(err)
	}
	if c.pooled {
		sharedAppServerPool.mu.Lock()
		if existing := sharedAppServerPool.conns[c.id.key()]; existing == c {
			delete(sharedAppServerPool.conns, c.id.key())
		}
		sharedAppServerPool.mu.Unlock()
	}
}

// failAll marks the shared process as draining and fails the turns that were
// mid-flight, so a wedged child never leaves the other sessions hanging on it.
func (c *appServerConn) failAll(err error) {
	c.draining.Store(true)
	for _, s := range c.allSessions() {
		s.failRunningTurn(err)
	}
}

func (c *appServerConn) handleResponse(resp rpcResponseEnvelope) {
	id, ok := rpcIDToInt64(resp.ID)
	if !ok {
		return
	}

	c.pendingMu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.pendingMu.Unlock()

	if ch == nil {
		return
	}

	select {
	case ch <- resp:
	default:
	}
}

func (c *appServerConn) routeNotification(method string, paramsRaw json.RawMessage) {
	threadID := appServerThreadID(paramsRaw)
	if threadID != "" {
		if s := c.sessionForThread(threadID); s != nil {
			s.handleNotification(method, paramsRaw)
			return
		}
		slog.Debug("codex app-server: notification for unknown thread dropped", "method", method, "thread_id", threadID)
		return
	}
	// Process-scoped notifications (rate limits, config warnings, errors) apply
	// to everyone sharing this process.
	for _, s := range c.allSessions() {
		s.handleNotification(method, paramsRaw)
	}
}

func appServerThreadID(paramsRaw json.RawMessage) string {
	if len(paramsRaw) == 0 {
		return ""
	}
	var probe struct {
		ThreadID string `json:"threadId"`
	}
	if err := json.Unmarshal(paramsRaw, &probe); err != nil {
		return ""
	}
	return strings.TrimSpace(probe.ThreadID)
}

func (c *appServerConn) routeServerRequest(probe map[string]json.RawMessage) {
	rawID := probe["id"]
	var method string
	if err := json.Unmarshal(probe["method"], &method); err != nil {
		return
	}
	params := probe["params"]

	if s := c.sessionForThread(appServerThreadID(params)); s != nil {
		s.handleServerRequest(c, rawID, method, params)
		return
	}

	// No live session owns this thread any more (its session closed while the
	// turn was still running). Answer with the safe default so a turn never
	// stalls on an approval nobody can see.
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		_ = c.writeJSON(map[string]any{"jsonrpc": "2.0", "id": rawID,
			"result": map[string]any{"decision": "decline"}})
	case "item/permissions/requestApproval":
		_ = c.writeJSON(map[string]any{"jsonrpc": "2.0", "id": rawID,
			"result": map[string]any{"permissions": map[string]any{}}})
	case "item/tool/requestUserInput":
		_ = c.writeJSON(map[string]any{"jsonrpc": "2.0", "id": rawID,
			"result": appServerRequestUserInputResponse{Answers: map[string]appServerRequestUserInputAnswer{}}})
	default:
		_ = c.writeJSON(map[string]any{"jsonrpc": "2.0", "id": rawID,
			"error": map[string]any{"code": -32601, "message": "method not found"}})
	}
}

func (c *appServerConn) request(method string, params any, out any) error {
	return c.requestWithTimeout(method, params, out, appServerRequestTimeout)
}

func (c *appServerConn) requestWithTimeout(method string, params any, out any, timeout time.Duration) error {
	id := c.nextID.Add(1)
	ch := make(chan rpcResponseEnvelope, 1)

	c.pendingMu.Lock()
	if c.pending == nil {
		c.pending = map[int64]chan rpcResponseEnvelope{}
	}
	c.pending[id] = ch
	c.pendingMu.Unlock()

	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}

	deadline := time.Now().Add(timeout)
	if err := c.writeJSONWithTimeout(method, payload, timeout); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return err
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("%s timed out", method)
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()
	ctxDone := c.contextDone()
	select {
	case resp := <-ch:
		c.touch()
		if resp.Error != nil {
			return fmt.Errorf("%s", strings.TrimSpace(resp.Error.Message))
		}
		if out != nil {
			if err := json.Unmarshal(resp.Result, out); err != nil {
				return fmt.Errorf("decode %s response: %w", method, err)
			}
		}
		return nil
	case <-ctxDone:
		return c.contextErr()
	case <-timer.C:
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("%s timed out", method)
	}
}

func (c *appServerConn) writeJSONWithTimeout(method string, v any, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- c.writeJSON(v)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ctxDone := c.contextDone()
	select {
	case err := <-done:
		return err
	case <-ctxDone:
		return c.contextErr()
	case <-timer.C:
		err := fmt.Errorf("%s write timed out", method)
		// A blocked stdin write means the child stopped reading its pipe: it is
		// wedged for every session sharing it, so the pool gets a fresh process
		// and the surviving sessions re-resume their threads.
		slog.Warn("codex app-server write timed out, recycling the process",
			"method", method, "timeout", timeout, "identity", c.id.describe())
		c.abortTransport()
		return err
	}
}

func (c *appServerConn) contextDone() <-chan struct{} {
	if c.ctx == nil {
		return nil
	}
	return c.ctx.Done()
}

func (c *appServerConn) contextErr() error {
	if c.ctx == nil {
		return context.Canceled
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

// stopProcess asks the child's process group to exit. cmd.Cancel turns this into
// a SIGTERM for the whole group, and cmd.WaitDelay kills it if that is ignored.
func (c *appServerConn) stopProcess() {
	if c.cancel != nil {
		c.cancel()
	}
	c.procMu.Lock()
	if c.stdin != nil {
		_ = c.stdin.Close()
		c.stdin = nil
	}
	c.procMu.Unlock()
}

// abortTransport stops the child immediately. Used when the pipe is wedged: the
// group is killed rather than asked, so a child that stopped reading stdin
// cannot linger as an orphan holding writer locks.
func (c *appServerConn) abortTransport() {
	c.alive.Store(false)
	if c.cancel != nil {
		c.cancel()
	}

	c.procMu.Lock()
	if c.stdin != nil {
		_ = c.stdin.Close()
		c.stdin = nil
	}
	cmd := c.cmd
	c.procMu.Unlock()

	_ = forceKillCmd(cmd)
}

func (c *appServerConn) notify(method string, params any) error {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
	}
	if params != nil {
		payload["params"] = params
	}
	return c.writeJSON(payload)
}

func (c *appServerConn) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("codex app-server encode: %w", err)
	}

	c.procMu.Lock()
	stdin := c.stdin
	c.procMu.Unlock()
	if stdin == nil {
		return fmt.Errorf("codex app-server connection is closed")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := stdin.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("codex app-server write: %w", err)
	}
	return nil
}

func (c *appServerConn) rejectPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for id, ch := range c.pending {
		delete(c.pending, id)
		select {
		case ch <- rpcResponseEnvelope{ID: id, Error: &rpcError{Message: err.Error()}}:
		default:
		}
	}
}

// Close shuts this process down. Attached sessions keep their thread ids and
// re-resume on the next turn.
//
// The shutdown is polite on purpose: a SIGTERM'd codex releases its per-thread
// writer locks and finishes the rollout it was writing, so the threads this
// process served can be resumed by the replacement straight away. A kill leaves
// those locks held by whatever is left of the child, and threads whose rollout
// was mid-write read back as incomplete.
func (c *appServerConn) Close() error {
	if c.cancel == nil {
		return nil
	}
	c.draining.Store(true)
	c.alive.Store(false)

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	c.stopProcess()
	select {
	case <-done:
	case <-time.After(appServerStopGrace + 2*time.Second):
		slog.Warn("codex app-server did not exit on SIGTERM, killing its process group", "identity", c.id.describe())
		c.procMu.Lock()
		cmd := c.cmd
		c.procMu.Unlock()
		_ = forceKillCmd(cmd)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}

	c.closeOnce.Do(func() {})
	return nil
}
