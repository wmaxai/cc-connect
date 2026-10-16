package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type rpcResponseEnvelope struct {
	ID     any             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcNotificationEnvelope struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type initResponse struct {
	ProtocolVersion string `json:"protocolVersion"`
}

type threadStartResponse struct {
	Cwd             string  `json:"cwd"`
	Model           string  `json:"model"`
	ModelProvider   string  `json:"modelProvider"`
	ReasoningEffort *string `json:"reasoningEffort"`
	Thread          struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type threadResumeResponse struct {
	Cwd             string  `json:"cwd"`
	Model           string  `json:"model"`
	ModelProvider   string  `json:"modelProvider"`
	ReasoningEffort *string `json:"reasoningEffort"`
	Thread          struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type turnStartResponse struct {
	Turn struct {
		ID string `json:"id"`
	} `json:"turn"`
}

type turnSteerResponse struct {
	TurnID string `json:"turnId"`
}

// SteersMidTurn reports that this session absorbs input sent while a turn is
// already running through Codex's native `turn/steer` request, rather than
// queueing it or writing to stdin. The engine uses this to decide whether a
// busy-session message can be applied to the running turn.
func (s *appServerSession) SteersMidTurn() bool { return true }

type turnNotification struct {
	ThreadID string `json:"threadId"`
	Turn     struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"turn"`
}

type itemNotification struct {
	ThreadID string         `json:"threadId"`
	TurnID   string         `json:"turnId"`
	Item     map[string]any `json:"item"`
}

type errorNotification struct {
	Message string `json:"message"`
}

type appServerRateLimitsResponse struct {
	RateLimits          appServerRateLimitSnapshot            `json:"rateLimits"`
	RateLimitsByLimitID map[string]appServerRateLimitSnapshot `json:"rateLimitsByLimitId"`
}

type appServerRateLimitSnapshot struct {
	LimitID   string                    `json:"limitId"`
	LimitName string                    `json:"limitName"`
	PlanType  string                    `json:"planType"`
	Primary   *appServerRateLimitWindow `json:"primary"`
	Secondary *appServerRateLimitWindow `json:"secondary"`
	Credits   *appServerCreditsSnapshot `json:"credits"`
}

type appServerRateLimitWindow struct {
	UsedPercent        int   `json:"usedPercent"`
	WindowDurationMins int   `json:"windowDurationMins"`
	ResetsAt           int64 `json:"resetsAt"`
}

type appServerCreditsSnapshot struct {
	Balance    *string `json:"balance"`
	HasCredits bool    `json:"hasCredits"`
	Unlimited  bool    `json:"unlimited"`
}

type appServerRequestUserInputParams struct {
	ThreadID  string                              `json:"threadId"`
	TurnID    string                              `json:"turnId"`
	ItemID    string                              `json:"itemId"`
	Questions []appServerRequestUserInputQuestion `json:"questions"`
}

type appServerRequestUserInputQuestion struct {
	ID       string                            `json:"id"`
	Header   string                            `json:"header"`
	Question string                            `json:"question"`
	IsOther  bool                              `json:"isOther"`
	IsSecret bool                              `json:"isSecret"`
	Options  []appServerRequestUserInputOption `json:"options"`
}

type appServerRequestUserInputOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type appServerRequestUserInputResponse struct {
	Answers map[string]appServerRequestUserInputAnswer `json:"answers"`
}

type appServerRequestUserInputAnswer struct {
	Answers []string `json:"answers"`
}

type appServerSession struct {
	workDir        string
	model          string
	effort         string
	mode           string
	baseURL        string
	modelProvider  string
	promptPreamble string

	events chan core.Event
	// eventsMu guards the channel close: the shared app-server routes
	// notifications from a conn goroutine, so a detach/close racing an in-flight
	// notification must not panic on a closed channel.
	eventsMu   sync.Mutex
	eventsDone bool

	ctx    context.Context
	cancel context.CancelFunc

	// One `codex app-server` per cc-connect (see appserver_conn.go) serves every
	// session; this session owns a single Codex thread on it. connBase describes
	// how to (re)acquire that process: after a crash, or after a config change
	// that needs a differently configured child.
	connMu   sync.Mutex
	conn     *appServerConn
	connBase appServerConnBase
	// pinned marks a transport injected by a test: never re-acquire.
	pinned bool

	attachedGen atomic.Int64

	approvalsMu      sync.Mutex
	pendingApprovals map[string]chan core.PermissionResult

	threadID atomic.Value
	alive    atomic.Bool

	closeOnce sync.Once

	stateMu      sync.Mutex
	pendingMsgs  []string
	currentTurn  string
	preambleSent bool

	runtimeMu sync.RWMutex
	usage     *core.UsageReport
	context   *core.ContextUsage
}

const (
	appServerUsageRefreshTimeout = 1500 * time.Millisecond
)

// appServerAttachAttempts bounds the retries for a thread that is still held by
// the process we just replaced. A thread writer lock is codex' guard against two
// processes writing one conversation; it is released when its owner exits
// cleanly, so a busy lock right after a restart means the old app-server (or the
// wrapper that spawned it) has not finished dying yet.
const appServerAttachAttempts = 3

// Kept as a variable only so tests do not have to wait out the backoff.
var appServerAttachRetryDelay = 1200 * time.Millisecond

func isThreadWriterBusy(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already has an active writer")
}

// codex answers "no rollout found" when the thread id has no conversation file
// in this CODEX_HOME: freshly created threads that never ran a turn, or a
// session whose rollout lives in a different home. Such a thread carries no
// history, so starting a new one loses nothing.
func isThreadRolloutMissing(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no rollout found")
}

// resumeThread resumes a thread, retrying the one failure that is worth waiting
// out: a writer lock left by the process we just replaced.
func (s *appServerSession) resumeThread(c *appServerConn, threadID string, params map[string]any) (threadResumeResponse, error) {
	var resp threadResumeResponse
	var err error
	for attempt := 1; attempt <= appServerAttachAttempts; attempt++ {
		err = c.request("thread/resume", params, &resp)
		if err == nil || !isThreadWriterBusy(err) {
			return resp, err
		}
		if attempt < appServerAttachAttempts {
			slog.Warn("codex app-server: thread is still held by a previous writer, retrying",
				"thread_id", threadID, "attempt", attempt, "error", err)
			time.Sleep(appServerAttachRetryDelay)
		}
	}
	return resp, fmt.Errorf("codex session is still held by a previous codex process, please retry in a moment: %w", err)
}

func newAppServerSession(ctx context.Context, base appServerConnBase, workDir, model, effort, mode, resumeID, baseURL, modelProvider string, systemPrompt string, appendPrompt string) (*appServerSession, error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	s := &appServerSession{
		connBase:         base,
		workDir:          workDir,
		model:            model,
		effort:           effort,
		mode:             mode,
		baseURL:          baseURL,
		modelProvider:    modelProvider,
		promptPreamble:   buildCodexPromptPreamble(systemPrompt, appendPrompt),
		events:           make(chan core.Event, 128),
		ctx:              sessionCtx,
		cancel:           cancel,
		pendingApprovals: make(map[string]chan core.PermissionResult),
		preambleSent:     resumeID != "" && resumeID != core.ContinueSession,
	}
	s.alive.Store(true)

	if err := s.ensureThread(resumeID); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.refreshUsage(context.Background()); err != nil {
		slog.Debug("codex app-server: initial rate limit fetch failed", "error", err)
	}

	return s, nil
}

// transport returns the shared app-server this session talks to, acquiring (or
// re-acquiring) it from the pool when the previous process died.
func (s *appServerSession) transport() (*appServerConn, error) {
	s.connMu.Lock()
	current := s.conn
	pinned := s.pinned
	s.connMu.Unlock()

	if current != nil && current.Alive() {
		return current, nil
	}
	if pinned {
		if current == nil {
			return nil, fmt.Errorf("codex app-server session has no transport")
		}
		return current, nil
	}

	fresh, err := sharedAppServerPool.acquire(s.connBase)
	if err != nil {
		return nil, err
	}
	s.connMu.Lock()
	if s.conn != fresh {
		s.conn = fresh
		s.attachedGen.Store(0)
	}
	s.connMu.Unlock()
	if current != fresh {
		fresh.attach(s)
	}
	return fresh, nil
}

func (s *appServerSession) currentConn() *appServerConn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn
}

// transportLost is called by the conn when the shared process died. The thread
// itself survives on disk, so the next turn re-resumes it on a fresh process.
func (s *appServerSession) transportLost(err error) {
	s.attachedGen.Store(0)
	if err != nil {
		s.failRunningTurn(fmt.Errorf("codex app-server: %w", err))
	}
}

// failRunningTurn surfaces a transport failure to whoever is waiting on the
// current turn instead of letting the request time out silently.
func (s *appServerSession) failRunningTurn(err error) {
	if err == nil {
		return
	}
	s.stateMu.Lock()
	running := s.currentTurn != ""
	s.stateMu.Unlock()
	if !running {
		return
	}
	s.failTurn(err)
}

// appServerListenURL returns the --listen value for the spawn. A stdio
// transport ("stdio" / "stdio://", case-insensitive, matching
// normalizeAppServerURL's EqualFold handling) must not open a listener:
// on codex 0.152+ any --listen value switches the app-server to serve the
// protocol over the listener only, leaving stdio unresponsive and timing
// out every initialize (see #1781).
func appServerListenURL(url string) string {
	listenURL := strings.TrimSpace(url)
	if strings.EqualFold(listenURL, "stdio://") || strings.EqualFold(listenURL, "stdio") {
		return ""
	}
	return listenURL
}

func (s *appServerSession) ensureThread(resumeID string) error {
	c, err := s.transport()
	if err != nil {
		return err
	}
	if resumeID != "" && resumeID != core.ContinueSession {
		params := s.threadRequestParams()
		params["threadId"] = resumeID
		params["persistExtendedHistory"] = true

		resp, err := s.resumeThread(c, resumeID, params)
		if err == nil {
			if resp.Thread.ID == "" {
				return fmt.Errorf("codex app-server resume returned empty thread id")
			}
			s.applyThreadRuntimeState(resp.Cwd, resp.Model, resp.ReasoningEffort)
			s.threadID.Store(resp.Thread.ID)
			s.bindTo(c)
			slog.Info("codex app-server thread resumed", "thread_id", resp.Thread.ID,
				"pid", c.pid(), "generation", c.Generation())
			return nil
		}
		if !isThreadRolloutMissing(err) {
			return err
		}
		// Nothing on disk to resume: the thread never wrote a rollout, or its
		// rollout belongs to a different CODEX_HOME. A fresh thread carries the
		// same (empty) history, so the session stays usable.
		slog.Warn("codex app-server: no rollout for the requested session, starting a new thread",
			"thread_id", resumeID, "error", err)
		s.stateMu.Lock()
		s.preambleSent = false
		s.stateMu.Unlock()
	}

	var resp threadStartResponse
	if err := c.request("thread/start", s.threadRequestParams(), &resp); err != nil {
		return err
	}
	if resp.Thread.ID == "" {
		return fmt.Errorf("codex app-server start returned empty thread id")
	}
	s.applyThreadRuntimeState(resp.Cwd, resp.Model, resp.ReasoningEffort)
	if provider := strings.TrimSpace(resp.ModelProvider); provider != "" {
		s.modelProvider = provider
	}
	s.threadID.Store(resp.Thread.ID)
	s.bindTo(c)
	slog.Info("codex app-server thread started", "thread_id", resp.Thread.ID,
		"pid", c.pid(), "generation", c.Generation(), "cwd", resp.Cwd,
		"model_provider", s.modelProvider)
	return nil
}

// bindTo records that this session's thread lives on c at its current
// generation, so a later turn can tell whether the process was replaced.
func (s *appServerSession) bindTo(c *appServerConn) {
	threadID := s.CurrentSessionID()
	c.bindThread(threadID, s)
	s.attachedGen.Store(c.Generation())
}

// ensureThreadAttached makes sure the thread is served by a live process. After
// a crash (or a config change that spawned a replacement) the thread is
// re-resumed, which is the same path a fresh cc-connect takes for a stored
// session id.
func (s *appServerSession) ensureThreadAttached() (*appServerConn, error) {
	c, err := s.transport()
	if err != nil {
		return nil, err
	}
	threadID := s.CurrentSessionID()
	if threadID == "" {
		return c, nil
	}
	if s.attachedGen.Load() == c.Generation() {
		return c, nil
	}

	params := s.threadRequestParams()
	params["threadId"] = threadID
	params["persistExtendedHistory"] = true

	if _, err := s.resumeThread(c, threadID, params); err != nil {
		if !isThreadRolloutMissing(err) {
			return nil, fmt.Errorf("codex app-server re-attach thread: %w", err)
		}
		// The thread the session was tracking has no rollout in this CODEX_HOME
		// (it never ran a turn, or the home changed with it). Re-attaching is
		// impossible, so continue on a fresh thread: an empty history is exactly
		// what there was.
		slog.Warn("codex app-server: thread has no rollout to re-attach, starting a new thread",
			"thread_id", threadID, "error", err)
		c.unbindThread(threadID)
		s.attachedGen.Store(0)
		s.threadID.Store("")
		s.stateMu.Lock()
		s.preambleSent = false
		s.stateMu.Unlock()
		if err := s.ensureThread(""); err != nil {
			return nil, err
		}
		return s.transport()
	}
	s.bindTo(c)
	slog.Info("codex app-server thread re-attached to shared process",
		"thread_id", threadID, "pid", c.pid(), "generation", c.Generation())
	return c, nil
}

func (s *appServerSession) threadRequestParams() map[string]any {
	params := map[string]any{
		"experimentalRawEvents":  false,
		"persistExtendedHistory": false,
	}
	// cwd and provider are per-thread now that one process serves every
	// project: this is what keeps two bots from sharing a working directory.
	if cwd := strings.TrimSpace(s.GetWorkDir()); cwd != "" {
		params["cwd"] = cwd
	}
	if model := s.GetModel(); model != "" {
		params["model"] = model
	}
	if provider := strings.TrimSpace(s.modelProvider); provider != "" {
		params["modelProvider"] = provider
	}
	if approval, sandbox := appServerModeSettings(s.mode); approval != "" {
		params["approvalPolicy"] = approval
		if sandbox != "" {
			params["sandbox"] = sandbox
		}
	}
	return params
}

func appServerModeSettings(mode string) (approval string, sandbox string) {
	switch normalizeMode(mode) {
	case "auto-edit", "full-auto":
		return "never", "workspace-write"
	case "yolo":
		return "never", "danger-full-access"
	default:
		return "on-request", "read-only"
	}
}

func (s *appServerSession) applyThreadRuntimeState(workDir, model string, effort *string) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if dir := strings.TrimSpace(workDir); dir != "" {
		s.workDir = dir
	}
	if m := strings.TrimSpace(model); m != "" {
		s.model = m
	}
	s.effort = normalizeRuntimeReasoningEffort(stringValue(effort))
}

func (s *appServerSession) refreshUsage(ctx context.Context) error {
	timeout := appServerUsageRefreshTimeout
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if until := time.Until(deadline); until > 0 && until < timeout {
				timeout = until
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if timeout <= 0 {
		return context.DeadlineExceeded
	}

	c, err := s.transport()
	if err != nil {
		return err
	}
	var resp appServerRateLimitsResponse
	if err := c.requestWithTimeout("account/rateLimits/read", map[string]any{}, &resp, timeout); err != nil {
		return err
	}
	s.storeUsage(mapAppServerRateLimits(resp))
	return nil
}

func (s *appServerSession) cachedUsage() *core.UsageReport {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return cloneUsageReport(s.usage)
}

func (s *appServerSession) cachedContextUsage() *core.ContextUsage {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return cloneContextUsage(s.context)
}

func (s *appServerSession) storeUsage(report *core.UsageReport) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.usage = cloneUsageReport(report)
}

func (s *appServerSession) storeContextUsage(usage *core.ContextUsage) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.context = cloneContextUsage(usage)
}

func (s *appServerSession) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !s.alive.Load() {
		return fmt.Errorf("session is closed")
	}

	if len(files) > 0 {
		filePaths := core.SaveFilesToDisk(s.workDir, messageID, files)
		prompt = core.AppendFileRefs(prompt, filePaths)
	}

	prompt, imagePaths, err := s.stageImages(prompt, images)
	if err != nil {
		return err
	}

	s.stateMu.Lock()
	activeTurn := s.currentTurn
	if activeTurn == "" && !s.preambleSent {
		prompt = prependCodexPromptPreamble(prompt, s.promptPreamble)
		s.preambleSent = true
	}
	s.stateMu.Unlock()

	threadID := s.CurrentSessionID()
	if threadID == "" {
		return fmt.Errorf("codex app-server thread id is empty")
	}

	input := make([]map[string]any, 0, 1+len(imagePaths))
	input = append(input, map[string]any{
		"type":          "text",
		"text":          prompt,
		"text_elements": []any{},
	})
	for _, path := range imagePaths {
		input = append(input, map[string]any{
			"type": "localImage",
			"path": path,
		})
	}

	if activeTurn != "" {
		return s.steerTurn(threadID, activeTurn, input)
	}
	return s.startTurn(threadID, input)
}

func (s *appServerSession) startTurn(threadID string, input []map[string]any) error {
	c, err := s.ensureThreadAttached()
	if err != nil {
		return err
	}
	params := map[string]any{
		"threadId": threadID,
		"input":    input,
	}
	if model := s.GetModel(); model != "" {
		params["model"] = model
	}
	if effort := s.GetReasoningEffort(); effort != "" {
		params["effort"] = effort
	}
	if approval, _ := appServerModeSettings(s.mode); approval != "" {
		params["approvalPolicy"] = approval
	}

	var resp turnStartResponse
	if err := c.request("turn/start", params, &resp); err != nil {
		return fmt.Errorf("codex app-server turn/start: %w", err)
	}
	if resp.Turn.ID == "" {
		return fmt.Errorf("codex app-server turn/start returned empty turn id")
	}

	s.stateMu.Lock()
	s.currentTurn = resp.Turn.ID
	s.pendingMsgs = s.pendingMsgs[:0]
	s.stateMu.Unlock()

	return nil
}

func (s *appServerSession) steerTurn(threadID, expectedTurnID string, input []map[string]any) error {
	c, err := s.ensureThreadAttached()
	if err != nil {
		return err
	}
	params := map[string]any{
		"threadId":       threadID,
		"expectedTurnId": expectedTurnID,
		"input":          input,
	}

	var resp turnSteerResponse
	if err := c.request("turn/steer", params, &resp); err != nil {
		return fmt.Errorf("codex app-server turn/steer: %w", err)
	}
	if resp.TurnID == "" {
		return fmt.Errorf("codex app-server turn/steer returned empty turn id")
	}
	if resp.TurnID != expectedTurnID {
		return fmt.Errorf("codex app-server turn/steer returned turn id %q, want %q", resp.TurnID, expectedTurnID)
	}
	return nil
}

func (s *appServerSession) stageImages(prompt string, images []core.ImageAttachment) (string, []string, error) {
	if len(images) == 0 {
		return prompt, nil, nil
	}

	imgDir := filepath.Join(s.workDir, ".cc-connect", "images")
	if err := os.MkdirAll(imgDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("codex app-server: create image dir: %w", err)
	}

	imagePaths := make([]string, 0, len(images))
	for i, img := range images {
		ext := codexImageExt(img.MimeType)
		fname := fmt.Sprintf("img_%d_%d%s", time.Now().UnixMilli(), i, ext)
		fpath := filepath.Join(imgDir, fname)
		if err := os.WriteFile(fpath, img.Data, 0o644); err != nil {
			return "", nil, fmt.Errorf("codex app-server: save image: %w", err)
		}
		imagePaths = append(imagePaths, fpath)
	}

	if strings.TrimSpace(prompt) == "" {
		prompt = "Please analyze the attached image(s)."
	}

	return prompt, imagePaths, nil
}

func (s *appServerSession) RespondPermission(requestID string, result core.PermissionResult) error {
	s.approvalsMu.Lock()
	ch := s.pendingApprovals[requestID]
	s.approvalsMu.Unlock()
	if ch == nil {
		return fmt.Errorf("codex app-server: no pending approval for request %s", requestID)
	}
	select {
	case ch <- result:
	default:
	}
	return nil
}

// handleServerRequest answers a server-initiated request that belongs to this
// session's thread. The conn has already resolved the threadId to this session,
// so every reply goes back out over that shared transport.
func (s *appServerSession) handleServerRequest(c *appServerConn, rawID json.RawMessage, method string, params json.RawMessage) {
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		s.handleApprovalRequest(c, rawID, method, params)
	case "item/permissions/requestApproval":
		s.handlePermissionsApproval(c, rawID, params)
	case "item/tool/requestUserInput":
		s.handleRequestUserInput(c, rawID, params)
	case "item/tool/call":
		s.handleDynamicToolCall(c, rawID, params)
	default:
		_ = c.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"error": map[string]any{"code": -32601, "message": "method not found"},
		})
	}
}

func (s *appServerSession) handleApprovalRequest(c *appServerConn, rawID json.RawMessage, method string, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params map[string]any
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return
	}

	toolName, toolInput := method, appServerJSON(params)
	switch method {
	case "item/commandExecution/requestApproval":
		toolName = "Bash"
		if cmd, _ := params["command"].(string); cmd != "" {
			toolInput = cmd
			if cwd, _ := params["cwd"].(string); cwd != "" {
				toolInput += "\n(in " + cwd + ")"
			}
		}
	case "item/fileChange/requestApproval":
		toolName = "Patch"
		if reason, _ := params["reason"].(string); reason != "" {
			toolInput = reason
		}
	}

	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     toolName,
		ToolInput:    toolInput,
		ToolInputRaw: params,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		decision := "decline"
		if strings.EqualFold(result.Behavior, "allow") {
			decision = "accept"
		}
		_ = c.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": map[string]any{"decision": decision},
		})
	}()
}

func (s *appServerSession) handlePermissionsApproval(c *appServerConn, rawID json.RawMessage, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params map[string]any
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return
	}

	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     "Permissions",
		ToolInput:    appServerJSON(params),
		ToolInputRaw: params,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		if strings.EqualFold(result.Behavior, "allow") {
			perms := params["permissions"]
			if perms == nil {
				perms = map[string]any{}
			}
			_ = c.writeJSON(map[string]any{
				"jsonrpc": "2.0", "id": rawID,
				"result": map[string]any{"permissions": perms, "scope": "turn"},
			})
		} else {
			_ = c.writeJSON(map[string]any{
				"jsonrpc": "2.0", "id": rawID,
				"result": map[string]any{"permissions": map[string]any{}},
			})
		}
	}()
}

func (s *appServerSession) handleRequestUserInput(c *appServerConn, rawID json.RawMessage, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params appServerRequestUserInputParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		_ = c.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"error": map[string]any{"code": -32602, "message": "invalid params"},
		})
		return
	}

	questions := appServerRequestUserInputQuestions(params.Questions)
	if len(questions) == 0 {
		_ = c.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": appServerRequestUserInputResponse{Answers: map[string]appServerRequestUserInputAnswer{}},
		})
		return
	}

	rawInput := appServerRequestUserInputRawInput(params)
	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     "AskUserQuestion",
		ToolInput:    appServerJSON(rawInput),
		ToolInputRaw: rawInput,
		Questions:    questions,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		response := appServerRequestUserInputResponseFromResult(params.Questions, result)
		_ = c.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": response,
		})
	}()
}

func (s *appServerSession) handleDynamicToolCall(c *appServerConn, rawID json.RawMessage, paramsRaw json.RawMessage) {
	_ = c.writeJSON(map[string]any{
		"jsonrpc": "2.0", "id": rawID,
		"result": map[string]any{
			"success":      false,
			"contentItems": []map[string]any{{"type": "inputText", "text": "tool not available on this client"}},
		},
	})
}

func appServerRequestUserInputQuestions(input []appServerRequestUserInputQuestion) []core.UserQuestion {
	questions := make([]core.UserQuestion, 0, len(input))
	for _, in := range input {
		questionText := strings.TrimSpace(in.Question)
		if questionText == "" {
			continue
		}
		q := core.UserQuestion{
			Question: questionText,
			Header:   strings.TrimSpace(in.Header),
		}
		for _, opt := range in.Options {
			q.Options = append(q.Options, core.UserQuestionOption{
				Label:       strings.TrimSpace(opt.Label),
				Description: strings.TrimSpace(opt.Description),
			})
		}
		questions = append(questions, q)
	}
	return questions
}

func appServerRequestUserInputRawInput(params appServerRequestUserInputParams) map[string]any {
	questions := make([]any, 0, len(params.Questions))
	for _, in := range params.Questions {
		q := map[string]any{
			"id":       in.ID,
			"header":   in.Header,
			"question": in.Question,
			"isOther":  in.IsOther,
			"isSecret": in.IsSecret,
			"options":  appServerRequestUserInputRawOptions(in.Options),
		}
		questions = append(questions, q)
	}
	return map[string]any{
		"threadId":  params.ThreadID,
		"turnId":    params.TurnID,
		"itemId":    params.ItemID,
		"questions": questions,
	}
}

func appServerRequestUserInputRawOptions(options []appServerRequestUserInputOption) []any {
	out := make([]any, 0, len(options))
	for _, opt := range options {
		out = append(out, map[string]any{
			"label":       opt.Label,
			"description": opt.Description,
		})
	}
	return out
}

func appServerRequestUserInputResponseFromResult(questions []appServerRequestUserInputQuestion, result core.PermissionResult) appServerRequestUserInputResponse {
	response := appServerRequestUserInputResponse{Answers: map[string]appServerRequestUserInputAnswer{}}
	if !strings.EqualFold(result.Behavior, "allow") {
		return response
	}

	answersRaw, _ := result.UpdatedInput["answers"].(map[string]any)
	if len(answersRaw) == 0 {
		return response
	}

	for _, q := range questions {
		id := strings.TrimSpace(q.ID)
		text := strings.TrimSpace(q.Question)
		if id == "" || text == "" {
			continue
		}
		values := appServerRequestUserInputAnswerValues(answersRaw[text])
		if len(values) == 0 {
			continue
		}
		response.Answers[id] = appServerRequestUserInputAnswer{Answers: values}
	}
	return response
}

func appServerRequestUserInputAnswerValues(raw any) []string {
	switch v := raw.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	case []string:
		values := make([]string, 0, len(v))
		for _, s := range v {
			if strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
		return values
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
		return values
	case map[string]any:
		return appServerRequestUserInputAnswerValues(v["answers"])
	case appServerRequestUserInputAnswer:
		return appServerRequestUserInputAnswerValues(v.Answers)
	default:
		return nil
	}
}

func (s *appServerSession) rejectPendingApprovals(_ error) {
	s.approvalsMu.Lock()
	defer s.approvalsMu.Unlock()
	for id, ch := range s.pendingApprovals {
		delete(s.pendingApprovals, id)
		select {
		case ch <- core.PermissionResult{Behavior: "deny"}:
		default:
		}
	}
}

func (s *appServerSession) Events() <-chan core.Event {
	return s.events
}

func (s *appServerSession) CurrentSessionID() string {
	v, _ := s.threadID.Load().(string)
	return v
}

func (s *appServerSession) GetWorkDir() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.workDir
}

func (s *appServerSession) GetModel() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return strings.TrimSpace(s.model)
}

func (s *appServerSession) GetReasoningEffort() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return strings.TrimSpace(s.effort)
}

func (s *appServerSession) GetUsage(ctx context.Context) (*core.UsageReport, error) {
	if err := s.refreshUsage(ctx); err != nil {
		if cached := s.cachedUsage(); cached != nil {
			return cached, nil
		}
		return nil, err
	}
	if cached := s.cachedUsage(); cached != nil {
		return cached, nil
	}
	return nil, fmt.Errorf("codex app-server usage unavailable")
}

func (s *appServerSession) GetContextUsage() *core.ContextUsage {
	return s.cachedContextUsage()
}

func (s *appServerSession) Alive() bool {
	return s.alive.Load()
}

func (s *appServerSession) Close() error {
	s.alive.Store(false)
	s.cancel()

	// A session owns only its thread: detaching from the shared app-server keeps
	// the process (and every other project's thread) alive. The pinned
	// per-session scope keeps the pre-fork ownership model, where closing the
	// session reaps the child it spawned.
	c := s.currentConn()
	if c != nil {
		c.detach(s)
		c.unbindThread(s.CurrentSessionID())
		if !c.pooled {
			_ = c.Close()
		}
	}
	s.rejectPendingApprovals(context.Canceled)

	s.eventsMu.Lock()
	if !s.eventsDone {
		s.eventsDone = true
		close(s.events)
	}
	s.eventsMu.Unlock()
	return nil
}

func (s *appServerSession) handleNotification(method string, paramsRaw json.RawMessage) {
	switch method {
	case "turn/started":
		var notif turnNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.stateMu.Lock()
			s.currentTurn = notif.Turn.ID
			s.pendingMsgs = s.pendingMsgs[:0]
			s.stateMu.Unlock()
			s.storeContextUsage(nil)
		}

	case "item/started":
		var notif itemNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.handleItemStarted(notif.Item)
		}

	case "item/completed":
		var notif itemNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.handleItemCompleted(notif.Item)
		}

	case "turn/completed":
		var notif turnNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			if strings.EqualFold(strings.TrimSpace(notif.Turn.Status), "failed") || notif.Turn.Error != nil {
				errMsg := ""
				if notif.Turn.Error != nil {
					errMsg = strings.TrimSpace(notif.Turn.Error.Message)
				}
				if errMsg == "" {
					errMsg = "turn failed (no details)"
				}
				s.failTurn(fmt.Errorf("%s", errMsg))
			} else {
				s.completeTurn()
			}
		}

	case "thread/status/changed":
		var notif struct {
			ThreadID string `json:"threadId"`
			Status   struct {
				Type string `json:"type"`
			} `json:"status"`
		}
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && notif.Status.Type == "idle" {
			// In codex 0.125+, thread going idle signals turn completion.
			s.completeTurn()
		}

	case "account/rateLimits/updated":
		var notif appServerRateLimitsResponse
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.storeUsage(mapAppServerRateLimits(notif))
		}

	case "thread/tokenUsage/updated":
		var notif appServerThreadTokenUsageNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.storeContextUsage(mapAppServerTokenUsage(notif))
		}

	case "error":
		var notif errorNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && strings.TrimSpace(notif.Message) != "" {
			s.emitError(fmt.Errorf("%s", notif.Message))
		}
	}
}

func (s *appServerSession) handleItemStarted(item map[string]any) {
	itemType, _ := item["type"].(string)
	if itemType == "" {
		return
	}

	switch itemType {
	case "agentMessage", "reasoning", "userMessage", "plan", "hookPrompt", "contextCompaction":
		return
	}

	s.flushPendingAsThinking()

	switch itemType {
	case "commandExecution":
		command, _ := item["command"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "Bash", ToolInput: command})

	case "mcpToolCall":
		server, _ := item["server"].(string)
		tool, _ := item["tool"].(string)
		name := strings.Trim(strings.Join([]string{server, tool}, ":"), ":")
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "MCP", ToolInput: name + "\n" + appServerJSON(item["arguments"])})

	case "webSearch":
		query, _ := item["query"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "WebSearch", ToolInput: query})

	case "dynamicToolCall":
		tool, _ := item["tool"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: tool, ToolInput: appServerJSON(item["arguments"])})

	case "fileChange":
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "Patch", ToolInput: appServerJSON(item["changes"])})
	}
}

func (s *appServerSession) handleItemCompleted(item map[string]any) {
	itemType, _ := item["type"].(string)
	if itemType == "" {
		return
	}

	switch itemType {
	case "reasoning":
		text := appServerReasoningText(item)
		if text != "" {
			s.emit(core.Event{Type: core.EventThinking, Content: text})
		}

	case "agentMessage":
		text, _ := item["text"].(string)
		if strings.TrimSpace(text) == "" {
			break
		}
		// Codex tags mid-turn assistant text with a MessagePhase. Publish
		// commentary as soon as the item completes: buffering it would demote it to
		// EventThinking at the next tool call (handleItemStarted ->
		// flushPendingAsThinking), and compact/quiet display modes drop thinking
		// messages — so a long turn looks frozen until it ends and a mid-turn
		// course correction gets no visible answer.
		//
		// Phaseless text and the turn's terminal answer keep the previous
		// buffering: the protocol says an absent phase means "phase unknown", and
		// completeTurn() still flushes the terminal answer as turn output.
		if appServerIsInterimAgentText(appServerItemPhase(item)) {
			s.emit(core.Event{Type: core.EventText, Content: text})
			break
		}
		s.stateMu.Lock()
		s.pendingMsgs = append(s.pendingMsgs, text)
		s.stateMu.Unlock()

	case "commandExecution":
		command, _ := item["command"].(string)
		status, _ := item["status"].(string)
		output, _ := item["aggregatedOutput"].(string)
		exitCode, hasExitCode := toInt(item["exitCode"])
		var exitCodePtr *int
		if hasExitCode {
			exitCodePtr = &exitCode
		}
		success := appServerToolSuccess(status, exitCodePtr)
		s.emit(core.Event{
			Type:         core.EventToolResult,
			ToolName:     "Bash",
			ToolInput:    command,
			ToolResult:   truncate(strings.TrimSpace(output), 500),
			ToolStatus:   strings.TrimSpace(status),
			ToolExitCode: exitCodePtr,
			ToolSuccess:  &success,
		})

	case "mcpToolCall":
		tool, _ := item["tool"].(string)
		status, _ := item["status"].(string)
		result := appServerJSON(item["result"])
		if errText := appServerJSON(item["error"]); strings.TrimSpace(errText) != "" && result == "" {
			result = errText
		}
		success := appServerToolSuccess(status, nil)
		s.emit(core.Event{
			Type:        core.EventToolResult,
			ToolName:    tool,
			ToolResult:  truncate(strings.TrimSpace(result), 500),
			ToolStatus:  strings.TrimSpace(status),
			ToolSuccess: &success,
		})

	case "webSearch":
		query, _ := item["query"].(string)
		s.emit(core.Event{
			Type:       core.EventToolResult,
			ToolName:   "WebSearch",
			ToolResult: truncate(strings.TrimSpace(query), 500),
		})

	case "dynamicToolCall":
		tool, _ := item["tool"].(string)
		status, _ := item["status"].(string)
		result := appServerDynamicToolText(item["contentItems"])
		success := appServerToolSuccess(status, nil)
		s.emit(core.Event{
			Type:        core.EventToolResult,
			ToolName:    tool,
			ToolResult:  truncate(strings.TrimSpace(result), 500),
			ToolStatus:  strings.TrimSpace(status),
			ToolSuccess: &success,
		})
	}
}

func appServerReasoningText(item map[string]any) string {
	var parts []string
	if summary, ok := item["summary"].([]any); ok {
		for _, entry := range summary {
			if text, ok := entry.(string); ok && strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
	}
	if len(parts) == 0 {
		if content, ok := item["content"].([]any); ok {
			for _, entry := range content {
				if text, ok := entry.(string); ok && strings.TrimSpace(text) != "" {
					parts = append(parts, text)
				}
			}
		}
	}
	return strings.Join(parts, "\n")
}

func appServerDynamicToolText(raw any) string {
	items, ok := raw.([]any)
	if !ok {
		return appServerJSON(raw)
	}
	var parts []string
	for _, entry := range items {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if text, _ := m["text"].(string); strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return appServerJSON(raw)
	}
	return strings.Join(parts, "\n")
}

// appServerItemPhase returns the MessagePhase Codex attached to an assistant
// text item ("commentary", "partial_answer" or "final_answer"). Providers do not
// emit it consistently, so an absent phase is reported as "" — phase unknown —
// and callers keep their compatibility behavior for those items.
func appServerItemPhase(item map[string]any) string {
	phase, _ := item["phase"].(string)
	return strings.ToLower(strings.TrimSpace(phase))
}

// appServerIsInterimAgentText reports whether an assistant text item is mid-turn
// output the user should see as it arrives, as opposed to the turn's terminal
// answer (or phaseless text, which stays on the buffered path).
func appServerIsInterimAgentText(phase string) bool {
	return phase == "commentary" || phase == "partial_answer"
}

func appServerToolSuccess(status string, exitCode *int) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	if exitCode != nil {
		return *exitCode == 0
	}
	return s == "completed" || s == "success" || s == "succeeded" || s == "ok"
}

func mapAppServerRateLimits(payload appServerRateLimitsResponse) *core.UsageReport {
	report := &core.UsageReport{Provider: "codex"}

	var snapshots []appServerRateLimitSnapshot
	if len(payload.RateLimitsByLimitID) > 0 {
		keys := make([]string, 0, len(payload.RateLimitsByLimitID))
		for key := range payload.RateLimitsByLimitID {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			snapshots = append(snapshots, payload.RateLimitsByLimitID[key])
		}
	} else if payload.RateLimits.LimitID != "" || payload.RateLimits.Primary != nil || payload.RateLimits.Secondary != nil || payload.RateLimits.Credits != nil {
		snapshots = append(snapshots, payload.RateLimits)
	}

	for _, snapshot := range snapshots {
		if report.Plan == "" && strings.TrimSpace(snapshot.PlanType) != "" {
			report.Plan = strings.TrimSpace(snapshot.PlanType)
		}
		if report.Credits == nil && snapshot.Credits != nil {
			report.Credits = &core.UsageCredits{
				HasCredits: snapshot.Credits.HasCredits,
				Unlimited:  snapshot.Credits.Unlimited,
			}
			if snapshot.Credits.Balance != nil {
				report.Credits.Balance = strings.TrimSpace(*snapshot.Credits.Balance)
			}
		}

		windows := appServerUsageWindows(snapshot)
		if len(windows) == 0 {
			continue
		}
		limitReached := false
		for _, window := range windows {
			if window.UsedPercent >= 100 {
				limitReached = true
				break
			}
		}

		report.Buckets = append(report.Buckets, core.UsageBucket{
			Name:         appServerBucketName(snapshot),
			Allowed:      !limitReached,
			LimitReached: limitReached,
			Windows:      windows,
		})
	}

	return report
}

func appServerBucketName(snapshot appServerRateLimitSnapshot) string {
	if name := strings.TrimSpace(snapshot.LimitName); name != "" {
		return name
	}
	if id := strings.TrimSpace(snapshot.LimitID); id != "" {
		return id
	}
	return "Rate limit"
}

func appServerUsageWindows(snapshot appServerRateLimitSnapshot) []core.UsageWindow {
	var windows []core.UsageWindow
	if snapshot.Primary != nil {
		windows = append(windows, appServerUsageWindow("Primary", snapshot.Primary))
	}
	if snapshot.Secondary != nil {
		windows = append(windows, appServerUsageWindow("Secondary", snapshot.Secondary))
	}
	return windows
}

func appServerUsageWindow(name string, window *appServerRateLimitWindow) core.UsageWindow {
	resetAfter := 0
	if window != nil && window.ResetsAt > 0 {
		resetAfter = int(time.Until(time.Unix(window.ResetsAt, 0)).Seconds())
		if resetAfter < 0 {
			resetAfter = 0
		}
	}
	return core.UsageWindow{
		Name:              name,
		UsedPercent:       window.UsedPercent,
		WindowSeconds:     window.WindowDurationMins * 60,
		ResetAfterSeconds: resetAfter,
		ResetAtUnix:       window.ResetsAt,
	}
}

func cloneUsageReport(report *core.UsageReport) *core.UsageReport {
	if report == nil {
		return nil
	}
	cloned := *report
	if len(report.Buckets) > 0 {
		cloned.Buckets = make([]core.UsageBucket, len(report.Buckets))
		for i, bucket := range report.Buckets {
			cloned.Buckets[i] = bucket
			if len(bucket.Windows) > 0 {
				cloned.Buckets[i].Windows = append([]core.UsageWindow(nil), bucket.Windows...)
			}
		}
	}
	if report.Credits != nil {
		credits := *report.Credits
		cloned.Credits = &credits
	}
	return &cloned
}

func normalizeRuntimeReasoningEffort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ""
	case "med":
		return "medium"
	case "x-high", "very-high":
		return "xhigh"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func appServerJSON(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "{}" || s == "[]" || s == `""` {
		return ""
	}
	return s
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func rpcIDToInt64(v any) (int64, bool) {
	switch id := v.(type) {
	case float64:
		return int64(id), true
	case int64:
		return id, true
	case int:
		return int64(id), true
	case json.Number:
		i, err := id.Int64()
		return i, err == nil
	}
	return 0, false
}

func (s *appServerSession) completeTurn() {
	s.stateMu.Lock()
	if s.currentTurn == "" {
		s.stateMu.Unlock()
		return
	}
	s.currentTurn = ""
	s.stateMu.Unlock()
	s.flushPendingAsText()
	s.emit(core.Event{Type: core.EventResult, SessionID: s.CurrentSessionID(), Done: true})
}

func (s *appServerSession) failTurn(err error) {
	s.stateMu.Lock()
	if s.currentTurn == "" {
		s.stateMu.Unlock()
		return
	}
	s.currentTurn = ""
	s.pendingMsgs = s.pendingMsgs[:0]
	s.stateMu.Unlock()
	s.emitError(err)
}

func (s *appServerSession) flushPendingAsThinking() {
	s.stateMu.Lock()
	msgs := append([]string(nil), s.pendingMsgs...)
	s.pendingMsgs = s.pendingMsgs[:0]
	s.stateMu.Unlock()

	for _, text := range msgs {
		if strings.TrimSpace(text) != "" {
			s.emit(core.Event{Type: core.EventThinking, Content: text})
		}
	}
}

func (s *appServerSession) flushPendingAsText() {
	s.stateMu.Lock()
	msgs := append([]string(nil), s.pendingMsgs...)
	s.pendingMsgs = s.pendingMsgs[:0]
	s.stateMu.Unlock()

	for _, text := range msgs {
		if strings.TrimSpace(text) != "" {
			s.emit(core.Event{Type: core.EventText, Content: text})
		}
	}
}

func (s *appServerSession) emit(event core.Event) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	if s.eventsDone {
		return
	}
	select {
	case s.events <- event:
	default:
		slog.Warn("codex appserver: event channel full, dropping event", "type", event.Type)
	}
}

func (s *appServerSession) emitError(err error) {
	if err == nil {
		return
	}
	s.emit(core.Event{Type: core.EventError, Error: err})
}
