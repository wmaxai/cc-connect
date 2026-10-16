package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestAppServerSession_ApplyThreadRuntimeState(t *testing.T) {
	s := &appServerSession{}
	effort := "xhigh"

	s.applyThreadRuntimeState("/tmp/project", "gpt-5.4", &effort)

	if got := s.GetWorkDir(); got != "/tmp/project" {
		t.Fatalf("GetWorkDir() = %q, want /tmp/project", got)
	}
	if got := s.GetModel(); got != "gpt-5.4" {
		t.Fatalf("GetModel() = %q, want gpt-5.4", got)
	}
	if got := s.GetReasoningEffort(); got != "xhigh" {
		t.Fatalf("GetReasoningEffort() = %q, want xhigh", got)
	}
}

func TestAppServerSession_HandleRateLimitsUpdatedCachesUsage(t *testing.T) {
	s := &appServerSession{}
	raw, err := json.Marshal(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:   "codex",
			PlanType:  "pro",
			Primary:   &appServerRateLimitWindow{UsedPercent: 25, WindowDurationMins: 15, ResetsAt: 1730947200},
			Secondary: &appServerRateLimitWindow{UsedPercent: 42, WindowDurationMins: 60, ResetsAt: 1730950800},
			Credits:   &appServerCreditsSnapshot{HasCredits: true, Unlimited: false},
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("account/rateLimits/updated", raw)

	report, err := s.GetUsage(context.Background())
	if err != nil {
		t.Fatalf("GetUsage() returned error: %v", err)
	}
	if report.Provider != "codex" {
		t.Fatalf("provider = %q, want codex", report.Provider)
	}
	if report.Plan != "pro" {
		t.Fatalf("plan = %q, want pro", report.Plan)
	}
	if len(report.Buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(report.Buckets))
	}
	if got := report.Buckets[0].Name; got != "codex" {
		t.Fatalf("bucket name = %q, want codex", got)
	}
	if got := report.Buckets[0].Windows[0].WindowSeconds; got != 15*60 {
		t.Fatalf("primary window seconds = %d, want %d", got, 15*60)
	}
	if got := report.Buckets[0].Windows[1].UsedPercent; got != 42 {
		t.Fatalf("secondary used percent = %d, want 42", got)
	}
	if report.Credits == nil || !report.Credits.HasCredits {
		t.Fatalf("credits = %#v, want has credits", report.Credits)
	}
}

func TestAppServerSession_HandleThreadTokenUsageUpdatedCachesContextUsage(t *testing.T) {
	s := &appServerSession{}
	raw, err := json.Marshal(appServerThreadTokenUsageNotification{
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		TokenUsage: struct {
			Total              codexTokenUsage `json:"total"`
			Last               codexTokenUsage `json:"last"`
			ModelContextWindow int             `json:"modelContextWindow"`
		}{
			Total: codexTokenUsage{
				TotalTokens:           52011395,
				InputTokens:           51847383,
				CachedInputTokens:     48187904,
				OutputTokens:          164012,
				ReasoningOutputTokens: 78910,
			},
			Last: codexTokenUsage{
				TotalTokens:           41061,
				InputTokens:           40849,
				CachedInputTokens:     36864,
				OutputTokens:          212,
				ReasoningOutputTokens: 32,
			},
			ModelContextWindow: 258400,
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("thread/tokenUsage/updated", raw)

	usage := s.GetContextUsage()
	if usage == nil {
		t.Fatal("GetContextUsage() = nil, want cached context usage")
	}
	if usage.UsedTokens != 41061 {
		t.Fatalf("used tokens = %d, want 41061", usage.UsedTokens)
	}
	if usage.BaselineTokens != codexContextBaselineTokens {
		t.Fatalf("baseline tokens = %d, want %d", usage.BaselineTokens, codexContextBaselineTokens)
	}
	if usage.TotalTokens != 41061 {
		t.Fatalf("total tokens = %d, want 41061", usage.TotalTokens)
	}
	if usage.ContextWindow != 258400 {
		t.Fatalf("context window = %d, want 258400", usage.ContextWindow)
	}
	if usage.CachedInputTokens != 36864 {
		t.Fatalf("cached input tokens = %d, want 36864", usage.CachedInputTokens)
	}
	if usage.InputTokens != 40849 {
		t.Fatalf("input tokens = %d, want 40849", usage.InputTokens)
	}
}

func TestAppServerSession_FailedTurnEmitsError(t *testing.T) {
	s := &appServerSession{
		events:      make(chan core.Event, 2),
		currentTurn: "turn-1",
		pendingMsgs: []string{"partial reasoning"},
	}

	raw, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turn": map[string]any{
			"id":     "turn-1",
			"status": "failed",
			"error":  map[string]any{"message": "service unavailable"},
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	s.handleNotification("turn/completed", raw)

	event := <-s.events
	if event.Type != core.EventError || event.Error == nil || event.Error.Error() != "service unavailable" {
		t.Fatalf("event = %#v, want EventError with service unavailable", event)
	}

	s.stateMu.Lock()
	currentTurn := s.currentTurn
	pendingCount := len(s.pendingMsgs)
	s.stateMu.Unlock()
	if currentTurn != "" {
		t.Fatalf("current turn = %q after failure, want empty", currentTurn)
	}
	if pendingCount != 0 {
		t.Fatalf("pending message count = %d after failure, want 0", pendingCount)
	}

	idleRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"status":   map[string]any{"type": "idle"},
	})
	if err != nil {
		t.Fatalf("marshal idle notification: %v", err)
	}
	s.handleNotification("thread/status/changed", idleRaw)
	select {
	case duplicate := <-s.events:
		t.Fatalf("idle notification emitted duplicate event %#v", duplicate)
	default:
	}
}

// The write path now lives on the shared conn, so a wedged stdin gets the whole
// process recycled instead of hanging one session's request forever.
func TestAppServerConn_RequestTimeoutIncludesBlockedStdinWrite(t *testing.T) {
	stdin := newBlockingWriteCloser()
	defer func() { _ = stdin.Close() }()

	c := newTestConn(stdin)

	done := make(chan error, 1)
	go func() {
		var out map[string]any
		done <- c.requestWithTimeout("turn/start", map[string]any{
			"input": strings.Repeat("x", 1024),
		}, &out, 25*time.Millisecond)
	}()

	select {
	case <-stdin.started:
	case <-time.After(time.Second):
		t.Fatal("request did not attempt to write to stdin")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("requestWithTimeout returned nil, want write timeout")
		}
		if !strings.Contains(err.Error(), "turn/start") || !strings.Contains(err.Error(), "write timed out") {
			t.Fatalf("error = %q, want turn/start write timeout", err.Error())
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("requestWithTimeout did not return while stdin write was blocked")
	}

	if !stdin.Closed() {
		t.Fatal("blocked stdin was not closed after timeout")
	}
}

func TestAppServerSession_SendSteersActiveTurn(t *testing.T) {
	s, stdin := newSendTestSession(t, "turn-1", "buffered answer")
	request, err := sendAndRespond(t, s, stdin, "focus on the requested fix", map[string]any{"turnId": "turn-1"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if request.Method != "turn/steer" {
		t.Fatalf("method = %q, want turn/steer", request.Method)
	}
	if got := request.Params["threadId"]; got != "thread-1" {
		t.Fatalf("threadId = %v, want thread-1", got)
	}
	if got := request.Params["expectedTurnId"]; got != "turn-1" {
		t.Fatalf("expectedTurnId = %v, want turn-1", got)
	}
	if got := request.Params["input"]; got == nil {
		t.Fatal("input = nil, want structured steering input")
	}

	currentTurn, pendingMsgs := sendTestState(s)
	if currentTurn != "turn-1" {
		t.Fatalf("current turn = %q, want turn-1", currentTurn)
	}
	if len(pendingMsgs) != 1 || pendingMsgs[0] != "buffered answer" {
		t.Fatalf("pending messages = %v, want buffered answer", pendingMsgs)
	}

	completedItem, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"item":     map[string]any{"type": "agentMessage", "text": "complete final answer"},
	})
	if err != nil {
		t.Fatalf("marshal completed item: %v", err)
	}
	s.handleNotification("item/completed", completedItem)
	completedTurn, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turn":     map[string]any{"id": "turn-1", "status": "completed"},
	})
	if err != nil {
		t.Fatalf("marshal completed turn: %v", err)
	}
	s.handleNotification("turn/completed", completedTurn)

	firstText := waitForEvent(t, s.events)
	finalText := waitForEvent(t, s.events)
	resultEvent := waitForEvent(t, s.events)
	if firstText.Type != core.EventText || firstText.Content != "buffered answer" {
		t.Fatalf("first text event = %#v, want buffered answer", firstText)
	}
	if finalText.Type != core.EventText || finalText.Content != "complete final answer" {
		t.Fatalf("final text event = %#v, want complete final answer", finalText)
	}
	if resultEvent.Type != core.EventResult || !resultEvent.Done {
		t.Fatalf("result event = %#v, want one completed result", resultEvent)
	}
}

func TestAppServerSession_SendStartsIdleTurn(t *testing.T) {
	s, stdin := newSendTestSession(t, "", "stale message")
	request, err := sendAndRespond(t, s, stdin, "start new work", map[string]any{"turn": map[string]any{"id": "turn-2"}})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if request.Method != "turn/start" {
		t.Fatalf("method = %q, want turn/start", request.Method)
	}
	currentTurn, pendingMsgs := sendTestState(s)
	if currentTurn != "turn-2" {
		t.Fatalf("current turn = %q, want turn-2", currentTurn)
	}
	if len(pendingMsgs) != 0 {
		t.Fatalf("pending messages = %v, want none", pendingMsgs)
	}
}

func TestAppServerSession_SendRejectsMismatchedSteeringTurn(t *testing.T) {
	s, stdin := newSendTestSession(t, "turn-1", "buffered answer")
	_, err := sendAndRespond(t, s, stdin, "steer current work", map[string]any{"turnId": "turn-other"})
	if err == nil || !strings.Contains(err.Error(), "turn/steer") || !strings.Contains(err.Error(), "turn-other") {
		t.Fatalf("Send() error = %v, want mismatched turn/steer error", err)
	}
	currentTurn, pendingMsgs := sendTestState(s)
	if currentTurn != "turn-1" {
		t.Fatalf("current turn = %q, want turn-1", currentTurn)
	}
	if len(pendingMsgs) != 1 || pendingMsgs[0] != "buffered answer" {
		t.Fatalf("pending messages = %v, want buffered answer", pendingMsgs)
	}
}

// Regression: mid-turn narration (phase=commentary) used to land in pendingMsgs
// and the next tool call demoted it to EventThinking, which compact/quiet
// display modes drop. A long turn therefore looked frozen until it ended, and a
// mid-turn course correction produced no visible answer.
func TestAppServerSession_CommentaryAgentMessageEmitsTextImmediately(t *testing.T) {
	s := &appServerSession{events: make(chan core.Event, 4), currentTurn: "turn-1"}

	s.handleNotification("item/completed", mustMarshalItemNotification(t, map[string]any{
		"type": "agentMessage", "text": "switching to the other repo instead", "phase": "commentary",
	}))

	event := waitForEvent(t, s.events)
	if event.Type != core.EventText || event.Content != "switching to the other repo instead" {
		t.Fatalf("event = %#v, want commentary published as EventText", event)
	}
	if _, pending := sendTestState(s); len(pending) != 0 {
		t.Fatalf("pending messages = %v, want none (commentary is not buffered)", pending)
	}
	select {
	case extra := <-s.events:
		t.Fatalf("unexpected extra event %#v, want exactly one text event", extra)
	default:
	}
}

func TestAppServerSession_CommentarySurvivesToolBoundary(t *testing.T) {
	s := &appServerSession{events: make(chan core.Event, 4), currentTurn: "turn-1"}

	s.handleNotification("item/completed", mustMarshalItemNotification(t, map[string]any{
		"type": "agentMessage", "text": "running the test suite now", "phase": "commentary",
	}))
	s.handleNotification("item/started", mustMarshalItemNotification(t, map[string]any{
		"type": "commandExecution", "command": "go test ./...",
	}))

	event := waitForEvent(t, s.events)
	if event.Type != core.EventText || event.Content != "running the test suite now" {
		t.Fatalf("event = %#v, want the commentary as EventText", event)
	}
	if _, pending := sendTestState(s); len(pending) != 0 {
		t.Fatalf("pending messages = %v, want none", pending)
	}
	// The tool call itself publishes a tool event; the narration must not come
	// back as a thinking event, because compact/quiet display modes drop those.
	for {
		select {
		case extra := <-s.events:
			if extra.Type == core.EventThinking {
				t.Fatalf("commentary was demoted to a thinking event: %#v", extra)
			}
		default:
			return
		}
	}
}

func TestAppServerSession_TerminalAgentMessageStaysBuffered(t *testing.T) {
	for _, tc := range []struct{ name, phase string }{
		{name: "phase unknown", phase: ""},
		{name: "final answer", phase: "final_answer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &appServerSession{events: make(chan core.Event, 4), currentTurn: "turn-1"}
			item := map[string]any{"type": "agentMessage", "text": "the terminal answer"}
			if tc.phase != "" {
				item["phase"] = tc.phase
			}

			s.handleNotification("item/completed", mustMarshalItemNotification(t, item))

			if _, pending := sendTestState(s); len(pending) != 1 || pending[0] != "the terminal answer" {
				t.Fatalf("pending messages = %v, want the terminal answer buffered", pending)
			}
			select {
			case event := <-s.events:
				t.Fatalf("unexpected event %#v, want the item buffered instead", event)
			default:
			}
		})
	}
}

func mustMarshalItemNotification(t *testing.T, item map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"item":     item,
	})
	if err != nil {
		t.Fatalf("marshal item notification: %v", err)
	}
	return raw
}

func TestMapAppServerRateLimits_PrefersMultiBucketView(t *testing.T) {
	report := mapAppServerRateLimits(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:  "legacy",
			PlanType: "team",
			Primary:  &appServerRateLimitWindow{UsedPercent: 99, WindowDurationMins: 15},
		},
		RateLimitsByLimitID: map[string]appServerRateLimitSnapshot{
			"codex": {
				LimitID:   "codex",
				LimitName: "Codex",
				PlanType:  "team",
				Primary:   &appServerRateLimitWindow{UsedPercent: 10, WindowDurationMins: 15},
			},
			"codex_other": {
				LimitID:  "codex_other",
				PlanType: "team",
				Primary:  &appServerRateLimitWindow{UsedPercent: 20, WindowDurationMins: 60},
			},
		},
	})

	if report.Plan != "team" {
		t.Fatalf("plan = %q, want team", report.Plan)
	}
	if len(report.Buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(report.Buckets))
	}
	if report.Buckets[0].Name != "Codex" {
		t.Fatalf("first bucket = %q, want Codex", report.Buckets[0].Name)
	}
	if report.Buckets[1].Name != "codex_other" {
		t.Fatalf("second bucket = %q, want codex_other", report.Buckets[1].Name)
	}
}

func TestAppServerSession_HandleRequestUserInputEmitsAskQuestion(t *testing.T) {
	stdin := &lockedWriteCloser{}
	c := newTestConn(stdin)
	t.Cleanup(func() { _ = c.Close() })
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              context.Background(),
		pendingApprovals: make(map[string]chan core.PermissionResult),
		conn:             c,
		pinned:           true,
	}

	s.handleServerRequest(c, json.RawMessage(`"rui-1"`), "item/tool/requestUserInput", serverRequestParams(t, map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-1",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"isOther":  true,
				"isSecret": false,
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if event.Type != core.EventPermissionRequest {
		t.Fatalf("event type = %s, want %s", event.Type, core.EventPermissionRequest)
	}
	if event.ToolName != "AskUserQuestion" {
		t.Fatalf("tool name = %q, want AskUserQuestion", event.ToolName)
	}
	if event.RequestID != `"rui-1"` {
		t.Fatalf("request id = %q, want raw JSON id", event.RequestID)
	}
	if len(event.Questions) != 1 {
		t.Fatalf("questions = %d, want 1", len(event.Questions))
	}
	q := event.Questions[0]
	if q.Question != "Which database should we use?" || q.Header != "Database" {
		t.Fatalf("question = %#v", q)
	}
	if len(q.Options) != 2 || q.Options[0].Label != "Postgres" || q.Options[1].Description != "Keep it embedded" {
		t.Fatalf("options = %#v", q.Options)
	}
	if stdin.String() != "" {
		t.Fatalf("request_user_input should not write before the answer, got %q", stdin.String())
	}
}

func TestAppServerSession_HandleRequestUserInputWritesCodexResponse(t *testing.T) {
	stdin := &lockedWriteCloser{}
	c := newTestConn(stdin)
	t.Cleanup(func() { _ = c.Close() })
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              context.Background(),
		pendingApprovals: make(map[string]chan core.PermissionResult),
		conn:             c,
		pinned:           true,
	}

	s.handleServerRequest(c, json.RawMessage(`"rui-2"`), "item/tool/requestUserInput", serverRequestParams(t, map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-2",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if err := s.RespondPermission(event.RequestID, core.PermissionResult{
		Behavior: "allow",
		UpdatedInput: map[string]any{
			"answers": map[string]any{
				"Which database should we use?": "Postgres",
			},
		},
	}); err != nil {
		t.Fatalf("RespondPermission() error = %v", err)
	}

	line := waitForWrittenJSONLine(t, stdin)
	var envelope struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Result  struct {
			Answers map[string]struct {
				Answers []string `json:"answers"`
			} `json:"answers"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("decode response %q: %v", line, err)
	}
	if envelope.JSONRPC != "2.0" || envelope.ID != "rui-2" {
		t.Fatalf("envelope = %#v", envelope)
	}
	got := envelope.Result.Answers["database"].Answers
	if len(got) != 1 || got[0] != "Postgres" {
		t.Fatalf("answers[database] = %#v, want [Postgres]", got)
	}
}

var _ interface {
	GetUsage(context.Context) (*core.UsageReport, error)
} = (*appServerSession)(nil)

var _ interface {
	GetContextUsage() *core.ContextUsage
} = (*appServerSession)(nil)

type lockedWriteCloser struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriteCloser) Close() error { return nil }

func (w *lockedWriteCloser) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

var _ io.WriteCloser = (*lockedWriteCloser)(nil)

type blockingWriteCloser struct {
	started   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	isClosed bool
}

func newBlockingWriteCloser() *blockingWriteCloser {
	return &blockingWriteCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (w *blockingWriteCloser) Write([]byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.isClosed = true
		w.mu.Unlock()
		close(w.closed)
	})
	return nil
}

func (w *blockingWriteCloser) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isClosed
}

var _ io.WriteCloser = (*blockingWriteCloser)(nil)

func serverRequestParams(t *testing.T, params any) json.RawMessage {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	return paramsJSON
}

// newTestConn builds a conn whose transport writes into w instead of a real
// `codex app-server` child, so tests can drive thread routing and the request
// path without spawning a process. It is pooled like a shared child; the
// process-scope tests flip that flag.
func newTestConn(w io.WriteCloser) *appServerConn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &appServerConn{
		gen:      1,
		ctx:      ctx,
		cancel:   cancel,
		stdin:    w,
		pending:  map[int64]chan rpcResponseEnvelope{},
		sessions: map[*appServerSession]struct{}{},
		routes:   map[string]*appServerSession{},
		pooled:   true,
	}
	c.alive.Store(true)
	return c
}

type clientRequestProbe struct {
	ID     any            `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

func newSendTestSession(t *testing.T, currentTurn string, pendingMsgs ...string) (*appServerSession, *lockedWriteCloser) {
	t.Helper()
	stdin := &lockedWriteCloser{}
	conn := newTestConn(stdin)
	t.Cleanup(func() { _ = conn.Close() })
	s := newPinnedSession(t, conn, "thread-1", currentTurn)
	// The Send tests keep the pre-fork ownership model: one session, one child.
	s.connBase = appServerConnBase{scope: appServerScopeProcess}
	s.pendingMsgs = append([]string(nil), pendingMsgs...)
	return s, stdin
}

// newPinnedSession builds a session bound to an already-created conn, which is
// how a shared app-server serves several projects at once.
func newPinnedSession(t *testing.T, conn *appServerConn, threadID, currentTurn string) *appServerSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &appServerSession{
		ctx:          ctx,
		cancel:       cancel,
		events:       make(chan core.Event, 8),
		conn:         conn,
		pinned:       true,
		connBase:     appServerConnBase{scope: appServerScopeShared},
		preambleSent: true,
		currentTurn:  currentTurn,
	}
	s.alive.Store(true)
	s.threadID.Store(threadID)
	conn.attach(s)
	conn.bindThread(threadID, s)
	s.attachedGen.Store(conn.Generation())
	return s
}

func sendAndRespond(t *testing.T, s *appServerSession, stdin *lockedWriteCloser, prompt string, result any) (clientRequestProbe, error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- s.Send(prompt, "", nil, nil)
	}()
	request := waitForClientRequest(t, stdin)
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	s.currentConn().handleResponse(rpcResponseEnvelope{ID: request.ID, Result: resultJSON})
	select {
	case err := <-done:
		return request, err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Send")
		return clientRequestProbe{}, nil
	}
}

func sendTestState(s *appServerSession) (string, []string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.currentTurn, append([]string(nil), s.pendingMsgs...)
}

func waitForEvent(t *testing.T, events <-chan core.Event) core.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for app-server event")
		return core.Event{}
	}
}

func waitForClientRequest(t *testing.T, w *lockedWriteCloser) clientRequestProbe {
	t.Helper()
	return waitForClientRequestAt(t, w, 0)
}

// waitForClientRequestAt decodes the nth JSON-RPC message written to w, so a
// test can drive a request sequence (retry, fallback) without a real child.
func waitForClientRequestAt(t *testing.T, w *lockedWriteCloser, index int) clientRequestProbe {
	t.Helper()
	line := waitForWrittenJSONLineAt(t, w, index)
	var request clientRequestProbe
	if err := json.Unmarshal([]byte(line), &request); err != nil {
		t.Fatalf("decode JSON-RPC request %q: %v", line, err)
	}
	return request
}

func waitForWrittenJSONLine(t *testing.T, w *lockedWriteCloser) string {
	t.Helper()
	return waitForWrittenJSONLineAt(t, w, 0)
}

func waitForWrittenJSONLineAt(t *testing.T, w *lockedWriteCloser, index int) string {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for JSON response #%d, buffer=%q", index, w.String())
		case <-ticker.C:
			lines := make([]string, 0, index+1)
			for _, line := range strings.Split(w.String(), "\n") {
				line = strings.TrimSpace(line)
				if line != "" {
					lines = append(lines, line)
				}
			}
			if len(lines) > index {
				return lines[index]
			}
		}
	}
}

func TestAppServerListenURL(t *testing.T) {
	cases := map[string]string{
		"":                     "",
		"  ":                   "",
		"stdio://":             "",
		"stdio":                "",
		"ws://127.0.0.1:3845":  "ws://127.0.0.1:3845",
		"ws://localhost:9000":  "ws://localhost:9000",
		"ws://127.0.0.1:3845 ": "ws://127.0.0.1:3845",
		" stdio ":              "",
		" stdio:// ":           "",
		"STDIO":                "",
		"STDIO://":             "",
	}
	for in, want := range cases {
		if got := appServerListenURL(in); got != want {
			t.Errorf("appServerListenURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// One app-server serving every project: the thread, not the process, carries the
// working directory and provider.
func TestAppServerSession_ThreadRequestParamsCarryPerThreadTarget(t *testing.T) {
	s := &appServerSession{
		workDir:       "/root/.cc-connect/profiles/codex-test",
		model:         "gpt-5.4",
		modelProvider: "shengsuanyun",
		mode:          "yolo",
	}

	params := s.threadRequestParams()

	if got := params["cwd"]; got != s.workDir {
		t.Fatalf("cwd = %v, want %q", got, s.workDir)
	}
	if got := params["model"]; got != "gpt-5.4" {
		t.Fatalf("model = %v, want gpt-5.4", got)
	}
	if got := params["modelProvider"]; got != "shengsuanyun" {
		t.Fatalf("modelProvider = %v, want shengsuanyun", got)
	}
	if got := params["approvalPolicy"]; got != "never" {
		t.Fatalf("approvalPolicy = %v, want never", got)
	}
	if got := params["sandbox"]; got != "danger-full-access" {
		t.Fatalf("sandbox = %v, want danger-full-access", got)
	}
}

// app_server_scope mirrors hermes' multiplex_profiles: one private process per
// session by default, a single shared app-server on request.
func TestNormalizeAppServerScope(t *testing.T) {
	for in, want := range map[string]string{
		// Sharing is opt-in: unset or unknown keeps one app-server per session,
		// which is what upstream always did (and hermes' multiplex default too).
		"":            appServerScopeProcess,
		"nonsense":    appServerScopeProcess,
		"process":     appServerScopeProcess,
		"private":     appServerScopeProcess,
		"per-session": appServerScopeProcess,
		"PER_SESSION": appServerScopeProcess,
		"shared":      appServerScopeShared,
		"  SHARED  ":  appServerScopeShared,
		"multiplex":   appServerScopeShared,
		"one":         appServerScopeShared,
	} {
		if got := normalizeAppServerScope(in); got != want {
			t.Errorf("normalizeAppServerScope(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAppServerScopeFromOpts(t *testing.T) {
	tests := []struct {
		name string
		opts map[string]any
		want string
	}{
		{name: "unset keeps one process per session", opts: map[string]any{}, want: appServerScopeProcess},
		{name: "explicit shared", opts: map[string]any{"app_server_scope": "shared"}, want: appServerScopeShared},
		{name: "explicit process", opts: map[string]any{"app_server_scope": "process"}, want: appServerScopeProcess},
		// hermes' spelling of the same knob.
		{name: "hermes alias on", opts: map[string]any{"multiplex_profiles": true}, want: appServerScopeShared},
		{name: "hermes alias off", opts: map[string]any{"multiplex_profiles": false}, want: appServerScopeProcess},
		{name: "hermes alias as string", opts: map[string]any{"multiplex_profiles": "true"}, want: appServerScopeShared},
		{
			name: "scope wins over the alias",
			opts: map[string]any{"app_server_scope": "process", "multiplex_profiles": true},
			want: appServerScopeProcess,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appServerScopeFromOpts(tt.opts); got != tt.want {
				t.Fatalf("appServerScopeFromOpts(%#v) = %q, want %q", tt.opts, got, tt.want)
			}
		})
	}
}

// A shared child must not inherit one project's provider credentials: those
// travel per thread through the registry's CC_CODEX_KEY_* variables instead.
func TestAppServerEnvForScope(t *testing.T) {
	env := []string{
		"HTTP_PROXY=http://proxy",
		"OPENAI_API_KEY=sk-a",
		"OPENAI_BASE_URL=https://a.example/v1",
		"CODEX_HOME=/root/.codex",
	}

	shared := appServerEnvForScope(env, appServerScopeShared, "shengsuanyun")
	if len(shared) != 1 || shared[0] != "HTTP_PROXY=http://proxy" {
		t.Fatalf("shared env = %#v, want only the non-credential entry", shared)
	}
	// No provider means nothing is registered for this project, so its own
	// credentials stay in the child's environment.
	if got := appServerEnvForScope(env, appServerScopeShared, ""); len(got) != len(env) {
		t.Fatalf("shared env without a provider = %#v, want the original env", got)
	}
	// scope=process keeps today's private-child behaviour untouched.
	if got := appServerEnvForScope(env, appServerScopeProcess, "shengsuanyun"); len(got) != len(env) {
		t.Fatalf("process env = %#v, want the original env", got)
	}
}

func TestAppServerProviderDefsFromCore(t *testing.T) {
	defs := appServerProviderDefsFromCore([]core.ProviderConfig{
		{Name: "shengsuanyun", BaseURL: "https://api.example/v1", APIKey: "sk-1", CodexWireAPI: "responses"},
		{Name: "no-base-url", APIKey: "sk-2"},
		{Name: "", BaseURL: "https://ignored.example"},
	})

	if len(defs) != 1 {
		t.Fatalf("defs = %#v, want only the provider with a base url", defs)
	}
	def := defs[0]
	if def.Name != "shengsuanyun" || def.BaseURL != "https://api.example/v1" || def.APIKey != "sk-1" || def.WireAPI != "responses" {
		t.Fatalf("def = %#v", def)
	}
	if def.EnvKey != "CC_CODEX_KEY_SHENGSUANYUN" {
		t.Fatalf("EnvKey = %q, want CC_CODEX_KEY_SHENGSUANYUN", def.EnvKey)
	}
}

func TestAppServerConn_RoutesNotificationsByThread(t *testing.T) {
	conn := newTestConn(&lockedWriteCloser{})
	t.Cleanup(func() { _ = conn.Close() })
	first := newPinnedSession(t, conn, "thread-1", "turn-1")
	second := newPinnedSession(t, conn, "thread-2", "turn-2")

	params, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turn":     map[string]any{"id": "turn-1", "status": "completed"},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	conn.routeNotification("turn/completed", params)

	if event := waitForEvent(t, first.events); event.Type != core.EventResult {
		t.Fatalf("event = %#v, want the first session's turn result", event)
	}
	select {
	case leaked := <-second.events:
		t.Fatalf("second session received another thread's event %#v", leaked)
	default:
	}
}

// Closing one project's session must leave the shared child serving the others.
func TestAppServerSession_CloseDetachesFromSharedProcess(t *testing.T) {
	conn := newTestConn(&lockedWriteCloser{})
	t.Cleanup(func() { _ = conn.Close() })
	s := newPinnedSession(t, conn, "thread-1", "")

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !conn.Alive() {
		t.Fatal("shared app-server was torn down together with one session")
	}
	if conn.hasSessions() {
		t.Fatal("session is still attached after Close")
	}
	if conn.sessionForThread("thread-1") != nil {
		t.Fatal("thread route survived Close")
	}
	if _, ok := <-s.Events(); ok {
		t.Fatal("events channel is still open after Close")
	}
}

// scope=process keeps the pre-fork behaviour: the session owns its child.
func TestAppServerSession_CloseOwnsPrivateProcessInProcessScope(t *testing.T) {
	conn := newTestConn(&lockedWriteCloser{})
	conn.pooled = false
	s := newPinnedSession(t, conn, "thread-1", "")
	s.connBase = appServerConnBase{scope: appServerScopeProcess}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if conn.Alive() {
		t.Fatal("process-scope session left its private app-server running")
	}
}

// After the child dies, the next turn re-resumes the same thread on a fresh
// process instead of losing the conversation.
func TestAppServerSession_EnsureThreadAttachedResumesAfterProcessReplaced(t *testing.T) {
	stdin := &lockedWriteCloser{}
	conn := newTestConn(stdin)
	t.Cleanup(func() { _ = conn.Close() })
	s := newPinnedSession(t, conn, "thread-1", "")
	s.workDir = "/root/.cc-connect/profiles/codex-rd"
	s.attachedGen.Store(0) // the process that served this thread is gone

	done := make(chan error, 1)
	go func() {
		_, err := s.ensureThreadAttached()
		done <- err
	}()

	request := waitForClientRequest(t, stdin)
	if request.Method != "thread/resume" {
		t.Fatalf("method = %q, want thread/resume", request.Method)
	}
	if got := request.Params["threadId"]; got != "thread-1" {
		t.Fatalf("threadId = %v, want thread-1", got)
	}
	if got := request.Params["cwd"]; got != s.workDir {
		t.Fatalf("cwd = %v, want %q", got, s.workDir)
	}

	result, err := json.Marshal(map[string]any{"thread": map[string]any{"id": "thread-1"}, "cwd": s.workDir})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	conn.handleResponse(rpcResponseEnvelope{ID: request.ID, Result: result})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensureThreadAttached() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureThreadAttached did not finish")
	}
	if got := s.attachedGen.Load(); got != conn.Generation() {
		t.Fatalf("attached generation = %d, want %d", got, conn.Generation())
	}
	if conn.sessionForThread("thread-1") != s {
		t.Fatal("thread route was not re-bound to the session")
	}
}

// A thread lock left over from the process we just replaced clears on its own;
// a re-attach has to wait it out instead of failing the turn outright.
func TestAppServerSession_EnsureThreadAttachedRetriesWhileWriterLockIsBusy(t *testing.T) {
	restoreDelay := appServerAttachRetryDelay
	appServerAttachRetryDelay = 5 * time.Millisecond
	t.Cleanup(func() { appServerAttachRetryDelay = restoreDelay })

	stdin := &lockedWriteCloser{}
	conn := newTestConn(stdin)
	t.Cleanup(func() { _ = conn.Close() })
	s := newPinnedSession(t, conn, "thread-1", "")
	s.attachedGen.Store(0)

	done := make(chan error, 1)
	go func() {
		_, err := s.ensureThreadAttached()
		done <- err
	}()

	first := waitForClientRequest(t, stdin)
	if first.Method != "thread/resume" {
		t.Fatalf("first method = %q, want thread/resume", first.Method)
	}
	conn.handleResponse(rpcResponseEnvelope{ID: first.ID, Error: &rpcError{
		Message: "thread thread-1 already has an active writer",
	}})

	second := waitForClientRequestAt(t, stdin, 1)
	if second.Method != "thread/resume" {
		t.Fatalf("second method = %q, want a thread/resume retry", second.Method)
	}
	result, err := json.Marshal(map[string]any{"thread": map[string]any{"id": "thread-1"}})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	conn.handleResponse(rpcResponseEnvelope{ID: second.ID, Result: result})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensureThreadAttached() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureThreadAttached did not finish after the lock cleared")
	}
	if got := s.attachedGen.Load(); got != conn.Generation() {
		t.Fatalf("attached generation = %d, want %d", got, conn.Generation())
	}
}

func TestAppServerSession_EnsureThreadAttachedStartsNewThreadWithoutRollout(t *testing.T) {
	stdin := &lockedWriteCloser{}
	conn := newTestConn(stdin)
	t.Cleanup(func() { _ = conn.Close() })
	s := newPinnedSession(t, conn, "thread-1", "")
	s.preambleSent = true
	s.attachedGen.Store(0)

	done := make(chan error, 1)
	go func() {
		_, err := s.ensureThreadAttached()
		done <- err
	}()

	resume := waitForClientRequest(t, stdin)
	if resume.Method != "thread/resume" {
		t.Fatalf("first method = %q, want thread/resume", resume.Method)
	}
	conn.handleResponse(rpcResponseEnvelope{ID: resume.ID, Error: &rpcError{
		Message: "no rollout found for thread id thread-1",
	}})

	start := waitForClientRequestAt(t, stdin, 1)
	if start.Method != "thread/start" {
		t.Fatalf("second method = %q, want thread/start", start.Method)
	}
	result, err := json.Marshal(map[string]any{"thread": map[string]any{"id": "thread-2"}})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	conn.handleResponse(rpcResponseEnvelope{ID: start.ID, Result: result})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensureThreadAttached() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureThreadAttached did not finish")
	}
	if got := s.CurrentSessionID(); got != "thread-2" {
		t.Fatalf("session id = %q, want the replacement thread", got)
	}
	if conn.sessionForThread("thread-1") != nil {
		t.Fatal("the dead thread id is still routed to the session")
	}
	s.stateMu.Lock()
	preambleSent := s.preambleSent
	s.stateMu.Unlock()
	if preambleSent {
		t.Fatal("a replacement thread has to receive the preamble again")
	}
}
