package core

import "testing"

// steeringAgentSession behaves like the Codex app-server backend: it can absorb
// a message into the turn that is already running (native `turn/steer`) and
// says so through SteersMidTurn.
type steeringAgentSession struct{ *queuingAgentSession }

func (s *steeringAgentSession) SteersMidTurn() bool { return true }

func newSteeringSession(id string) *steeringAgentSession {
	return &steeringAgentSession{queuingAgentSession: newQueuingSession(id)}
}

func steerBusyFixture(session AgentSession) (*Engine, *stubPlatformEngine) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangChinese)
	e.interactiveStates["test:user1"] = &interactiveState{agentSession: session}
	return e, p
}

func steerBusyMessage(content string) *Message {
	return &Message{
		SessionKey: "test:user1",
		UserID:     "user1",
		UserName:   "user1",
		Content:    content,
		ReplyCtx:   "ctx",
	}
}

func TestSteerBusyPlainText_SteersPlainTextIntoRunningTurn(t *testing.T) {
	sess := newSteeringSession("steer-1")
	e, p := steerBusyFixture(sess)
	session := &Session{ID: "test:user1"}
	msg := steerBusyMessage("stop the current command; use the other repo instead")

	if !e.steerBusyPlainText(p, msg, "test:user1", session) {
		t.Fatal("plain text from a steerable session must be steered into the running turn")
	}
	if len(sess.sendCalls) != 1 || sess.sendCalls[0] != msg.Content {
		t.Fatalf("Send calls = %q, want the message content verbatim", sess.sendCalls)
	}
	// The steered text belongs to the running turn, so it must land in history
	// itself — it never goes through the normal process-message path.
	if len(session.History) != 1 || session.History[0].Role != "user" {
		t.Fatalf("session history = %+v, want one user entry", session.History)
	}
	if got := p.getSent(); len(got) != 1 || got[0] != e.i18n.T(MsgSteered) {
		t.Fatalf("replies = %q, want the steered receipt", got)
	}
}

func TestSteerBusyPlainText_NonSteeringSessionKeepsQueueing(t *testing.T) {
	// A session that does not advertise mid-turn steering (CLI-backed agents)
	// must fall through to the queue: returning true here would swallow the
	// message without anyone replaying it.
	sess := newQueuingSession("queue-1")
	e, p := steerBusyFixture(sess)
	msg := steerBusyMessage("stop the current command")

	if e.steerBusyPlainText(p, msg, "test:user1", &Session{ID: "test:user1"}) {
		t.Fatal("a session without SteersMidTurn must keep the existing queue path")
	}
	if len(sess.sendCalls) != 0 {
		t.Fatalf("Send calls = %q, want none", sess.sendCalls)
	}
	if got := p.getSent(); len(got) != 0 {
		t.Fatalf("replies = %q, want none", got)
	}
}

func TestSteerBusyPlainText_FallsBackForMediaEmptyAndDeadSessions(t *testing.T) {
	cases := []struct {
		name   string
		alive  bool
		mutate func(*Message)
	}{
		{name: "empty text", alive: true, mutate: func(m *Message) { m.Content = "   " }},
		{name: "image attachment", alive: true, mutate: func(m *Message) {
			m.Images = []ImageAttachment{{MimeType: "image/png", Data: []byte("x")}}
		}},
		{name: "file attachment", alive: true, mutate: func(m *Message) {
			m.Files = []FileAttachment{{FileName: "a.txt", Data: []byte("x")}}
		}},
		{name: "location", alive: true, mutate: func(m *Message) {
			m.Location = &LocationAttachment{Latitude: 1, Longitude: 2}
		}},
		{name: "dead session", alive: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSteeringSession("steer-2")
			sess.alive = tc.alive
			e, p := steerBusyFixture(sess)
			msg := steerBusyMessage("stop the current command")
			if tc.mutate != nil {
				tc.mutate(msg)
			}
			if e.steerBusyPlainText(p, msg, "test:user1", &Session{ID: "test:user1"}) {
				t.Fatal("unsteerable payload must fall back to the busy queue")
			}
			if len(sess.sendCalls) != 0 {
				t.Fatalf("Send calls = %q, want none", sess.sendCalls)
			}
		})
	}
}

func TestSteerBusyPlainText_NoInteractiveStateFallsBack(t *testing.T) {
	e, p := steerBusyFixture(nil)
	msg := steerBusyMessage("stop the current command")
	if e.steerBusyPlainText(p, msg, "test:user1", &Session{ID: "test:user1"}) {
		t.Fatal("a missing agent session must fall back to the busy queue")
	}
}

func TestSteerBusyPlainText_StaleReplayFallsBackToQueue(t *testing.T) {
	// A platform that resyncs can redeliver an older user message while a turn
	// runs. Steering it would hand the agent an instruction the user already
	// moved past, so it has to keep going down the queue path, which drops
	// stale messages and logs why.
	sess := newSteeringSession("steer-3")
	e, p := steerBusyFixture(sess)
	state := e.interactiveStates["test:user1"]
	state.mu.Lock()
	state.lastCompletedUserMessageTimeMs = 2000
	state.mu.Unlock()
	msg := steerBusyMessage("stop the current command")
	msg.UserMessageTimeMs = 1000

	if e.steerBusyPlainText(p, msg, "test:user1", &Session{ID: "test:user1"}) {
		t.Fatal("a stale replayed message must not be steered into the running turn")
	}
	if len(sess.sendCalls) != 0 {
		t.Fatalf("Send calls = %q, want none", sess.sendCalls)
	}
}
