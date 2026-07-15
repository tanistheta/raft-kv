package maelstrom

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"raft-kv/kv"
	"raft-kv/raft"
)

// clientBody covers the fields used by Maelstrom's lin-kv client
// operations. Per Maelstrom's protocol docs, key/value/from/to are always
// plain strings for this workload.
type clientBody struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// pendingClientOp is a write/cas this leader has appended to its own log
// but not yet resolved. index/term identify the log slot; kind decides
// which *_ok reply type to send once it commits.
type pendingClientOp struct {
	req   Message
	kind  string // "write" or "cas"
	index int
	term  int
}

// ClientHandler answers Maelstrom's lin-kv read/write/cas requests against
// a raft.Node + kv.StateMachine pair. Every method here must only be called
// while holding the mutex that guards the underlying raft.Node - see
// prod.RealClock's doc comment for why.
type ClientHandler struct {
	transport *Node
	raftNode  *raft.Node
	store     *kv.StateMachine
	tracker   *resultTracker

	pending []*pendingClientOp
}

func NewClientHandler(transport *Node, raftNode *raft.Node, store *kv.StateMachine, tracker *resultTracker) *ClientHandler {
	return &ClientHandler{transport: transport, raftNode: raftNode, store: store, tracker: tracker}
}

// Handle processes msg if it's a client op this handler recognizes
// (read/write/cas), and reports whether it did - so a caller composing
// this with NetworkAdapter knows whether to try the other one.
func (h *ClientHandler) Handle(msg Message) bool {
	var body clientBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return false
	}
	switch body.Type {
	case "read":
		h.handleRead(msg, body)
	case "write":
		h.handleWrite(msg, body)
	case "cas":
		h.handleCAS(msg, body)
	default:
		return false
	}
	return true
}

func (h *ClientHandler) handleRead(msg Message, body clientBody) {
	if h.raftNode.Role != raft.Leader {
		h.replyError(msg, errTemporarilyUnavailable, "not leader")
		return
	}
	// A freshly-elected leader can have inherited already-durable
	// entries it hasn't yet counted as committed in its own term - see
	// docs/bugs.md, "stale/missing reads" bug. Same guard as
	// sim/workload.go's read path.
	if h.raftNode.CommitIndex < h.raftNode.LastLogIndex() {
		h.replyError(msg, errTemporarilyUnavailable, "leader not fresh yet")
		return
	}
	val, ok := h.store.Get(body.Key)
	if !ok {
		h.replyError(msg, errKeyDoesNotExist, "key does not exist")
		return
	}
	h.transport.Reply(msg, map[string]any{"type": "read_ok", "value": val})
}

func (h *ClientHandler) handleWrite(msg Message, body clientBody) {
	if h.raftNode.Role != raft.Leader {
		h.replyError(msg, errTemporarilyUnavailable, "not leader")
		return
	}
	if !validToken(body.Key) || !validToken(body.Value) {
		h.replyError(msg, errMalformedRequest, "key/value must not contain whitespace or '='")
		return
	}
	h.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    h.raftNode.CurrentTerm,
		Command: []byte(fmt.Sprintf("SET %s=%s", body.Key, body.Value)),
	})
	h.trackPending(msg, "write")
}

func (h *ClientHandler) handleCAS(msg Message, body clientBody) {
	if h.raftNode.Role != raft.Leader {
		h.replyError(msg, errTemporarilyUnavailable, "not leader")
		return
	}
	if !validToken(body.Key) || !validToken(body.From) || !validToken(body.To) {
		h.replyError(msg, errMalformedRequest, "key/from/to must not contain whitespace or '='")
		return
	}
	h.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    h.raftNode.CurrentTerm,
		Command: []byte(fmt.Sprintf("CAS %s %s %s", body.Key, body.From, body.To)),
	})
	h.trackPending(msg, "cas")
}

func (h *ClientHandler) trackPending(msg Message, kind string) {
	h.pending = append(h.pending, &pendingClientOp{
		req: msg, kind: kind,
		index: h.raftNode.LastLogIndex(),
		term:  h.raftNode.CurrentTerm,
	})
}

// ResolvePending checks every outstanding write/cas against current commit
// progress and replies to whichever are now decided. This is
// sim/workload.go's resolvePending, ported from tick-polling to being
// called after every dispatched message - commit progress only ever
// advances in reaction to a message, so that's sufficient without a
// separate polling goroutine.
func (h *ClientHandler) ResolvePending() {
	if len(h.pending) == 0 {
		return
	}
	var still []*pendingClientOp
	for _, p := range h.pending {
		if h.raftNode.LastLogIndex() < p.index {
			still = append(still, p)
			continue
		}
		entry, err := h.raftNode.GetLogEntry(p.index)
		if err != nil {
			still = append(still, p)
			continue
		}
		if entry.Term != p.term {
			// A different leader's entry ended up at this slot -
			// ours lost out. Tell the client to retry elsewhere.
			h.replyError(p.req, errAbort, "entry overwritten before commit")
			continue
		}
		if h.raftNode.CommitIndex < p.index {
			still = append(still, p)
			continue
		}
		applyErr, ok := h.tracker.resultAt(p.index)
		if !ok {
			// CommitIndex >= p.index should imply applyCommitted
			// already ran for this index. Defensive fallback:
			// leave it pending rather than dropping the request.
			still = append(still, p)
			continue
		}
		h.replyForResult(p, applyErr)
	}
	h.pending = still
}

func (h *ClientHandler) replyForResult(p *pendingClientOp, applyErr error) {
	switch {
	case applyErr == nil:
		okType := "write_ok"
		if p.kind == "cas" {
			okType = "cas_ok"
		}
		h.transport.Reply(p.req, map[string]any{"type": okType})
	case errors.Is(applyErr, kv.ErrKeyNotFound):
		h.replyError(p.req, errKeyDoesNotExist, "key does not exist")
	case errors.Is(applyErr, kv.ErrCASMismatch):
		h.replyError(p.req, errPreconditionFailed, "cas expected value did not match")
	default:
		h.replyError(p.req, errAbort, applyErr.Error())
	}
}

func (h *ClientHandler) replyError(req Message, code int, text string) {
	h.transport.Reply(req, map[string]any{"type": "error", "code": code, "text": text})
}

// validToken rejects values our space-delimited command encoding can't
// carry safely. Known limitation: real lin-kv values are always simple
// strings per Maelstrom's docs, but nothing stops a value containing a
// space from breaking this encoding if one ever showed up.
func validToken(s string) bool {
	return !strings.ContainsAny(s, " \t\n=")
}