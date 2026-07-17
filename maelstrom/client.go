package maelstrom

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"raft-kv/kv"
	"raft-kv/raft"
)

// clientBody covers the fields used by Maelstrom's lin-kv client
// operations. key/value/from/to are left as raw JSON rather than assumed to
// be strings: lin-kv's actual generator sends integers for both keys and
// values (confirmed by running `maelstrom test -w lin-kv` against this code
// - see docs/results.md), and decoding a JSON number into a Go string field
// fails silently from Handle's caller's point of view, dropping the message
// entirely. canonicalToken below re-encodes whatever scalar arrives into
// the single space-and-'='-free token kv.StateMachine's command format
// requires.
type clientBody struct {
	Type  string          `json:"type"`
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
	From  json.RawMessage `json:"from,omitempty"`
	To    json.RawMessage `json:"to,omitempty"`
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
	tracker   *kv.ResultTracker

	pending []*pendingClientOp
}

func NewClientHandler(transport *Node, raftNode *raft.Node, store *kv.StateMachine, tracker *kv.ResultTracker) *ClientHandler {
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
	key, err := canonicalToken(body.Key)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "key: "+err.Error())
		return
	}
	val, ok := h.store.Get(key)
	if !ok {
		h.replyError(msg, errKeyDoesNotExist, "key does not exist")
		return
	}
	// val is the exact JSON text stored by handleWrite/handleCAS, so
	// wrapping it in json.RawMessage emits it verbatim - a number stays a
	// number - rather than re-quoting it as a Go string.
	h.transport.Reply(msg, map[string]any{"type": "read_ok", "value": json.RawMessage(val)})
}

func (h *ClientHandler) handleWrite(msg Message, body clientBody) {
	if h.raftNode.Role != raft.Leader {
		h.replyError(msg, errTemporarilyUnavailable, "not leader")
		return
	}
	key, err := canonicalToken(body.Key)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "key: "+err.Error())
		return
	}
	value, err := canonicalToken(body.Value)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "value: "+err.Error())
		return
	}
	h.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    h.raftNode.CurrentTerm,
		Command: []byte(fmt.Sprintf("SET %s=%s", key, value)),
	})
	h.trackPending(msg, "write")
}

func (h *ClientHandler) handleCAS(msg Message, body clientBody) {
	if h.raftNode.Role != raft.Leader {
		h.replyError(msg, errTemporarilyUnavailable, "not leader")
		return
	}
	key, err := canonicalToken(body.Key)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "key: "+err.Error())
		return
	}
	from, err := canonicalToken(body.From)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "from: "+err.Error())
		return
	}
	to, err := canonicalToken(body.To)
	if err != nil {
		h.replyError(msg, errMalformedRequest, "to: "+err.Error())
		return
	}
	h.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    h.raftNode.CurrentTerm,
		Command: []byte(fmt.Sprintf("CAS %s %s %s", key, from, to)),
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
// progress and replies to whichever are now decided. Deciding what
// "decided" means is kv.ResolveIndex's job now (kv/store.go) - the same
// function sim/workload.go uses - so this only has to act on the outcome
// and handle the Maelstrom-specific parts: what reply to send, and looking
// up what Apply actually returned via the tracker. This is called after
// every dispatched message rather than on a poll loop, since commit
// progress only ever advances in reaction to one.
func (h *ClientHandler) ResolvePending() {
	if len(h.pending) == 0 {
		return
	}
	var still []*pendingClientOp
	for _, p := range h.pending {
		switch kv.ResolveIndex(h.raftNode, p.index, p.term) {
		case kv.StillPending:
			still = append(still, p)
		case kv.Superseded:
			// A different leader's entry ended up at this slot -
			// ours lost out. Tell the client to retry elsewhere.
			h.replyError(p.req, errAbort, "entry overwritten before commit")
		case kv.Committed:
			applyErr, ok := h.tracker.ResultAt(p.index)
			if !ok {
				// Committed should imply applyCommitted already ran
				// for this index. Defensive fallback: leave it
				// pending rather than dropping the request.
				still = append(still, p)
				continue
			}
			h.replyForResult(p, applyErr)
		}
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

// canonicalToken re-encodes raw (a JSON key/value/from/to straight off the
// wire) as compact JSON text, then rejects any encoding our space-delimited
// SET/CAS command format (kv/statemachine.go) can't carry safely. Numbers,
// booleans, and null always compact to something free of whitespace and
// '=', so they're always accepted - this is what fixed the real Maelstrom
// run, which sends integer keys and values, not strings. A JSON string
// containing whitespace or '=' is the one case still rejected: json.Compact
// only strips insignificant whitespace outside string literals, so
// characters inside the quotes come through unchanged. Known limitation,
// same one the string-only version of this check had.
func canonicalToken(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("missing")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}
	tok := buf.String()
	if strings.ContainsAny(tok, " \t\n=") {
		return "", fmt.Errorf("value %s not representable (contains whitespace or '=')", tok)
	}
	return tok, nil
}