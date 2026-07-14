package maelstrom

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func decodeLines(t *testing.T, buf *bytes.Buffer) []Message {
	t.Helper()
	var out []Message
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var msg Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("decoding output line %q: %v", line, err)
		}
		out = append(out, msg)
	}
	return out
}

func TestInitHandshakeSetsNodeIDAndReplies(t *testing.T) {
	in := strings.NewReader(`{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}` + "\n")
	var out bytes.Buffer

	n := NewNodeWithIO(in, &out)
	if err := n.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if n.NodeID != "n1" {
		t.Errorf("NodeID = %q, want %q", n.NodeID, "n1")
	}
	if len(n.NodeIDs) != 3 {
		t.Errorf("NodeIDs = %v, want 3 entries", n.NodeIDs)
	}

	replies := decodeLines(t, &out)
	if len(replies) != 1 {
		t.Fatalf("got %d reply messages, want 1", len(replies))
	}
	if replies[0].Dest != "c1" {
		t.Errorf("reply dest = %q, want %q", replies[0].Dest, "c1")
	}

	var body baseBody
	if err := json.Unmarshal(replies[0].Body, &body); err != nil {
		t.Fatalf("decoding reply body: %v", err)
	}
	if body.Type != "init_ok" {
		t.Errorf("reply type = %q, want %q", body.Type, "init_ok")
	}
	if body.InReplyTo != 1 {
		t.Errorf("in_reply_to = %d, want 1", body.InReplyTo)
	}
}

func TestHandlerReceivesNonInitMessages(t *testing.T) {
	in := strings.NewReader(
		`{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1"]}}` + "\n" +
			`{"src":"c1","dest":"n1","body":{"type":"echo","msg_id":2,"echo":"hi"}}` + "\n",
	)
	var out bytes.Buffer

	n := NewNodeWithIO(in, &out)
	var received []Message
	n.Handler = func(msg Message) {
		received = append(received, msg)
	}

	if err := n.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(received) != 1 {
		t.Fatalf("Handler called %d times, want 1", len(received))
	}
	var body baseBody
	if err := json.Unmarshal(received[0].Body, &body); err != nil {
		t.Fatalf("decoding handled body: %v", err)
	}
	if body.Type != "echo" {
		t.Errorf("handled message type = %q, want %q", body.Type, "echo")
	}
}

func TestReplySetsInReplyToFromRequestMsgID(t *testing.T) {
	var out bytes.Buffer
	n := NewNodeWithIO(strings.NewReader(""), &out)
	n.NodeID = "n1"

	req := Message{
		Src:  "c1",
		Dest: "n1",
		Body: json.RawMessage(`{"type":"echo","msg_id":42,"echo":"hi"}`),
	}
	if err := n.Reply(req, map[string]any{"type": "echo_ok", "echo": "hi"}); err != nil {
		t.Fatalf("Reply returned error: %v", err)
	}

	replies := decodeLines(t, &out)
	if len(replies) != 1 {
		t.Fatalf("got %d messages, want 1", len(replies))
	}
	var body baseBody
	if err := json.Unmarshal(replies[0].Body, &body); err != nil {
		t.Fatalf("decoding reply body: %v", err)
	}
	if body.InReplyTo != 42 {
		t.Errorf("in_reply_to = %d, want 42", body.InReplyTo)
	}
}

func TestSendAssignsIncrementingMsgIDs(t *testing.T) {
	var out bytes.Buffer
	n := NewNodeWithIO(strings.NewReader(""), &out)
	n.NodeID = "n1"

	if err := n.Send("n2", map[string]any{"type": "ping"}); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	if err := n.Send("n2", map[string]any{"type": "ping"}); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	msgs := decodeLines(t, &out)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	var b1, b2 baseBody
	json.Unmarshal(msgs[0].Body, &b1)
	json.Unmarshal(msgs[1].Body, &b2)
	if b1.MsgID != 1 || b2.MsgID != 2 {
		t.Errorf("msg_ids = %d, %d, want 1, 2", b1.MsgID, b2.MsgID)
	}
}