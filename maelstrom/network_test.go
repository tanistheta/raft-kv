package maelstrom

import (
	"bytes"
	"testing"

	"raft-kv/raft"
)

// roundTrip sends msg from a NetworkAdapter over node A and feeds exactly
// what A wrote to stdout into node B's dispatch, returning whatever B's
// registered handler received. This exercises the real marshal/unmarshal
// path end to end instead of testing Send and dispatch in isolation.
func roundTrip(t *testing.T, msg raft.RPCMessage) raft.RPCMessage {
	t.Helper()

	var wire bytes.Buffer
	nodeA := NewNodeWithIO(nil, &wire)
	nodeA.NodeID = "n1"
	adapterA := NewNetworkAdapter(nodeA)

	if err := adapterA.Send("n2", msg); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	nodeB := NewNodeWithIO(&wire, &bytes.Buffer{})
	nodeB.NodeID = "n2"
	adapterB := NewNetworkAdapter(nodeB)

	var received raft.RPCMessage
	var gotCall bool
	adapterB.Register("n2", func(m raft.RPCMessage) {
		received = m
		gotCall = true
	})

	if err := nodeB.Run(); err != nil {
		t.Fatalf("nodeB.Run returned error: %v", err)
	}
	if !gotCall {
		t.Fatalf("handler was never called")
	}
	return received
}

func TestNetworkAdapterRoundTripsRequestVote(t *testing.T) {
	sent := raft.RPCMessage{
		Type: "RequestVote",
		From: "n1",
		Term: 3,
		Payload: raft.RequestVoteArgs{
			Term:         3,
			CandidateID:  "n1",
			LastLogIndex: 5,
			LastLogTerm:  2,
		},
	}
	got := roundTrip(t, sent)

	if got.Type != sent.Type || got.From != sent.From || got.Term != sent.Term {
		t.Fatalf("envelope mismatch: got %+v, want %+v", got, sent)
	}
	payload, ok := got.Payload.(raft.RequestVoteArgs)
	if !ok {
		t.Fatalf("Payload type = %T, want raft.RequestVoteArgs", got.Payload)
	}
	if payload != sent.Payload.(raft.RequestVoteArgs) {
		t.Errorf("payload mismatch: got %+v, want %+v", payload, sent.Payload)
	}
}

func TestNetworkAdapterRoundTripsAppendEntries(t *testing.T) {
	sent := raft.RPCMessage{
		Type: "AppendEntries",
		From: "n1",
		Term: 4,
		Payload: raft.AppendEntriesArgs{
			Term:         4,
			LeaderID:     "n1",
			PrevLogIndex: 2,
			PrevLogTerm:  3,
			Entries: []raft.LogEntry{
				{Term: 4, Index: 3, Command: []byte("SET x=1")},
			},
			LeaderCommit: 2,
		},
	}
	got := roundTrip(t, sent)

	payload, ok := got.Payload.(raft.AppendEntriesArgs)
	if !ok {
		t.Fatalf("Payload type = %T, want raft.AppendEntriesArgs", got.Payload)
	}
	if len(payload.Entries) != 1 || string(payload.Entries[0].Command) != "SET x=1" {
		t.Errorf("entries mismatch: got %+v", payload.Entries)
	}
	if payload.LeaderID != "n1" || payload.PrevLogIndex != 2 {
		t.Errorf("payload mismatch: got %+v", payload)
	}
}

func TestNetworkAdapterRoundTripsReplies(t *testing.T) {
	voteSent := raft.RPCMessage{
		Type: "RequestVoteReply", From: "n2", Term: 3,
		Payload: raft.RequestVoteReply{Term: 3, VoteGranted: true},
	}
	got := roundTrip(t, voteSent)
	votePayload, ok := got.Payload.(raft.RequestVoteReply)
	if !ok || !votePayload.VoteGranted {
		t.Fatalf("RequestVoteReply round trip failed: %+v (ok=%v)", got, ok)
	}

	appendSent := raft.RPCMessage{
		Type: "AppendEntriesReply", From: "n2", Term: 4,
		Payload: raft.AppendEntriesReply{Term: 4, Success: true, From: "n2", MatchIndex: 7},
	}
	got = roundTrip(t, appendSent)
	appendPayload, ok := got.Payload.(raft.AppendEntriesReply)
	if !ok || appendPayload.MatchIndex != 7 {
		t.Fatalf("AppendEntriesReply round trip failed: %+v (ok=%v)", got, ok)
	}
}

func TestDispatchDropsUnrecognizedRPCType(t *testing.T) {
	var wire bytes.Buffer
	node := NewNodeWithIO(&wire, &bytes.Buffer{})
	node.NodeID = "n1"
	adapter := NewNetworkAdapter(node)

	called := false
	adapter.Register("n1", func(m raft.RPCMessage) { called = true })

	adapter.dispatch(Message{
		Src: "n2", Dest: "n1",
		Body: []byte(`{"type":"SomeUnknownType","from":"n2","term":1,"payload":{}}`),
	})

	if called {
		t.Errorf("handler was called for an unrecognized RPC type")
	}
}