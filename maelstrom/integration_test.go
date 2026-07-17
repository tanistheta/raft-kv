package maelstrom

import (
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"raft-kv/kv"
	"raft-kv/prod"
	"raft-kv/raft"
	"raft-kv/sim"
)

// bus is an in-process stand-in for Maelstrom's own message relay. Each
// node's outgoing writes land here; bus forwards them to the addressed
// node's stdin pipe if it's a cluster member, or records them as a
// "client reply" otherwise (mirroring how a real Maelstrom client would
// receive the reply over its own connection).
type bus struct {
	mu      sync.Mutex
	writers map[string]*io.PipeWriter
	client  []Message
}

func newBus() *bus { return &bus{writers: make(map[string]*io.PipeWriter)} }

func (b *bus) register(id string, w *io.PipeWriter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.writers[id] = w
}

func (b *bus) clientReplies() []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Message, len(b.client))
	copy(out, b.client)
	return out
}

// writerFor returns an io.Writer that forwards each write (one Maelstrom
// message per call, given protocol.go's write() flushes exactly once per
// message) to whichever destination the message names.
func (b *bus) writerFor() io.Writer {
	return busWriter{b}
}

type busWriter struct{ b *bus }

func (w busWriter) Write(p []byte) (int, error) {
	// Copy p: the caller (bufio.Writer) reuses its buffer after Write
	// returns, but delivery below happens on another goroutine.
	line := make([]byte, len(p))
	copy(line, p)

	var msg Message
	if err := json.Unmarshal(line, &msg); err != nil {
		return len(p), nil // malformed line; drop rather than fail the sender
	}

	w.b.mu.Lock()
	dest, ok := w.b.writers[msg.Dest]
	w.b.mu.Unlock()

	if !ok {
		w.b.mu.Lock()
		w.b.client = append(w.b.client, msg)
		w.b.mu.Unlock()
		return len(p), nil
	}

	// io.Pipe is synchronous: a Write blocks until something Reads it.
	// Delivering inline here would block this node's single processing
	// goroutine on the destination's, and two nodes messaging each other
	// at once can deadlock both. A real network doesn't block the sender
	// on the receiver being busy, so neither should this bus.
	go dest.Write(line)
	return len(p), nil
}

// testNode bundles everything one cluster member needs, so the test can
// reach into it (send client requests, inspect the local store) without
// going through the wire.
type testNode struct {
	id        raft.NodeID
	mu        *sync.Mutex
	transport *Node
	raftNode  *raft.Node
	store     *kv.StateMachine
	router    *Router

	// in is the read end of this node's stdin pipe. transport.Run() blocks
	// reading from it on its own goroutine; closing it in Cleanup is what
	// unblocks that Read and lets Run() return, instead of leaking the
	// goroutine past the end of the test the way raftNode.Stop() alone
	// does (Stop only halts Raft logic - it does nothing to a goroutine
	// parked in a blocking Read).
	in *io.PipeReader
}

func newTestCluster(t *testing.T, ids []string) (map[string]*testNode, *bus) {
	t.Helper()
	b := newBus()
	nodes := make(map[string]*testNode)

	for _, id := range ids {
		pr, pw := io.Pipe()
		b.register(id, pw)

		mu := &sync.Mutex{}
		transport := NewNodeWithIO(pr, b.writerFor())
		transport.NodeID = id

		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, raft.NodeID(other))
			}
		}

		store := kv.NewStateMachine()
		tracker := kv.NewResultTracker(store)
		network := NewNetworkAdapter(transport)

		raftNode := &raft.Node{
			NodeID:       raft.NodeID(id),
			Peers:        peers,
			Clock:        prod.NewRealClock(mu),
			Network:      network,
			Storage:      sim.NewMemStorage(),
			StateMachine: tracker,
			RNG:          prod.RealRNG{},
		}
		client := NewClientHandler(transport, raftNode, store, tracker)
		router := NewRouter(transport, network, client, mu)

		nodes[id] = &testNode{id: raft.NodeID(id), mu: mu, transport: transport, raftNode: raftNode, store: store, router: router, in: pr}
	}

	for _, n := range nodes {
		n.mu.Lock()
		n.raftNode.Start()
		n.mu.Unlock()
		go n.transport.Run()
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.mu.Lock()
			n.raftNode.Stop()
			n.mu.Unlock()
			n.in.Close()
		}
	})

	return nodes, b
}

// awaitLeader polls every node's Role (under its own lock) until exactly
// one leader emerges, or fails the test after timeout.
func awaitLeader(t *testing.T, nodes map[string]*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			n.mu.Lock()
			isLeader := n.raftNode.Role == raft.Leader
			n.mu.Unlock()
			if isLeader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

// awaitClientReply polls bus replies for one matching msgID, returning its
// decoded body.
func awaitClientReply(t *testing.T, b *bus, msgID int, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, msg := range b.clientReplies() {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				continue
			}
			if inReplyTo, ok := body["in_reply_to"].(float64); ok && int(inReplyTo) == msgID {
				return body
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no reply to msg_id %d within %v", msgID, timeout)
	return nil
}

func sendClientOp(leader *testNode, msgID int, body map[string]any) {
	body["msg_id"] = msgID
	raw, _ := json.Marshal(body)
	leader.router.Dispatch(Message{Src: "c1", Dest: string(leader.id), Body: raw})
}

func TestClusterServesWriteReadCASOverMaelstromTransport(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	nodes, b := newTestCluster(t, ids)
	leader := awaitLeader(t, nodes, 3*time.Second)

	sendClientOp(leader, 1, map[string]any{"type": "write", "key": "x", "value": "1"})
	reply := awaitClientReply(t, b, 1, 2*time.Second)
	if reply["type"] != "write_ok" {
		t.Fatalf("write reply = %+v, want type write_ok", reply)
	}

	sendClientOp(leader, 2, map[string]any{"type": "read", "key": "x"})
	reply = awaitClientReply(t, b, 2, 2*time.Second)
	if reply["type"] != "read_ok" || reply["value"] != "1" {
		t.Fatalf("read reply = %+v, want type read_ok value 1", reply)
	}

	sendClientOp(leader, 3, map[string]any{"type": "cas", "key": "x", "from": "1", "to": "2"})
	reply = awaitClientReply(t, b, 3, 2*time.Second)
	if reply["type"] != "cas_ok" {
		t.Fatalf("cas reply = %+v, want type cas_ok", reply)
	}

	sendClientOp(leader, 4, map[string]any{"type": "read", "key": "x"})
	reply = awaitClientReply(t, b, 4, 2*time.Second)
	if reply["value"] != "2" {
		t.Fatalf("read after cas = %+v, want value 2", reply)
	}

	sendClientOp(leader, 5, map[string]any{"type": "cas", "key": "x", "from": "99", "to": "3"})
	reply = awaitClientReply(t, b, 5, 2*time.Second)
	if reply["type"] != "error" {
		t.Fatalf("cas-mismatch reply = %+v, want type error", reply)
	}
	if code, _ := reply["code"].(float64); int(code) != errPreconditionFailed {
		t.Fatalf("cas-mismatch error code = %v, want %d", reply["code"], errPreconditionFailed)
	}

	// Give replication a moment, then confirm all three replicas agree -
	// the whole point of routing writes through consensus. store.Get takes
	// the same JSON-compact token canonicalToken produces (quotes and
	// all), since that's what ClientHandler now writes through the log -
	// see canonicalToken's doc comment in client.go.
	deadline := time.Now().Add(2 * time.Second)
	for {
		allMatch := true
		for _, n := range nodes {
			n.mu.Lock()
			val, ok := n.store.Get(`"x"`)
			n.mu.Unlock()
			if !ok || val != `"2"` {
				allMatch = false
			}
		}
		if allMatch {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				n.mu.Lock()
				val, ok := n.store.Get(`"x"`)
				n.mu.Unlock()
				t.Errorf("node %s store[x] = %q, %v", n.id, val, ok)
			}
			t.Fatal("replicas never converged on x=2")
		}
		time.Sleep(20 * time.Millisecond)
	}
}