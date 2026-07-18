package prod

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft-kv/kv"
	"raft-kv/prod/raftrpc"
	"raft-kv/raft"
)

// clientOpTimeout bounds how long a leader-side write waits for its
// proposed entry to be decided (Committed or Superseded), and how long a
// forwarded call waits on the leader it was sent to. Set comfortably
// above one heartbeat interval (cmd/node's -heartbeat, 250ms by default)
// so ordinary replication latency never trips it, and comfortably under a
// typical election timeout (150-300ms base, but a real cluster can be
// mid-election when a request arrives) so a genuinely stuck cluster fails
// the caller instead of hanging it indefinitely.
const clientOpTimeout = 3 * time.Second

// clientPollInterval is how often a pending write re-checks
// kv.ResolveIndex while waiting inside clientOpTimeout. There's no
// event-driven wakeup for this the way maelstrom.ClientHandler gets one
// for free from its message-dispatch loop (see maelstrom/client.go's
// ResolvePending, called after every processed message): grpc_transport.go's
// deliver() handles each incoming Raft message on its own goroutine with
// no natural place to hook a "commit progressed" signal without coupling
// ClientAPI to GRPCTransport internals. A short poll is the simplest thing
// that's still cheap - worst case this adds one interval of latency on
// top of whatever heartbeat tick actually committed the entry.
const clientPollInterval = 50 * time.Millisecond

// ClientAPI implements raftrpc.ClientAPIServer against a raft.Node +
// kv.StateMachine pair - the production, gRPC-facing counterpart to
// maelstrom.ClientHandler. Two things maelstrom.ClientHandler doesn't have
// to do that this does: block synchronously until a proposed entry is
// decided (a gRPC unary call returns exactly one reply; there's no
// separate *_ok message sent later the way Maelstrom's reply-by-message
// model allows), and forward a non-leader request to whoever LeaderID
// says the leader is, rather than just telling the caller to go find it
// themselves.
//
// Known limitation: forwarding is one hop trusting whatever LeaderID
// currently says, not a redirect loop with its own hop counter. Two nodes
// can transiently disagree about who's leader (LeaderID is advisory - see
// raft.Node's doc comment on the field) and forward a single request back
// and forth between them; this is bounded only by clientOpTimeout's
// deadline propagating across hops via the gRPC context (each forward
// derives its outgoing deadline from the incoming one), not by a hop
// limit. Acceptable for a 3-5 node cluster where such disagreement
// resolves within one heartbeat interval; a hop-count guard would be the
// natural addition if that stops being true.
type ClientAPI struct {
	raftrpc.UnimplementedClientAPIServer

	raftNode *raft.Node
	store    *kv.StateMachine
	tracker  *kv.ResultTracker

	// nodeMu is the same mutex shared with this node's prod.RealClock and
	// prod.GRPCTransport - see clock.go's doc comment. Every read or
	// write of raftNode below must hold it.
	nodeMu *sync.Mutex

	// peerClientAddrs maps every other cluster member's NodeID to the
	// address *its* ClientAPI server listens on. Deliberately a separate
	// map from GRPCTransport's peerAddrs (RaftTransport addresses) - the
	// two services are expected to listen on different ports of the same
	// host (cmd/node/main.go's job to wire that up consistently), and
	// conflating them would forward client traffic at a node's raft port.
	peerClientAddrs map[raft.NodeID]string

	mu        sync.Mutex // guards peerConns only
	peerConns map[raft.NodeID]raftrpc.ClientAPIClient
}

// NewClientAPI wires up a ClientAPI for one node.
func NewClientAPI(raftNode *raft.Node, store *kv.StateMachine, tracker *kv.ResultTracker, peerClientAddrs map[raft.NodeID]string, nodeMu *sync.Mutex) *ClientAPI {
	return &ClientAPI{
		raftNode:        raftNode,
		store:           store,
		tracker:         tracker,
		nodeMu:          nodeMu,
		peerClientAddrs: peerClientAddrs,
		peerConns:       make(map[raft.NodeID]raftrpc.ClientAPIClient),
	}
}

func (a *ClientAPI) Get(ctx context.Context, req *raftrpc.GetRequest) (*raftrpc.GetReply, error) {
	a.nodeMu.Lock()
	isLeader := a.raftNode.Role == raft.Leader
	leaderID := a.raftNode.LeaderID
	// A freshly-elected leader can have inherited already-durable
	// entries it hasn't yet counted as committed in its own term - same
	// guard as maelstrom/client.go's handleRead and sim/workload.go's
	// read path (see docs/bugs.md, "stale/missing reads").
	fresh := a.raftNode.CommitIndex >= a.raftNode.LastLogIndex()
	a.nodeMu.Unlock()

	if !isLeader {
		if leaderID == "" {
			return &raftrpc.GetReply{Status: raftrpc.Status_NO_LEADER}, nil
		}
		reply, err := a.forwardGet(ctx, req, leaderID)
		if err != nil {
			return &raftrpc.GetReply{Status: raftrpc.Status_UNAVAILABLE, LeaderHint: string(leaderID)}, nil
		}
		return reply, nil
	}
	if !fresh {
		return &raftrpc.GetReply{Status: raftrpc.Status_UNAVAILABLE}, nil
	}

	key, err := requireToken(req.Key)
	if err != nil {
		return &raftrpc.GetReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	val, ok := a.store.Get(key)
	if !ok {
		return &raftrpc.GetReply{Status: raftrpc.Status_KEY_NOT_FOUND, ServedBy: string(a.raftNode.NodeID)}, nil
	}
	return &raftrpc.GetReply{Status: raftrpc.Status_OK, Value: val, ServedBy: string(a.raftNode.NodeID)}, nil
}

func (a *ClientAPI) Put(ctx context.Context, req *raftrpc.PutRequest) (*raftrpc.PutReply, error) {
	key, err := requireToken(req.Key)
	if err != nil {
		return &raftrpc.PutReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	value, err := requireToken(req.Value)
	if err != nil {
		return &raftrpc.PutReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	command := []byte(fmt.Sprintf("SET %s=%s", key, value))

	status, applyErr, leaderID := a.proposeAndWait(command)
	if status == raftrpc.Status_NOT_LEADER {
		if leaderID == "" {
			return &raftrpc.PutReply{Status: raftrpc.Status_NO_LEADER}, nil
		}
		reply, err := a.forwardPut(ctx, req, leaderID)
		if err != nil {
			return &raftrpc.PutReply{Status: raftrpc.Status_UNAVAILABLE, LeaderHint: string(leaderID)}, nil
		}
		return reply, nil
	}
	return &raftrpc.PutReply{Status: statusForApplyErr(status, applyErr), ServedBy: string(a.raftNode.NodeID)}, nil
}

func (a *ClientAPI) Delete(ctx context.Context, req *raftrpc.DeleteRequest) (*raftrpc.DeleteReply, error) {
	key, err := requireToken(req.Key)
	if err != nil {
		return &raftrpc.DeleteReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	command := []byte(fmt.Sprintf("DELETE %s", key))

	status, applyErr, leaderID := a.proposeAndWait(command)
	if status == raftrpc.Status_NOT_LEADER {
		if leaderID == "" {
			return &raftrpc.DeleteReply{Status: raftrpc.Status_NO_LEADER}, nil
		}
		reply, err := a.forwardDelete(ctx, req, leaderID)
		if err != nil {
			return &raftrpc.DeleteReply{Status: raftrpc.Status_UNAVAILABLE, LeaderHint: string(leaderID)}, nil
		}
		return reply, nil
	}
	return &raftrpc.DeleteReply{Status: statusForApplyErr(status, applyErr), ServedBy: string(a.raftNode.NodeID)}, nil
}

func (a *ClientAPI) Cas(ctx context.Context, req *raftrpc.CasRequest) (*raftrpc.CasReply, error) {
	key, err := requireToken(req.Key)
	if err != nil {
		return &raftrpc.CasReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	from, err := requireToken(req.From)
	if err != nil {
		return &raftrpc.CasReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	to, err := requireToken(req.To)
	if err != nil {
		return &raftrpc.CasReply{Status: raftrpc.Status_INVALID_REQUEST}, nil
	}
	command := []byte(fmt.Sprintf("CAS %s %s %s", key, from, to))

	status, applyErr, leaderID := a.proposeAndWait(command)
	if status == raftrpc.Status_NOT_LEADER {
		if leaderID == "" {
			return &raftrpc.CasReply{Status: raftrpc.Status_NO_LEADER}, nil
		}
		reply, err := a.forwardCas(ctx, req, leaderID)
		if err != nil {
			return &raftrpc.CasReply{Status: raftrpc.Status_UNAVAILABLE, LeaderHint: string(leaderID)}, nil
		}
		return reply, nil
	}
	return &raftrpc.CasReply{Status: statusForApplyErr(status, applyErr), ServedBy: string(a.raftNode.NodeID)}, nil
}

// Status reports this node's raft state as-is, with no leader-forwarding
// and no freshness check - see client.proto's doc comment on why that's
// correct here even though every other RPC on this type forwards. A
// dashboard polling all three nodes needs each one's own honest view,
// including a stale follower's, not a single cluster-wide answer.
func (a *ClientAPI) Status(ctx context.Context, req *raftrpc.StatusRequest) (*raftrpc.StatusReply, error) {
	a.nodeMu.Lock()
	defer a.nodeMu.Unlock()

	return &raftrpc.StatusReply{
		NodeId:       string(a.raftNode.NodeID),
		Role:         string(a.raftNode.Role),
		Term:         int64(a.raftNode.CurrentTerm),
		LeaderId:     string(a.raftNode.LeaderID),
		CommitIndex:  int64(a.raftNode.CommitIndex),
		LastLogIndex: int64(a.raftNode.LastLogIndex()),
	}, nil
}

// proposeAndWait appends command to the log if this node is currently
// leader, then blocks (polling kv.ResolveIndex, same function
// maelstrom/client.go and sim/workload.go both already use) until it's
// Committed, Superseded, or clientOpTimeout elapses. Returns
// Status_NOT_LEADER immediately, without proposing anything, if this node
// isn't leader - callers are expected to forward in that case rather than
// treat it as a terminal failure.
func (a *ClientAPI) proposeAndWait(command []byte) (status raftrpc.Status, applyErr error, leaderID raft.NodeID) {
	a.nodeMu.Lock()
	if a.raftNode.Role != raft.Leader {
		leaderID = a.raftNode.LeaderID
		a.nodeMu.Unlock()
		return raftrpc.Status_NOT_LEADER, nil, leaderID
	}
	a.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    a.raftNode.CurrentTerm,
		Command: command,
	})
	index := a.raftNode.LastLogIndex()
	term := a.raftNode.CurrentTerm
	a.nodeMu.Unlock()

	deadline := time.Now().Add(clientOpTimeout)
	for {
		a.nodeMu.Lock()
		outcome := kv.ResolveIndex(a.raftNode, index, term)
		a.nodeMu.Unlock()

		switch outcome {
		case kv.Committed:
			if err, ok := a.tracker.ResultAt(index); ok {
				return raftrpc.Status_OK, err, ""
			}
			// applyCommitted hasn't run for this index quite yet;
			// fall through and poll again rather than report a
			// false negative.
		case kv.Superseded:
			return raftrpc.Status_UNAVAILABLE, errors.New("entry overwritten before commit"), ""
		}

		if time.Now().After(deadline) {
			return raftrpc.Status_TIMEOUT, nil, ""
		}
		time.Sleep(clientPollInterval)
	}
}

// statusForApplyErr translates proposeAndWait's outcome into the reply
// status a client sees, once forwarding has already been ruled out.
func statusForApplyErr(status raftrpc.Status, applyErr error) raftrpc.Status {
	if status != raftrpc.Status_OK {
		return status // TIMEOUT or UNAVAILABLE, nothing to translate
	}
	switch {
	case applyErr == nil:
		return raftrpc.Status_OK
	case errors.Is(applyErr, kv.ErrKeyNotFound):
		return raftrpc.Status_KEY_NOT_FOUND
	case errors.Is(applyErr, kv.ErrCASMismatch):
		return raftrpc.Status_CAS_MISMATCH
	default:
		return raftrpc.Status_UNAVAILABLE
	}
}

func (a *ClientAPI) forwardGet(ctx context.Context, req *raftrpc.GetRequest, leaderID raft.NodeID) (*raftrpc.GetReply, error) {
	client, err := a.clientFor(leaderID)
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithTimeout(ctx, clientOpTimeout)
	defer cancel()
	return client.Get(fctx, req)
}

func (a *ClientAPI) forwardPut(ctx context.Context, req *raftrpc.PutRequest, leaderID raft.NodeID) (*raftrpc.PutReply, error) {
	client, err := a.clientFor(leaderID)
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithTimeout(ctx, clientOpTimeout)
	defer cancel()
	return client.Put(fctx, req)
}

func (a *ClientAPI) forwardDelete(ctx context.Context, req *raftrpc.DeleteRequest, leaderID raft.NodeID) (*raftrpc.DeleteReply, error) {
	client, err := a.clientFor(leaderID)
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithTimeout(ctx, clientOpTimeout)
	defer cancel()
	return client.Delete(fctx, req)
}

func (a *ClientAPI) forwardCas(ctx context.Context, req *raftrpc.CasRequest, leaderID raft.NodeID) (*raftrpc.CasReply, error) {
	client, err := a.clientFor(leaderID)
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithTimeout(ctx, clientOpTimeout)
	defer cancel()
	return client.Cas(fctx, req)
}

// clientFor returns a cached ClientAPI connection to id, dialing lazily on
// first use - same pattern as grpc_transport.go's clientFor, over a
// different address map (peerClientAddrs, not RaftTransport peer
// addresses) and a different generated client type.
func (a *ClientAPI) clientFor(id raft.NodeID) (raftrpc.ClientAPIClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if c, ok := a.peerConns[id]; ok {
		return c, nil
	}
	addr, ok := a.peerClientAddrs[id]
	if !ok {
		return nil, fmt.Errorf("client api: no known client address for peer %s", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client api: dialing %s at %s: %w", id, addr, err)
	}
	client := raftrpc.NewClientAPIClient(conn)
	a.peerConns[id] = client
	return client, nil
}

// requireToken rejects a key/value/from/to that kv/statemachine.go's
// space-delimited "SET key=value" / "CAS key from to" command format
// can't carry safely - same restriction maelstrom/client.go's
// canonicalToken enforces on raw JSON tokens, adapted here for plain Go
// strings coming straight off a typed gRPC field instead of json.RawMessage.
func requireToken(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("missing")
	}
	if strings.ContainsAny(s, " \t\n=") {
		return "", fmt.Errorf("value %q not representable (contains whitespace or '=')", s)
	}
	return s, nil
}