package prod

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"raft-kv/prod/raftrpc"
	"raft-kv/raft"
)

// sendTimeout bounds how long an outgoing Send waits for the peer to ack
// receipt. This is a transport-level timeout, not a Raft-level one: a
// RequestVote that never gets a reply just means that vote never comes in
// and the candidate loses the election on its own timeout, same as a
// dropped message on any other transport. Kept well under a typical
// election timeout so a genuinely unreachable peer can't stall the caller
// long enough to look like an unresponsive node from the outside.
const sendTimeout = 2 * time.Second

// GRPCTransport implements raft.Network over real gRPC connections between
// real OS processes - the transport Phase 4's Docker Compose cluster and
// toxiproxy chaos actually run on, in contrast to sim.Network's in-memory
// routing and maelstrom.NetworkAdapter's JSON-over-stdin framing. Same
// one-network-per-node model as NetworkAdapter: one GRPCTransport wraps one
// node's listener and dials out to its peers, and expects exactly one
// raft.Node to Register against it.
//
// Known limitation: connections are unauthenticated and unencrypted
// (grpc/credentials/insecure). Fine for a cluster confined to a private
// Docker network, not fine for anything crossing an untrusted network -
// the natural upgrade path is credentials/tls or mTLS, isolated to
// dial() and NewGRPCTransport's grpc.NewServer call.
type GRPCTransport struct {
	nodeID raft.NodeID

	mu      sync.Mutex // protects handler/peerConns below, internal bookkeeping only
	handler func(raft.RPCMessage)

	// nodeMu is the *external* mutex shared with this node's prod.RealClock
	// (see clock.go's doc comment: "Callers must guard every other path
	// into the same Node with the same mutex"). deliver holds this around
	// every call into the registered handler, since that handler runs
	// straight into raft.Node methods and raft.Node has no locking of its
	// own. Without this, an incoming gRPC message on one goroutine and a
	// firing election/heartbeat timer on another can call into the same
	// *raft.Node concurrently - exactly the race TestClusterElects...
	// caught the first time this was written without it.
	nodeMu *sync.Mutex

	peerAddrs map[raft.NodeID]string
	peerConns map[raft.NodeID]raftrpc.RaftTransportClient

	server   *grpc.Server
	listener net.Listener
}

// grpcServer is the thin adapter that actually satisfies
// raftrpc.RaftTransportServer. It exists as its own type, separate from
// GRPCTransport, because Go doesn't allow one type to have two methods
// both named Send with different signatures - and GRPCTransport needs
// exactly that: raft.Network's outgoing Send(to, msg) error, and the
// generated server interface's incoming Send(ctx, *Envelope) (*Ack,
// error). This type exists only to hold that second signature and
// delegate straight into the transport it wraps.
type grpcServer struct {
	raftrpc.UnimplementedRaftTransportServer
	t *GRPCTransport
}

func (s *grpcServer) Send(ctx context.Context, env *raftrpc.Envelope) (*raftrpc.Ack, error) {
	return s.t.deliver(env)
}

// NewGRPCTransport starts listening on listenAddr and begins serving
// immediately (in a background goroutine) - the returned transport is
// ready to accept incoming RPCs before this function returns, though no
// raft.Node has Registered a handler with it yet (deliver just no-ops
// until one does). peerAddrs maps every other cluster member's NodeID to
// its own listenAddr; Send dials a peer's address the first time it's
// needed and reuses that connection afterward.
//
// mu must be the same *sync.Mutex given to this node's prod.NewRealClock:
// deliver takes it before calling into the registered handler, since that
// handler runs straight into raft.Node and raft.Node assumes a single
// synchronized caller, same as every timer callback does. Passing two
// different mutexes here compiles fine and races at runtime.
func NewGRPCTransport(id raft.NodeID, listenAddr string, peerAddrs map[raft.NodeID]string, mu *sync.Mutex) (*GRPCTransport, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("grpc transport: listen on %s: %w", listenAddr, err)
	}

	t := &GRPCTransport{
		nodeID:    id,
		nodeMu:    mu,
		peerAddrs: peerAddrs,
		peerConns: make(map[raft.NodeID]raftrpc.RaftTransportClient),
		listener:  lis,
	}

	server := grpc.NewServer()
	raftrpc.RegisterRaftTransportServer(server, &grpcServer{t: t})
	t.server = server

	go server.Serve(lis) //nolint:errcheck // Close() below stops this cleanly; a post-Close Serve error is expected and not actionable.

	return t, nil
}

// Register implements raft.Network. id is accepted rather than validated
// against t.nodeID, matching maelstrom.NetworkAdapter's Register - a
// raft.Node under test can register under any id.
func (t *GRPCTransport) Register(id raft.NodeID, handler func(raft.RPCMessage)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

// Send implements raft.Network by dialing (or reusing a cached connection
// to) to's address and delivering msg as one gRPC call. Consistent with
// every other transport in this repo, this is fire-and-forget from the
// caller's point of view: it returns only transport-level errors (peer
// unreachable, timed out), never a reply value - a RequestVote's reply
// comes back later as an independent incoming RequestVoteReply message,
// handled by deliver below, exactly like sim.Network and
// maelstrom.NetworkAdapter.
func (t *GRPCTransport) Send(to raft.NodeID, msg raft.RPCMessage) error {
	client, err := t.clientFor(to)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("grpc transport: marshal %s payload: %w", msg.Type, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	_, err = client.Send(ctx, &raftrpc.Envelope{
		Type:    msg.Type,
		From:    string(msg.From),
		To:      string(to),
		Term:    int64(msg.Term),
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("grpc transport: sending %s to %s: %w", msg.Type, to, err)
	}
	return nil
}

// clientFor returns a cached client connection to id, dialing lazily on
// first use. grpc.NewClient (unlike the older grpc.Dial) doesn't block or
// actually connect here - the real TCP handshake happens on first RPC,
// which is where a genuinely unreachable peer actually surfaces as an
// error, not here.
func (t *GRPCTransport) clientFor(id raft.NodeID) (raftrpc.RaftTransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.peerConns[id]; ok {
		return c, nil
	}
	addr, ok := t.peerAddrs[id]
	if !ok {
		return nil, fmt.Errorf("grpc transport: no known address for peer %s", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc transport: dialing %s at %s: %w", id, addr, err)
	}
	client := raftrpc.NewRaftTransportClient(conn)
	t.peerConns[id] = client
	return client, nil
}

// deliver decodes an incoming Envelope back into a raft.RPCMessage and
// hands it to whatever handler Register gave this transport. Mirrors
// maelstrom/network.go's dispatch method exactly, since both exist to
// bridge the same raft.RPCMessage contract onto a different wire.
func (t *GRPCTransport) deliver(env *raftrpc.Envelope) (*raftrpc.Ack, error) {
	payload, err := decodeGRPCPayload(env.Type, env.Payload)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	t.mu.Lock()
	h := t.handler
	t.mu.Unlock()
	if h == nil {
		// No raft.Node has Registered yet (e.g. a message arrived while
		// the node is still starting up). Dropping it is safe: Raft is
		// built to tolerate lost messages, that's the entire premise
		// behind resending on timeout.
		return &raftrpc.Ack{}, nil
	}

	// Processing happens on its own goroutine rather than inline before
	// this RPC returns. This isn't just an optimization - it's required
	// for correctness. handleRequestVote/handleAppendEntries synchronously
	// call Network.Send to deliver their reply, sometimes back to the very
	// node that sent this message. If that original sender is still
	// blocked inside its own Send call holding nodeMu (waiting for THIS
	// RPC to return), and this RPC in turn waits for h(msg) - which tries
	// to send the reply back through a Send call that needs that same
	// sender to free nodeMu to receive it - the two calls deadlock each
	// other. Acking immediately and handling the message asynchronously
	// breaks that cycle: this RPC only ever promises "delivered," never
	// "fully processed," which is the actual contract every other
	// transport here (sim.Network, maelstrom.NetworkAdapter) already has.
	go func() {
		t.nodeMu.Lock()
		defer t.nodeMu.Unlock()
		h(raft.RPCMessage{
			Type:    env.Type,
			From:    raft.NodeID(env.From),
			To:      t.nodeID,
			Term:    int(env.Term),
			Payload: payload,
		})
	}()
	return &raftrpc.Ack{}, nil
}

// Close stops accepting new RPCs, waits for in-flight ones to finish, and
// closes the listener. Not part of raft.Network; callers building a real
// node (cmd/node/main.go) should call this on shutdown.
func (t *GRPCTransport) Close() error {
	t.server.GracefulStop()
	return nil
}

// decodeGRPCPayload decodes raw into the concrete Go type raft.HandleMessage
// expects to type-assert for the given RPC type. Must stay in lockstep with
// the switch in raft.Node.HandleMessage (raft/node.go) - exactly the same
// requirement maelstrom/network.go's decodeRPCPayload has, kept as a
// separate copy rather than a shared helper since the two live in
// different packages (maelstrom/ intentionally doesn't import prod/, or
// vice versa) and the function is four lines of pure stdlib.
func decodeGRPCPayload(msgType string, raw []byte) (any, error) {
	switch msgType {
	case "RequestVote":
		var p raft.RequestVoteArgs
		err := json.Unmarshal(raw, &p)
		return p, err
	case "RequestVoteReply":
		var p raft.RequestVoteReply
		err := json.Unmarshal(raw, &p)
		return p, err
	case "AppendEntries":
		var p raft.AppendEntriesArgs
		err := json.Unmarshal(raw, &p)
		return p, err
	case "AppendEntriesReply":
		var p raft.AppendEntriesReply
		err := json.Unmarshal(raw, &p)
		return p, err
	default:
		return nil, fmt.Errorf("unknown RPC type %q", msgType)
	}
}