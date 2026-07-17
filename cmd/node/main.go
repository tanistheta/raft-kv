// Command node runs one raft-kv cluster member as a real OS process:
// durable storage (prod.WAL), peer-to-peer replication over real gRPC
// (prod.GRPCTransport), and a client-facing gRPC service
// (prod.ClientAPI) - the Phase 4 counterpart to cmd/sim (in-memory DST)
// and cmd/maelstrom (Maelstrom's stdin/stdout protocol).
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"raft-kv/kv"
	"raft-kv/prod"
	"raft-kv/prod/raftrpc"
	"raft-kv/raft"
)

// peerList collects repeated -peer flags into a slice. flag.Value rather
// than a plain string flag because a cluster needs one entry per member,
// including this node's own - see main()'s comment on why self is
// described the same way as every other peer instead of via separate
// -listen flags.
type peerList []string

func (p *peerList) String() string { return strings.Join(*p, ",") }
func (p *peerList) Set(v string) error {
	*p = append(*p, v)
	return nil
}

// peerSpec is one -peer entry after parsing: an id and the two addresses
// that id's node listens on.
type peerSpec struct {
	id         raft.NodeID
	raftAddr   string
	clientAddr string
}

// parsePeer parses "id=raftAddr=clientAddr", e.g.
// "n1=127.0.0.1:9001=127.0.0.1:9101". '=' is safe as a separator since
// neither a NodeID nor a host:port address can legitimately contain one.
func parsePeer(spec string) (peerSpec, error) {
	parts := strings.Split(spec, "=")
	if len(parts) != 3 {
		return peerSpec{}, fmt.Errorf("expected id=raftAddr=clientAddr, got %q", spec)
	}
	return peerSpec{id: raft.NodeID(parts[0]), raftAddr: parts[1], clientAddr: parts[2]}, nil
}

func main() {
	var (
		idFlag         = flag.String("id", "", "this node's ID (must match one -peer entry's id)")
		dataDirFlag    = flag.String("data-dir", "", "directory for this node's WAL (log + state)")
		raftListenFlag = flag.String("raft-listen", "", "override where this node's raft transport binds (default: this node's own -peer raft address)")
		peers          peerList
	)
	// One -peer flag per cluster member, self included: this node finds
	// its own listen addresses by matching -id against the same list
	// every node is given, rather than via separate -raft-listen/
	// -client-listen flags. That way every node in a Docker Compose
	// cluster (Phase 4E) can be handed the identical peer list and differ
	// only in -id - one fewer thing to get wrong per container.
	flag.Var(&peers, "peer", "id=raftAddr=clientAddr for one cluster member; repeat once per member, including this node")
	flag.Parse()

	if err := run(*idFlag, *dataDirFlag, *raftListenFlag, []string(peers)); err != nil {
		fmt.Fprintf(os.Stderr, "raft-kv node exited: %v\n", err)
		os.Exit(1)
	}
}

func run(idStr, dataDir, raftListen string, peerSpecs []string) error {
	if idStr == "" {
		return fmt.Errorf("-id is required")
	}
	if dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	if len(peerSpecs) == 0 {
		return fmt.Errorf("at least one -peer is required (including this node's own entry)")
	}
	selfID := raft.NodeID(idStr)

	raftAddrs := make(map[raft.NodeID]string)
	clientAddrs := make(map[raft.NodeID]string)
	var allPeerIDs []raft.NodeID
	for _, spec := range peerSpecs {
		p, err := parsePeer(spec)
		if err != nil {
			return fmt.Errorf("-peer %q: %w", spec, err)
		}
		if _, dup := raftAddrs[p.id]; dup {
			return fmt.Errorf("-peer: duplicate id %q", p.id)
		}
		raftAddrs[p.id] = p.raftAddr
		clientAddrs[p.id] = p.clientAddr
		allPeerIDs = append(allPeerIDs, p.id)
	}
	selfRaftAddr, ok := raftAddrs[selfID]
	if !ok {
		return fmt.Errorf("-id %q does not match any -peer entry", selfID)
	}
	selfClientAddr := clientAddrs[selfID]

	// Normally this node binds to exactly the address its own -peer entry
	// advertises to everyone else - true for a plain local run and for
	// Piece E's Docker Compose setup, where every container is reachable
	// directly by its hostname. -raft-listen exists for the one case
	// where that's no longer true: a chaos setup (docker-compose.chaos.yml)
	// where peers dial this node through a toxiproxy proxy instead of
	// directly, so the address peers use (in the shared -peer list) and
	// the address this node actually binds to have to be allowed to
	// differ. Left unset, behavior is identical to before this flag
	// existed.
	if raftListen != "" {
		selfRaftAddr = raftListen
	}

	// raft.Node.Peers must list every *other* member, not itself -
	// mirrors sim's cluster setup and maelstrom's NodeIDs-minus-self
	// convention (maelstrom/process.go).
	var otherIDs []raft.NodeID
	otherRaftAddrs := make(map[raft.NodeID]string)
	otherClientAddrs := make(map[raft.NodeID]string)
	for _, id := range allPeerIDs {
		if id == selfID {
			continue
		}
		otherIDs = append(otherIDs, id)
		otherRaftAddrs[id] = raftAddrs[id]
		otherClientAddrs[id] = clientAddrs[id]
	}

	wal, err := prod.NewWAL(dataDir)
	if err != nil {
		return fmt.Errorf("opening WAL: %w", err)
	}
	defer wal.Close()

	// Shared with GRPCTransport, ClientAPI, and RealClock - every path
	// into raftNode goes through this one lock. See clock.go's doc
	// comment for why a second mutex here would race instead of just
	// failing to compile.
	var nodeMu sync.Mutex

	transport, err := prod.NewGRPCTransport(selfID, selfRaftAddr, otherRaftAddrs, &nodeMu)
	if err != nil {
		return fmt.Errorf("starting raft transport: %w", err)
	}
	defer transport.Close()

	store := kv.NewStateMachine()
	tracker := kv.NewResultTracker(store)

	raftNode := &raft.Node{
		NodeID:       selfID,
		Peers:        otherIDs,
		Role:         raft.Follower,
		StateMachine: tracker,
		Clock:        prod.NewRealClock(&nodeMu),
		Network:      transport,
		Storage:      wal,
		RNG:          prod.RealRNG{},
	}

	nodeMu.Lock()
	raftNode.Start()
	nodeMu.Unlock()

	clientAPI := prod.NewClientAPI(raftNode, store, tracker, otherClientAddrs, &nodeMu)

	var clientServer *grpc.Server
	if selfClientAddr != "" {
		lis, err := net.Listen("tcp", selfClientAddr)
		if err != nil {
			return fmt.Errorf("starting client api listener: %w", err)
		}
		clientServer = grpc.NewServer()
		raftrpc.RegisterClientAPIServer(clientServer, clientAPI)
		go func() {
			// ErrServerStopped (via GracefulStop below) is expected on
			// shutdown and not actionable, same as GRPCTransport's
			// server.Serve call in grpc_transport.go.
			_ = clientServer.Serve(lis)
		}()
	}

	fmt.Fprintf(os.Stderr, "raft-kv node %s: raft=%s client=%s data-dir=%s peers=%v\n",
		selfID, selfRaftAddr, selfClientAddr, dataDir, otherIDs)

	// Block until asked to stop, then shut down in dependency order:
	// stop accepting new client work first, then stop the Raft node
	// (Stop() only flips a flag other goroutines check - it doesn't
	// itself wait for anything), then close the transport and WAL via
	// the deferred Close calls above.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if clientServer != nil {
		stopped := make(chan struct{})
		go func() {
			clientServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			clientServer.Stop() // force-close if graceful drain overruns the deadline
		}
	}

	nodeMu.Lock()
	raftNode.Stop()
	nodeMu.Unlock()

	return nil
}