package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft-kv/prod/raftrpc"
)

// nodeState is one node's last-known Status, plus whether the most recent
// poll actually reached it. Unreachable is the dashboard's only signal
// for "this node is down" - there's no separate health RPC, since a
// failed Status call already means the same thing a health check would.
type nodeState struct {
	ID        string `json:"id"`
	Reachable bool   `json:"reachable"`
	Role      string `json:"role,omitempty"`
	// Term, CommitIndex, and LastLogIndex deliberately have no omitempty:
	// 0 is their real value for a fresh cluster (term 0, nothing
	// committed yet), and omitempty would drop the field entirely rather
	// than send 0 - the frontend can't tell "legitimately zero" apart
	// from "field missing" without it.
	Term         int64  `json:"term"`
	LeaderID     string `json:"leaderId,omitempty"`
	CommitIndex  int64  `json:"commitIndex"`
	LastLogIndex int64  `json:"lastLogIndex"`
	Err          string `json:"err,omitempty"`
}

// opEntry is one KV operation the dashboard itself proxied - see
// handleKV. This is the entire "live traffic" feed: the dashboard is the
// only path demo traffic takes, so there's nothing to observe on the
// node side beyond what's already captured here.
type opEntry struct {
	Time     time.Time `json:"time"`
	Op       string    `json:"op"`
	Key      string    `json:"key"`
	Value    string    `json:"value,omitempty"`
	Status   string    `json:"status"`
	ServedBy string    `json:"servedBy,omitempty"`
	Err      string    `json:"err,omitempty"`
}

// maxOps bounds the in-memory ops feed. This is a demo tool, not a
// durable log - old entries just fall off.
const maxOps = 100

type clusterView struct {
	order []string // node IDs, in the order given on the command line - keeps /api/state's node ordering stable across polls

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]raftrpc.ClientAPIClient
	states  map[string]nodeState
	ops     []opEntry
}

func newClusterView(specs map[string]string, order []string) (*clusterView, error) {
	cv := &clusterView{
		order:   order,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]raftrpc.ClientAPIClient),
		states:  make(map[string]nodeState),
	}
	for _, id := range order {
		addr := specs[id]
		// grpc.NewClient doesn't dial immediately (same lazy-connect
		// behavior client_api.go's clientFor relies on) - a node that's
		// down when the dashboard starts still gets a client here and
		// simply shows up as unreachable on the first poll, rather than
		// failing dashboard startup entirely.
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cv.close()
			return nil, fmt.Errorf("dialing %s at %s: %w", id, addr, err)
		}
		cv.conns[id] = conn
		cv.clients[id] = raftrpc.NewClientAPIClient(conn)
		cv.states[id] = nodeState{ID: id, Reachable: false}
	}
	return cv, nil
}

func (cv *clusterView) close() {
	for _, conn := range cv.conns {
		_ = conn.Close()
	}
}

// pollLoop refreshes every node's Status on a fixed interval for as long
// as the dashboard process runs. There's no shutdown signal wired up to
// stop this - the goroutine just dies with the process, same as
// GRPCTransport's Serve loop in the main cluster code.
func (cv *clusterView) pollLoop() {
	cv.pollOnce()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for range ticker.C {
		cv.pollOnce()
	}
}

func (cv *clusterView) pollOnce() {
	var wg sync.WaitGroup
	for _, id := range cv.order {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			cv.pollNode(id)
		}()
	}
	wg.Wait()
}

func (cv *clusterView) pollNode(id string) {
	cv.mu.Lock()
	client := cv.clients[id]
	cv.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()

	reply, err := client.Status(ctx, &raftrpc.StatusRequest{})

	cv.mu.Lock()
	defer cv.mu.Unlock()
	if err != nil {
		cv.states[id] = nodeState{ID: id, Reachable: false, Err: err.Error()}
		return
	}
	cv.states[id] = nodeState{
		ID:           id,
		Reachable:    true,
		Role:         reply.Role,
		Term:         reply.Term,
		LeaderID:     reply.LeaderId,
		CommitIndex:  reply.CommitIndex,
		LastLogIndex: reply.LastLogIndex,
	}
}

func (cv *clusterView) handleState(w http.ResponseWriter, r *http.Request) {
	cv.mu.Lock()
	nodes := make([]nodeState, 0, len(cv.order))
	for _, id := range cv.order {
		nodes = append(nodes, cv.states[id])
	}
	// Copy, not a slice of the live backing array: ops is appended to
	// under cv.mu from handleKV, and json.NewEncoder below runs after
	// this function releases the lock.
	ops := make([]opEntry, len(cv.ops))
	copy(ops, cv.ops)
	cv.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Nodes []nodeState `json:"nodes"`
		Ops   []opEntry   `json:"ops"`
	}{nodes, ops})
}

// kvRequest is the body handleKV expects: {"op":"put","key":"foo","value":"bar"}
// (value ignored for get/delete).
type kvRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// handleKV proxies one KV operation to the cluster - to whichever node
// happens to be first in cv.order, relying on ClientAPI's existing
// leader-forwarding (client_api.go) rather than the dashboard tracking
// who's leader itself. Every call is recorded into cv.ops regardless of
// outcome, since a failed op (e.g. mid-election NO_LEADER) is exactly
// the kind of thing a chaos demo wants visible in the traffic feed.
func (cv *clusterView) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req kvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	cv.mu.Lock()
	entryNode := cv.order[0]
	client := cv.clients[entryNode]
	cv.mu.Unlock()

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	entry := opEntry{Time: time.Now(), Op: req.Op, Key: req.Key, Value: req.Value}

	switch req.Op {
	case "put":
		reply, err := client.Put(ctx, &raftrpc.PutRequest{Key: req.Key, Value: req.Value})
		fillFromPut(&entry, reply, err)
	case "get":
		reply, err := client.Get(ctx, &raftrpc.GetRequest{Key: req.Key})
		if err == nil {
			entry.Value = reply.Value
		}
		fillFromGet(&entry, reply, err)
	case "delete":
		reply, err := client.Delete(ctx, &raftrpc.DeleteRequest{Key: req.Key})
		fillFromDelete(&entry, reply, err)
	default:
		http.Error(w, fmt.Sprintf("unknown op %q (want put, get, or delete)", req.Op), http.StatusBadRequest)
		return
	}

	cv.mu.Lock()
	cv.ops = append(cv.ops, entry)
	if len(cv.ops) > maxOps {
		cv.ops = cv.ops[len(cv.ops)-maxOps:]
	}
	cv.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entry)
}

func fillFromPut(e *opEntry, reply *raftrpc.PutReply, err error) {
	if err != nil {
		e.Err = err.Error()
		return
	}
	e.Status = reply.Status.String()
	e.ServedBy = reply.ServedBy
}

func fillFromGet(e *opEntry, reply *raftrpc.GetReply, err error) {
	if err != nil {
		e.Err = err.Error()
		return
	}
	e.Status = reply.Status.String()
	e.ServedBy = reply.ServedBy
}

func fillFromDelete(e *opEntry, reply *raftrpc.DeleteReply, err error) {
	if err != nil {
		e.Err = err.Error()
		return
	}
	e.Status = reply.Status.String()
	e.ServedBy = reply.ServedBy
}
