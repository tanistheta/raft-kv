package maelstrom

import (
	"encoding/json"
	"fmt"

	"raft-kv/raft"
)

// rpcBody is the on-the-wire shape of a Raft RPCMessage's body, as
// exchanged between node processes over the Maelstrom transport. Payload
// stays undecoded until Type tells us which concrete struct it holds.
type rpcBody struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	Term    int             `json:"term"`
	Payload json.RawMessage `json:"payload"`
}

// NetworkAdapter implements raft.Network on top of a maelstrom.Node's
// transport. It lets a raft.Node gossip RequestVote/AppendEntries RPCs
// through Maelstrom exactly the way it gossips them through sim.Network in
// tests - raft/ itself never knows the difference.
//
// One NetworkAdapter wraps exactly one maelstrom.Node and expects exactly
// one raft.Node to register against it, matching the one-raft-node-per-OS-
// process model Maelstrom runs.
type NetworkAdapter struct {
	node    *Node
	handler func(raft.RPCMessage)
}

// NewNetworkAdapter wires itself in as node's Handler and returns an
// adapter ready to be assigned to a raft.Node's Network field.
func NewNetworkAdapter(node *Node) *NetworkAdapter {
	a := &NetworkAdapter{node: node}
	node.Handler = a.dispatch
	return a
}

// Register implements raft.Network. id is expected to equal the underlying
// maelstrom.Node's own NodeID; it's accepted rather than validated so a
// raft.Node under test can register under any id.
func (a *NetworkAdapter) Register(id raft.NodeID, handler func(raft.RPCMessage)) {
	a.handler = handler
}

// Send implements raft.Network by marshaling msg onto the wire as a
// Maelstrom message addressed to to.
func (a *NetworkAdapter) Send(to raft.NodeID, msg raft.RPCMessage) error {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("marshaling %s payload: %w", msg.Type, err)
	}
	body := map[string]any{
		"type":    msg.Type,
		"from":    string(msg.From),
		"term":    msg.Term,
		"payload": json.RawMessage(payload),
	}
	return a.node.Send(string(to), body)
}

// dispatch is wired as the maelstrom.Node's Handler. It decodes an incoming
// wire message back into a raft.RPCMessage - resolving Payload's concrete
// type from Type - and hands the result to whatever handler Register gave
// us. Messages this adapter doesn't recognize (any non-Raft-RPC traffic)
// are silently dropped rather than crashing the process; piece C will add
// its own dispatch for client ops (read/write/cas) ahead of this one.
func (a *NetworkAdapter) dispatch(msg Message) {
	var body rpcBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return
	}

	payload, err := decodeRPCPayload(body.Type, body.Payload)
	if err != nil || a.handler == nil {
		return
	}

	a.handler(raft.RPCMessage{
		Type:    body.Type,
		From:    raft.NodeID(body.From),
		To:      raft.NodeID(a.node.NodeID),
		Term:    body.Term,
		Payload: payload,
	})
}

// decodeRPCPayload decodes raw into the concrete Go type raft.HandleMessage
// expects to type-assert for the given RPC type. It must stay in lockstep
// with the switch in raft.Node.HandleMessage (raft/node.go).
func decodeRPCPayload(msgType string, raw json.RawMessage) (any, error) {
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