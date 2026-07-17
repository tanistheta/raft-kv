package maelstrom

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type Message struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

type baseBody struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

type initBody struct {
	baseBody
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type Node struct {
	NodeID  string
	NodeIDs []string

	Handler func(msg Message)

	// OnInit, if set, runs once init has populated NodeID/NodeIDs above and
	// before this node's init_ok reply goes out. It exists because whoever
	// actually builds the raft.Node (peers, storage, etc.) can't do so
	// until NodeID/NodeIDs are known, but Run's loop only learns them
	// mid-stream from Maelstrom's first message - see RunProcess in
	// process.go, the real caller. Returning an error aborts Run without
	// replying, so Maelstrom sees a missing init_ok rather than a node
	// that claims to be ready but isn't actually wired up.
	OnInit func(nodeID string, nodeIDs []string) error

	in    io.Reader
	out   *bufio.Writer
	outMu sync.Mutex

	msgID   int
	msgIDMu sync.Mutex
}

func NewNode() *Node {
	return NewNodeWithIO(os.Stdin, os.Stdout)
}

func NewNodeWithIO(in io.Reader, out io.Writer) *Node {
	return &Node{in: in, out: bufio.NewWriter(out)}
}

func (n *Node) nextMsgID() int {
	n.msgIDMu.Lock()
	defer n.msgIDMu.Unlock()
	n.msgID++
	return n.msgID
}

func (n *Node) Send(dest string, body map[string]any) error {
	body["msg_id"] = n.nextMsgID()
	return n.write(dest, body)
}

func (n *Node) Reply(req Message, body map[string]any) error {
	var reqBody baseBody
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return fmt.Errorf("reply: decoding request body: %w", err)
	}
	body["in_reply_to"] = reqBody.MsgID
	return n.write(req.Src, body)
}

func (n *Node) write(dest string, body any) error {
	rawBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling body: %w", err)
	}
	line, err := json.Marshal(Message{Src: n.NodeID, Dest: dest, Body: rawBody})
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	n.outMu.Lock()
	defer n.outMu.Unlock()
	if _, err := n.out.Write(line); err != nil {
		return err
	}
	if err := n.out.WriteByte('\n'); err != nil {
		return err
	}
	return n.out.Flush()
}

func (n *Node) Run() error {
	scanner := bufio.NewScanner(n.in)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var msg Message
		if err := json.Unmarshal(line, &msg); err != nil {
			return fmt.Errorf("decoding message: %w", err)
		}

		var body baseBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("decoding body: %w", err)
		}

		if body.Type == "init" {
			if err := n.handleInit(msg); err != nil {
				return err
			}
			continue
		}

		if n.Handler != nil {
			n.Handler(msg)
		}
	}
	return scanner.Err()
}

func (n *Node) handleInit(msg Message) error {
	var body initBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("decoding init body: %w", err)
	}
	n.NodeID = body.NodeID
	n.NodeIDs = body.NodeIDs
	if n.OnInit != nil {
		if err := n.OnInit(n.NodeID, n.NodeIDs); err != nil {
			return fmt.Errorf("OnInit: %w", err)
		}
	}
	return n.Reply(msg, map[string]any{"type": "init_ok"})
}