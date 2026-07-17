// Command smoketest is a throwaway verification tool, not part of the
// project proper - it exists only to prove cmd/node's three pieces
// (WAL, GRPCTransport, ClientAPI) actually work together as real OS
// processes, since raft-kv node itself logs nothing after startup.
// Point it at ANY node's client address: if that node isn't leader,
// ClientAPI forwards the request transparently, so success here proves
// forwarding works too, not just the direct-leader path.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft-kv/prod/raftrpc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9101", "any node's client API address")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	client := raftrpc.NewClientAPIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Printf("PUT foo=bar via %s ...\n", *addr)
	putReply, err := client.Put(ctx, &raftrpc.PutRequest{Key: "foo", Value: "bar"})
	if err != nil {
		log.Fatalf("Put RPC failed: %v", err)
	}
	fmt.Printf("  status=%s served_by=%q\n", putReply.Status, putReply.ServedBy)
	if putReply.Status != raftrpc.Status_OK {
		fmt.Printf("  leader_hint=%q (not OK - see status meaning in client.proto)\n", putReply.LeaderHint)
		return
	}

	fmt.Printf("GET foo via %s ...\n", *addr)
	getReply, err := client.Get(ctx, &raftrpc.GetRequest{Key: "foo"})
	if err != nil {
		log.Fatalf("Get RPC failed: %v", err)
	}
	fmt.Printf("  status=%s value=%q served_by=%q\n", getReply.Status, getReply.Value, getReply.ServedBy)
	if getReply.Status == raftrpc.Status_OK && getReply.Value == "bar" {
		fmt.Println("OK: wrote and read back through the cluster.")
	} else {
		fmt.Println("MISMATCH: something's wrong - see status/value above.")
	}
}