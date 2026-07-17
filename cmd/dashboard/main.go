// Command dashboard is a standalone observability + demo tool for a
// running raft-kv cluster - it is not part of the cluster itself and
// doesn't touch raft.Node. It talks to every node purely as an external
// gRPC client (the same relationship cmd/smoketest has), plus shells out
// to `docker compose` and toxiproxy's HTTP control API for the chaos
// buttons. Killing this process does not affect the cluster in any way.
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"time"
)

//go:embed static
var staticFS embed.FS

// pollInterval governs both how often the dashboard refreshes cluster
// state and how stale /api/state is allowed to be when a browser polls
// it. Short enough that a leader election or a killed node shows up
// within roughly one UI refresh cycle, long enough not to spam three
// Status RPCs per second for no reason.
const pollInterval = 500 * time.Millisecond

// statusTimeout bounds a single node's Status call. Deliberately short -
// a node that doesn't answer within this window is shown as unreachable
// rather than making the whole /api/state response wait on it.
const statusTimeout = 400 * time.Millisecond

// nodeList collects repeated -node flags, same flag.Value pattern as
// cmd/node's peerList.
type nodeList []string

func (n *nodeList) String() string { return strings.Join(*n, ",") }
func (n *nodeList) Set(v string) error {
	*n = append(*n, v)
	return nil
}

func main() {
	var nodes nodeList
	flag.Var(&nodes, "node", "id=clientAddr for one cluster member; repeat once per node (e.g. -node=n1=127.0.0.1:9101)")
	listenAddr := flag.String("listen", ":8080", "address the dashboard's own HTTP server listens on")
	toxiproxyAddr := flag.String("toxiproxy-addr", "127.0.0.1:8474", "toxiproxy control API address (only used by Isolate/Heal; requires the cluster to be running with docker-compose.chaos.yml)")
	composeDir := flag.String("compose-dir", ".", "directory to run `docker compose` in - must match the directory you ran `docker compose up` from, so it resolves to the same project")
	flag.Parse()

	if len(nodes) == 0 {
		log.Fatal("at least one -node is required, e.g. -node=n1=127.0.0.1:9101 -node=n2=127.0.0.1:9102 -node=n3=127.0.0.1:9103")
	}

	specs := make(map[string]string) // id -> clientAddr
	var order []string
	for _, n := range nodes {
		parts := strings.SplitN(n, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("-node %q: expected id=clientAddr", n)
		}
		specs[parts[0]] = parts[1]
		order = append(order, parts[0])
	}

	cluster, err := newClusterView(specs, order)
	if err != nil {
		log.Fatalf("connecting to cluster: %v", err)
	}
	defer cluster.close()

	go cluster.pollLoop()

	chaos := &chaosController{toxiproxyAddr: *toxiproxyAddr, composeDir: *composeDir}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", cluster.handleState)
	mux.HandleFunc("/api/kv", cluster.handleKV)
	mux.HandleFunc("/api/chaos/", chaos.handle)

	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("embedding static assets: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticContent)))

	log.Printf("dashboard listening on %s, watching nodes %v", *listenAddr, order)
	log.Fatal(http.ListenAndServe(*listenAddr, mux))
}
