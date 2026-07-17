package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// chaosController drives the two independent fault-injection mechanisms
// already built in Phase 4F/4G (see docker-compose.chaos.yml and
// toxiproxy.json's doc comments): a container kill/restart via `docker
// compose`, and toxiproxy's whole-node isolate/heal via its HTTP control
// API. Neither talks to the raft cluster itself - both act entirely from
// outside it, same as a human running these commands by hand.
type chaosController struct {
	toxiproxyAddr string
	composeDir    string
}

// handle serves POST /api/chaos/{action}/{node}, where action is one of
// kill, start, isolate, heal. A GET or an unknown action/node is a 4xx,
// not silently ignored - the dashboard UI always sends real button
// clicks, so a malformed request here means something's out of sync
// between the frontend and this handler, worth surfacing rather than
// swallowing.
func (c *chaosController) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/chaos/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "expected /api/chaos/{kill|start|isolate|heal}/{node}", http.StatusBadRequest)
		return
	}
	action, node := parts[0], parts[1]

	var err error
	switch action {
	case "kill":
		err = c.dockerComposeAction("kill", node)
	case "start":
		err = c.dockerComposeAction("start", node)
	case "isolate":
		err = c.setProxyEnabled(node, false)
	case "heal":
		err = c.setProxyEnabled(node, true)
	default:
		http.Error(w, fmt.Sprintf("unknown action %q (want kill, start, isolate, or heal)", action), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// dockerComposeAction runs `docker compose <verb> <node>` in c.composeDir.
// Using the compose service name rather than a container name/ID sidesteps
// having to guess docker compose's project-prefix naming convention
// (raft-kv-n2-1, raft-kv_n2_1, etc. depending on version/config) - compose
// itself resolves "n2" to whatever container it's actually running, the
// same way `docker compose logs n2` or `docker compose restart n2` would
// from a terminal.
func (c *chaosController) dockerComposeAction(verb, service string) error {
	cmd := exec.Command("docker", "compose", verb, service)
	cmd.Dir = c.composeDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s %s: %w: %s", verb, service, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// setProxyEnabled toggles the toxiproxy proxy fronting one node's raft
// port (see toxiproxy.json: proxy names are "<node>-raft"). This is the
// documented way to take a proxy down without deleting it (Toxiproxy's
// own docs: POST /proxies/{name} with {"enabled": false/true}) - not a
// toxic, since a toxic only degrades traffic rather than cutting it, and
// isolate wants the node to look genuinely unreachable to its peers,
// same as a real network partition. Only meaningful when the cluster is
// running with docker-compose.chaos.yml; against a plain docker-compose.yml
// cluster (no toxiproxy container) this just fails with a connection
// error, which the UI surfaces as-is rather than trying to distinguish
// "no chaos overlay running" from any other toxiproxy failure.
func (c *chaosController) setProxyEnabled(node string, enabled bool) error {
	proxyName := node + "-raft"
	body, _ := json.Marshal(struct {
		Enabled bool `json:"enabled"`
	}{enabled})

	url := fmt.Sprintf("http://%s/proxies/%s", c.toxiproxyAddr, proxyName)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy at %s unreachable (is docker-compose.chaos.yml running?): %w", c.toxiproxyAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("toxiproxy returned %s for proxy %q", resp.Status, proxyName)
	}
	return nil
}
