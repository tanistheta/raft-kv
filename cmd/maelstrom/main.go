package main

import (
	"fmt"
	"os"

	"raft-kv/maelstrom"
)

func main() {
	if err := maelstrom.RunProcess(); err != nil {
		fmt.Fprintf(os.Stderr, "raft-kv maelstrom node exited: %v\n", err)
		os.Exit(1)
	}
}