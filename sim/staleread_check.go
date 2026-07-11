package sim

import "raft-kv/checker"

type StaleRead struct {
	Op        checker.Op
	Partition PartitionEvent
}

func FindStaleReads(history []checker.Op, partitions []PartitionEvent) []StaleRead {
	var found []StaleRead
	for _, op := range history {
		if op.Kind != checker.Read {
			continue
		}
		for _, pe := range partitions {
			if op.Start < pe.Start {
				continue
			}
			if pe.End != -1 && op.Start >= pe.End {
				continue
			}
			for _, m := range pe.Minority {
				if op.ServedBy == m {
					found = append(found, StaleRead{Op: op, Partition: pe})
				}
			}
		}
	}
	return found
}