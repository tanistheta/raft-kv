package kv

import (
	"fmt"
	"strings"
	"sync"
)

type StateMachine struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{data: make(map[string]string)}
}

func (s *StateMachine) Apply(command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parts := strings.Fields(string(command))
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) != 2 {
			return fmt.Errorf("malformed SET command: %q", command)
		}
		kv := strings.SplitN(parts[1], "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("malformed SET command: %q", command)
		}
		s.data[kv[0]] = kv[1]
	case "DELETE":
		if len(parts) != 2 {
			return fmt.Errorf("malformed DELETE command: %q", command)
		}
		delete(s.data, parts[1])
	default:
		return fmt.Errorf("unknown command: %q", command)
	}
	return nil
}

func (s *StateMachine) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	return val, ok
}
