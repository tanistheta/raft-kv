package kv

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrCASMismatch = errors.New("cas mismatch")
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
	case "CAS":
		if len(parts) != 4 {
			return fmt.Errorf("malformed CAS command: %q", command)
		}
		key, from, to := parts[1], parts[2], parts[3]
		current, ok := s.data[key]
		if !ok {
			return fmt.Errorf("cas %q: %w", key, ErrKeyNotFound)
		}
		if current != from {
			return fmt.Errorf("cas %q: %w", key, ErrCASMismatch)
		}
		s.data[key] = to
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