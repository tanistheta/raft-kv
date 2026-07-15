package kv

import (
	"errors"
	"testing"
)

func TestSetThenGet(t *testing.T) {
	s := NewStateMachine()
	if err := s.Apply([]byte("SET x=1")); err != nil {
		t.Fatalf("Apply(SET) returned error: %v", err)
	}
	val, ok := s.Get("x")
	if !ok || val != "1" {
		t.Errorf("Get(x) = %q, %v, want \"1\", true", val, ok)
	}
}

func TestCASSucceedsWhenValueMatches(t *testing.T) {
	s := NewStateMachine()
	s.Apply([]byte("SET x=1"))

	if err := s.Apply([]byte("CAS x 1 2")); err != nil {
		t.Fatalf("Apply(CAS) returned error: %v", err)
	}
	val, _ := s.Get("x")
	if val != "2" {
		t.Errorf("Get(x) = %q, want \"2\"", val)
	}
}

func TestCASFailsOnMismatch(t *testing.T) {
	s := NewStateMachine()
	s.Apply([]byte("SET x=1"))

	err := s.Apply([]byte("CAS x 99 2"))
	if !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("Apply(CAS mismatch) error = %v, want ErrCASMismatch", err)
	}
	val, _ := s.Get("x")
	if val != "1" {
		t.Errorf("Get(x) = %q, want unchanged \"1\" after failed CAS", val)
	}
}

func TestCASFailsOnMissingKey(t *testing.T) {
	s := NewStateMachine()

	err := s.Apply([]byte("CAS missing 1 2"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Apply(CAS on missing key) error = %v, want ErrKeyNotFound", err)
	}
}

func TestCASMalformedCommand(t *testing.T) {
	s := NewStateMachine()
	s.Apply([]byte("SET x=1"))

	err := s.Apply([]byte("CAS x 1"))
	if err == nil {
		t.Fatal("Apply(malformed CAS) returned nil error, want one")
	}
	if errors.Is(err, ErrCASMismatch) || errors.Is(err, ErrKeyNotFound) {
		t.Errorf("malformed command should not be a CAS-mismatch/not-found sentinel: %v", err)
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	s := NewStateMachine()
	s.Apply([]byte("SET x=1"))
	if err := s.Apply([]byte("DELETE x")); err != nil {
		t.Fatalf("Apply(DELETE) returned error: %v", err)
	}
	if _, ok := s.Get("x"); ok {
		t.Errorf("Get(x) found a value after DELETE")
	}
}