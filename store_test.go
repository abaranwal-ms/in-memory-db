package main

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	val, ok, err := s.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != "bar" {
		t.Fatalf("expected bar, got %q (ok=%v)", val, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, ok, err := s.Get("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not found")
	}
}

func TestOverwrite(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Put("foo", "bar")
	s.Put("foo", "baz")

	val, ok, err := s.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != "baz" {
		t.Fatalf("expected baz, got %q", val)
	}
}

func TestDelete(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Put("foo", "bar")
	if err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}

	_, ok, err := s.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestDeleteNonexistent(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Delete("ghost"); err != nil {
		t.Fatal(err)
	}
}

func TestReloadIndex(t *testing.T) {
	dir := t.TempDir()

	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("foo", "bar")
	s.Put("baz", "qux")
	s.Delete("baz")
	s.Close()

	// Reopen and verify the index was rebuilt correctly.
	s2, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	val, ok, _ := s2.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("expected bar after reload, got %q (ok=%v)", val, ok)
	}

	_, ok, _ = s2.Get("baz")
	if ok {
		t.Fatal("expected baz to be deleted after reload")
	}
}

func TestMultipleKeys(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	keys := map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
		"delta": "four",
	}
	for k, v := range keys {
		if err := s.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	for k, want := range keys {
		got, ok, err := s.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || got != want {
			t.Fatalf("key %s: expected %q, got %q", k, want, got)
		}
	}
}
