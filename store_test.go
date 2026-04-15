package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestCompactRemovesStaleEntries(t *testing.T) {
	dir := t.TempDir()

	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data, then overwrite and delete to create stale entries.
	s.Put("a", "1")
	s.Put("b", "2")
	s.Put("c", "3")
	s.Put("a", "updated") // overwrite
	s.Delete("c")         // tombstone

	// Force a file rotation so the first file becomes inactive.
	s.mu.Lock()
	if err := s.rotateFile(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.mu.Unlock()

	// Write more data into the new active file.
	s.Put("d", "4")

	// Count files before compaction.
	filesBefore := datFileCount(t, dir)
	if filesBefore < 2 {
		t.Fatalf("expected at least 2 data files, got %d", filesBefore)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// Verify all live keys are still readable.
	assertGet(t, s, "a", "updated", true)
	assertGet(t, s, "b", "2", true)
	assertGet(t, s, "d", "4", true)

	// Deleted key must stay deleted.
	assertGet(t, s, "c", "", false)

	s.Close()

	// Verify the store can reload after compaction.
	s2, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	assertGet(t, s2, "a", "updated", true)
	assertGet(t, s2, "b", "2", true)
	assertGet(t, s2, "d", "4", true)
	assertGet(t, s2, "c", "", false)
}

func TestCompactNoInactiveFiles(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Put("x", "y")

	// Compact with only one (active) file should be a no-op.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	assertGet(t, s, "x", "y", true)
}

func TestCompactDeletesOldFiles(t *testing.T) {
	dir := t.TempDir()

	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	s.Put("a", "1")

	// Force rotate to make the first file inactive.
	s.mu.Lock()
	oldFile := s.activeFileName
	s.rotateFile()
	s.mu.Unlock()

	s.Put("b", "2")

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// The original inactive file should be gone.
	if _, err := os.Stat(filepath.Join(dir, oldFile)); !os.IsNotExist(err) {
		t.Fatalf("expected old file %s to be deleted", oldFile)
	}

	assertGet(t, s, "a", "1", true)
	assertGet(t, s, "b", "2", true)
	s.Close()
}

func assertGet(t *testing.T, s *Store, key, wantVal string, wantOK bool) {
	t.Helper()
	got, ok, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if ok != wantOK {
		t.Fatalf("Get(%q): ok=%v, want %v", key, ok, wantOK)
	}
	if ok && got != wantVal {
		t.Fatalf("Get(%q) = %q, want %q", key, got, wantVal)
	}
}

func datFileCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".dat") {
			count++
		}
	}
	return count
}

// TestLoadGeneratorWithCompaction runs concurrent writers, readers, deleters,
// and a compaction loop simultaneously, then verifies every key returns the
// last-written value (or is correctly deleted).
func TestLoadGeneratorWithCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const (
		numWorkers   = 8
		numKeys      = 1000
		opsPerWorker = 5000
		valSize      = 200
		deleteRatio  = 0.05
	)

	// truth tracks the last value written per key (empty string = deleted).
	// Protected by its own mutex, independent of the store lock.
	var truthMu sync.Mutex
	truth := make(map[string]string)

	val := strings.Repeat("v", valSize)
	var totalOps atomic.Int64

	// Compaction loop: runs every 50ms until stopped.
	compactDone := make(chan struct{})
	var compactWg sync.WaitGroup
	compactWg.Add(1)
	go func() {
		defer compactWg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Force a rotation so compaction has inactive files to work on.
				s.mu.Lock()
				s.rotateFile()
				s.mu.Unlock()
				if err := s.Compact(); err != nil {
					t.Errorf("compaction error: %v", err)
				}
			case <-compactDone:
				return
			}
		}
	}()

	// Writers + deleters.
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID) + time.Now().UnixNano()))
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", rng.Intn(numKeys))

				if rng.Float64() < deleteRatio {
					if err := s.Delete(key); err != nil {
						t.Errorf("Delete(%s): %v", key, err)
						return
					}
					truthMu.Lock()
					truth[key] = ""
					truthMu.Unlock()
				} else {
					v := fmt.Sprintf("%s-w%d-i%d", val, workerID, i)
					if err := s.Put(key, v); err != nil {
						t.Errorf("Put(%s): %v", key, err)
						return
					}
					truthMu.Lock()
					truth[key] = v
					truthMu.Unlock()
				}
				totalOps.Add(1)
			}
		}(w)
	}

	// Concurrent readers (best-effort, just must not error).
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(readerID+100) + time.Now().UnixNano()))
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", rng.Intn(numKeys))
				_, _, err := s.Get(key)
				if err != nil {
					t.Errorf("Get(%s): %v", key, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(compactDone)
	compactWg.Wait()

	t.Logf("load test: %d ops across %d workers, %d unique keys",
		totalOps.Load(), numWorkers, numKeys)

	// Final verification: every key in truth must match the store.
	for key, expected := range truth {
		got, ok, err := s.Get(key)
		if err != nil {
			t.Fatalf("final Get(%s): %v", key, err)
		}
		if expected == "" {
			if ok {
				t.Fatalf("key %s should be deleted, got %q", key, got)
			}
		} else {
			if !ok {
				t.Fatalf("key %s not found, expected %q", key, expected)
			}
			if got != expected {
				t.Fatalf("key %s: got %q, want %q", key, got, expected)
			}
		}
	}

	// Reload and verify again.
	s.Close()
	s2, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for key, expected := range truth {
		got, ok, _ := s2.Get(key)
		if expected == "" {
			if ok {
				t.Fatalf("after reload: key %s should be deleted, got %q", key, got)
			}
		} else {
			if !ok || got != expected {
				t.Fatalf("after reload: key %s: got %q (ok=%v), want %q", key, got, ok, expected)
			}
		}
	}

	t.Logf("verified %d keys after reload", len(truth))
}
