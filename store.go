package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxFileSize int64 = 200 * 1024 * 1024 // 200 MB

// IndexEntry is the in-memory pointer to a record on disk.
type IndexEntry struct {
	FileName     string
	Offset       int64
	RecordLength int32
}

// Store is an append-only key-value store backed by log-structured files.
type Store struct {
	mu             sync.RWMutex
	index          map[string]IndexEntry
	activeFile     *os.File
	activeFileName string
	activeOffset   int32
	dataDir        string
}

// NewStore opens (or creates) a store rooted at dataDir.
// It rebuilds the in-memory index by replaying all data files.
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	s := &Store{
		index:   make(map[string]IndexEntry),
		dataDir: dataDir,
	}

	if err := s.loadIndex(); err != nil {
		return nil, err
	}

	if s.activeFile == nil {
		if err := s.rotateFile(); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// loadIndex replays every .dat file in chronological order to rebuild the index,
// then opens the newest file as the active file for appending.
func (s *Store) loadIndex() error {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".dat") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, fname := range files {
		if err := s.replayFile(fname); err != nil {
			return fmt.Errorf("replaying %s: %w", fname, err)
		}
	}

	if len(files) > 0 {
		last := files[len(files)-1]
		f, err := os.OpenFile(filepath.Join(s.dataDir, last), os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		s.activeFile = f
		s.activeFileName = last
		s.activeOffset = int32(info.Size())
	}

	return nil
}

// replayFile reads every record in a single data file, updating the index.
func (s *Store) replayFile(fname string) error {
	f, err := os.Open(filepath.Join(s.dataDir, fname))
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	for {
		key, value, recLen, err := decodeRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(value) == 0 {
			delete(s.index, key)
		} else {
			s.index[key] = IndexEntry{
				FileName:     fname,
				Offset:       offset,
				RecordLength: recLen,
			}
		}
		offset += int64(recLen)
	}

	return nil
}

// rotateFile closes the current active file and creates a new one.
func (s *Store) rotateFile() error {
	if s.activeFile != nil {
		s.activeFile.Close()
	}

	name := fmt.Sprintf("%d.dat", time.Now().UnixMilli())
	f, err := os.OpenFile(filepath.Join(s.dataDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	s.activeFile = f
	s.activeFileName = name
	s.activeOffset = 0
	return nil
}

// Put inserts or updates a key with the given value.
func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int64(s.activeOffset) >= maxFileSize {
		if err := s.rotateFile(); err != nil {
			return err
		}
	}

	record := encodeRecord(key, value)
	offset := s.activeOffset

	if _, err := s.activeFile.Write(record); err != nil {
		return err
	}

	s.index[key] = IndexEntry{
		FileName:     s.activeFileName,
		Offset:       int64(offset),
		RecordLength: int32(len(record)),
	}
	s.activeOffset += int32(len(record))

	return nil
}

// Get retrieves the value for a key. Returns ("", false, nil) if not found.
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	entry, ok := s.index[key]
	s.mu.RUnlock()

	if !ok {
		return "", false, nil
	}

	f, err := os.Open(filepath.Join(s.dataDir, entry.FileName))
	if err != nil {
		return "", false, err
	}
	defer f.Close()

	if _, err := f.Seek(entry.Offset, io.SeekStart); err != nil {
		return "", false, err
	}

	_, value, _, err := decodeRecord(f)
	if err != nil {
		return "", false, err
	}

	return value, true, nil
}

// Delete writes a tombstone record (empty value) and removes the key from the index.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.index[key]; !ok {
		return nil
	}

	if int64(s.activeOffset) >= maxFileSize {
		if err := s.rotateFile(); err != nil {
			return err
		}
	}

	record := encodeRecord(key, "")
	if _, err := s.activeFile.Write(record); err != nil {
		return err
	}
	s.activeOffset += int32(len(record))

	delete(s.index, key)
	return nil
}

// Compact merges all inactive (non-active) data files into new compacted files,
// keeping only the live records that the in-memory index still points to.
// Old inactive files are deleted after compaction.
//
// Strategy: "Small Stop-the-World"
//
// Instead of holding a write-lock for the entire compaction (which would block
// all reads and writes), we split the work into three phases with two very
// brief lock windows and one lock-free I/O phase:
//
//	Phase 1 — Snapshot (RLock):
//	  Take a read-lock just long enough to capture which files are inactive
//	  and which index entries point into them. This is a fast in-memory scan.
//	  Reads and writes are NOT blocked during this phase (RLock allows concurrent reads).
//
//	Phase 2 — Rewrite (no lock):
//	  Read every live record from the old immutable files and write them into
//	  a new compacted file. No lock is held, so Put/Get/Delete proceed freely
//	  against the active file. Because inactive files are append-only and
//	  never modified, reading them without a lock is safe.
//
//	Phase 3 — Swap (Lock):
//	  Take a brief write-lock to atomically swap each index entry from the
//	  old file location to the new compacted file location. A key is only
//	  updated if it still points to the same file+offset captured in the
//	  snapshot — if a concurrent Put overwrote it or a Delete removed it,
//	  the stale compacted copy is simply ignored. Old files are then deleted.
//
// The two "stop-the-world" windows (Phase 1 and 3) are proportional to the
// number of live keys, not the size of the data on disk, keeping pause times
// small regardless of how much data is being compacted.
func (s *Store) Compact() error {
	// Phase 1 (Snapshot): brief read-lock to capture inactive files and their live keys.
	s.mu.RLock()
	activeFile := s.activeFileName

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		s.mu.RUnlock()
		return err
	}

	var inactiveFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".dat") && e.Name() != activeFile {
			inactiveFiles = append(inactiveFiles, e.Name())
		}
	}

	if len(inactiveFiles) == 0 {
		s.mu.RUnlock()
		return nil
	}

	inactiveSet := make(map[string]struct{}, len(inactiveFiles))
	for _, f := range inactiveFiles {
		inactiveSet[f] = struct{}{}
	}

	// Snapshot: key -> IndexEntry for keys living in inactive files.
	snapshot := make(map[string]IndexEntry)
	for key, entry := range s.index {
		if _, ok := inactiveSet[entry.FileName]; ok {
			snapshot[key] = entry
		}
	}
	s.mu.RUnlock()

	if len(snapshot) == 0 {
		// No live keys in inactive files — just delete the stale files.
		for _, fname := range inactiveFiles {
			os.Remove(filepath.Join(s.dataDir, fname))
		}
		return nil
	}

	// Phase 2 (Rewrite): heavy I/O with no lock held.
	// Read live records from immutable old files, write them into a new compacted file.
	var compactFile *os.File
	var compactName string
	var compactOffset int32

	closeCompactFile := func() {
		if compactFile != nil {
			compactFile.Close()
			compactFile = nil
		}
	}

	newCompactFile := func() error {
		closeCompactFile()
		name := fmt.Sprintf("c_%d.dat", time.Now().UnixMilli())
		f, err := os.OpenFile(filepath.Join(s.dataDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		compactFile = f
		compactName = name
		compactOffset = 0
		return nil
	}

	if err := newCompactFile(); err != nil {
		return err
	}
	defer closeCompactFile()

	// newEntries collects the index updates to apply atomically later.
	type compactedEntry struct {
		key   string
		entry IndexEntry
	}
	var newEntries []compactedEntry

	for key, snapEntry := range snapshot {
		// Read the record from the old file (no lock needed — file is immutable).
		f, err := os.Open(filepath.Join(s.dataDir, snapEntry.FileName))
		if err != nil {
			return err
		}
		if _, err := f.Seek(snapEntry.Offset, io.SeekStart); err != nil {
			f.Close()
			return err
		}
		_, value, _, err := decodeRecord(f)
		f.Close()
		if err != nil {
			return err
		}

		record := encodeRecord(key, value)

		if int64(compactOffset)+int64(len(record)) > maxFileSize {
			if err := newCompactFile(); err != nil {
				return err
			}
		}

		if _, err := compactFile.Write(record); err != nil {
			return err
		}

		newEntries = append(newEntries, compactedEntry{
			key: key,
			entry: IndexEntry{
				FileName:     compactName,
				Offset:       int64(compactOffset),
				RecordLength: int32(len(record)),
			},
		})
		compactOffset += int32(len(record))
	}

	closeCompactFile()

	// Phase 3 (Swap): brief write-lock to atomically update index entries.
	// Only entries that haven't been modified by concurrent Put/Delete are swapped.
	s.mu.Lock()
	for _, ce := range newEntries {
		current, exists := s.index[ce.key]
		if !exists {
			// Key was deleted during compaction — skip.
			continue
		}
		// Only update if the key still points to the same old file+offset
		// (i.e. it wasn't overwritten by a concurrent Put).
		if current.FileName == snapshot[ce.key].FileName && current.Offset == snapshot[ce.key].Offset {
			s.index[ce.key] = ce.entry
		}
	}
	s.mu.Unlock()

	for _, fname := range inactiveFiles {
		os.Remove(filepath.Join(s.dataDir, fname))
	}

	return nil
}

// Close flushes and closes the active file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeFile != nil {
		err := s.activeFile.Close()
		s.activeFile = nil
		return err
	}
	return nil
}

// --------------- Record codec ---------------
//
// On-disk format per record:
//   CRC32 (4 bytes) | keyLen (4 bytes) | valLen (4 bytes) | key | value
//
// CRC32 covers keyLen + valLen + key + value.
// A record with valLen == 0 is a tombstone (delete marker).

func encodeRecord(key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))

	// payload = keyLen(4) + valLen(4) + key + value
	payload := make([]byte, 8+len(keyBytes)+len(valBytes))
	binary.LittleEndian.PutUint32(payload[0:4], keyLen)
	binary.LittleEndian.PutUint32(payload[4:8], valLen)
	copy(payload[8:], keyBytes)
	copy(payload[8+keyLen:], valBytes)

	crc := crc32.ChecksumIEEE(payload)

	record := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(record[0:4], crc)
	copy(record[4:], payload)

	return record
}

func decodeRecord(r io.Reader) (key, value string, recordLen int32, err error) {
	header := make([]byte, 12)
	if _, err = io.ReadFull(r, header); err != nil {
		if err == io.ErrUnexpectedEOF {
			err = fmt.Errorf("corrupt record: incomplete header")
		}
		return
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint32(header[4:8])
	valLen := binary.LittleEndian.Uint32(header[8:12])

	data := make([]byte, keyLen+valLen)
	if len(data) > 0 {
		if _, err = io.ReadFull(r, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				err = fmt.Errorf("corrupt record: incomplete body")
			}
			return
		}
	}

	// Verify CRC over the payload (keyLen + valLen + key + value)
	payload := make([]byte, 8+keyLen+valLen)
	copy(payload[0:8], header[4:12])
	copy(payload[8:], data)

	if crc32.ChecksumIEEE(payload) != storedCRC {
		err = fmt.Errorf("crc mismatch")
		return
	}

	key = string(data[:keyLen])
	value = string(data[keyLen:])
	recordLen = int32(12 + keyLen + valLen)
	return
}
