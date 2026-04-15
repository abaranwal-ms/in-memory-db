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
