# in-memory-db

An append-only, log-structured key-value store written in Go.

## How it works

- Keys and values are stored in append-only `.dat` files on disk.
- Each record is written as: `CRC32 (4B) | keyLen (4B) | valLen (4B) | key | value`.
- An in-memory hash map holds each key's file name, offset, and record length for fast lookups.
- Deletes write a tombstone (empty value) and remove the key from the map.
- When a file exceeds 200 MB, a new file is created automatically.

## Compaction

A background goroutine runs compaction every 15 seconds using a **small stop-the-world** strategy:

1. **Snapshot (read-lock):** Capture which keys live in inactive files.
2. **Rewrite (no lock):** Copy live records into a new compacted file. Reads and writes continue unblocked.
3. **Swap (write-lock):** Atomically update the index, skipping any keys modified during compaction.

## API

```
PUT    /kv/{key}   — upsert (body = value)
GET    /kv/{key}   — read
DELETE /kv/{key}   — delete
POST   /compact    — trigger compaction manually
```

## Run

```bash
go build -o kv-inmem .
./kv-inmem           # listens on :8080, data stored in ./data
./kv-inmem /tmp/db   # custom data directory
```

## Seed / Load Test

```bash
go run ./cmd/seed                        # 3 GB, 16 workers, 500K keys
go run ./cmd/seed -bytes 1073741824      # 1 GB
```

## Test

```bash
go test -v ./...
```
