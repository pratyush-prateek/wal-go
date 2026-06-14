# wal-go

A segmented, crash-safe **write-ahead log** in Go. It stores `[]byte` records
keyed by a monotonically increasing **log sequence number (LSN)** and knows nothing
about what the records mean — so it works for any underlying data store, or anything
else that needs a durable append-only log.

## Features

- **Append-only log** of byte records, addressed by log sequence number (LSN).
- **Durability via `fsync`** — explicit `Sync`, per-write `WriteAndSync`, or auto-flush mode.
- **Segmented files** with automatic rotation at a configurable size.
- **Crash recovery** — on open, the log is replayed and any torn/corrupt tail
  (length + CRC32 checked) is discarded.
- **Suffix truncation** (`TruncateSuffix`) — delete a conflicting tail (this was added to support log repair use case in Raft consensus protocol).
- **Snapshotting + compaction** — when sealed segments accumulate, the WAL calls
  an app-supplied `SnapshotProvider` in the background to materialize state,
  stores the snapshot, and deletes the now-obsolete segments. A manual
  `CompactAndSnapshot` is also available.
- **Protobuf serialization** for records and snapshots.
- **Data-store agnostic** — no terms, votes, or commit logic; just a durable log.

## Install

```bash
go get github.com/pratyush-prateek/wal-go
```

Requires Go 1.26+.

## Usage

### Basic write / read

```go
import wal "github.com/pratyush-prateek/wal-go"

w, err := wal.NewWal(wal.WalConfig{
    DataDirectory: "/var/lib/myapp/wal",
    Mode:          wal.MODE_MANUAL_FLUSH,
})
if err != nil { panic(err) }
defer w.Close()

lsn, _ := w.Write([]byte("hello"))   // buffered append; returns the LSN
_ = w.Sync()                          // make everything durable (one fsync)

data, _ := w.Read(lsn)                // -> "hello"
```

### Durability modes

```go
// fsync each write:
lsn, _ := w.WriteAndSync(record)

// group commit (fast): many Writes, then one Sync
for _, r := range batch { w.Write(r) }
w.Sync()

// auto-flush mode: a background goroutine fsyncs periodically, application can block until the record is durable using WaitUntilSynced
w, _ := wal.NewWal(wal.WalConfig{
    DataDirectory: dir,
    Mode:          wal.MODE_AUTO_FLUSH,
    FlushInterval: 100 * time.Millisecond,
})
w.Write(record)
w.WaitUntilSynced()   // block until the record is durable
```

### Snapshotting + compaction

The app supplies a `SnapshotProvider` — the WAL can't interpret records, so only
the app can collapse them into current state:

```go
provider := func() (snapshot []byte, uptoLSN uint64, err error) {
    return stateMachine.Serialize(), stateMachine.AppliedLSN(), nil
}

w, _ := wal.NewWal(wal.WalConfig{
    DataDirectory:            dir,
    MaxFileSize:              64 << 20, // new segment at 64 MiB
    MaxSegmentsUntilSnapshot: 8,        // auto-compact after 8 finished segments
    SnapshotProvider:         provider,
})
```

### Recovery (snapshot + replay)

```go
blob, upto, _ := w.LatestSnapshot()   // load the latest snapshot
stateMachine.Restore(blob)
for lsn := upto + 1; lsn <= w.LastIndex(); lsn++ {
    rec, _ := w.Read(lsn)
    stateMachine.Apply(lsn, rec)       // replay the tail on top
}
```

## API

Methods:

| Method | Purpose |
|---|---|
| `Write(entry) (lsn, err)` | buffered append |
| `WriteAndSync(entry) (lsn, err)` | append + fsync |
| `Sync()` | flush + fsync |
| `WaitUntilSynced()` | block until durable (auto-flush mode) |
| `Read(lsn) ([]byte, err)` | read an entry |
| `FirstIndex()` / `LastIndex()` | log bounds |
| `TruncateSuffix(fromLSN)` | delete entries `>= fromLSN` |
| `LatestSnapshot() ([]byte, uptoLSN, err)` | latest snapshot, for recovery |
| `CompactAndSnapshot()` | force a compaction now |
| `Close()` | flush, sync, release |

## Benchmarks

Benchmark numbers using 128-byte records:

| Benchmark | ns/op | Notes |
|---|---|---|
| `Write` (buffered) | ~264 | ~485 MB/s (no fsync) |
| `WriteAndSync` (fsync each) | ~4,000,000 | ~245 durable writes/s |
| `GroupCommit` batch=100 | ~41,000 | one fsync per 100 writes |
| `Read` (random) | ~746 | 2 `pread` + unmarshal |


Run benchmarks:

```bash
go test -run '^$' -bench=. -benchmem ./test/
```

## Testing

```bash
go test -race ./test/
```

Tests are black-box (public API only) and live in `./test`.
