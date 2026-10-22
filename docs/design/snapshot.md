# Design: Periodic Snapshot Persistence

| | |
|---|---|
| **Status** | Implemented |
| **Author** | manhhung2111 |
| **Created** | 2026-10-22 |

## 1. Summary

Add point-in-time snapshots of the whole keyspace, written to disk on a fixed interval and loaded on startup. The feature is **enabled by default**. Snapshots are produced by serializing the store into an in-memory buffer on the event-loop goroutine and handing that buffer to a background goroutine that writes it to disk atomically ("Option B").

## 2. Goals / Non-goals

**Goals**
- Survive restarts and crashes, losing at most one snapshot interval of writes.
- Never leave a partially written or corrupted dump in place of a good one.
- Support every existing object type and encoding, round-tripping exactly (including probabilistic structures).
- Zero changes to command handlers or data-type mutation paths.
- On by default, with config/flags to disable or tune.

**Non-goals**
- Append-only file (AOF) / per-write durability.
- Compatibility with the Redis RDB format.
- Change-count based save rules (`save 60 1000`); a fixed interval is used for now (see §11).
- Replication.

## 3. Background: constraints in the current code

- **Single-threaded data access.** `server.runEventLoop` executes every command on one goroutine; `store` / `dict` have no locking. Any read of the store must happen on that goroutine.
- **One timer.** `EventLoop.RegisterTimer` supports a single timer (`timerFd` on epoll, fixed ident on kqueue). `handleEvent` maps every timer event to `ActiveExpireCycle()`.
- **Absolute expiries.** `store.expires` stores Unix epoch milliseconds (`uint64`), so they can be persisted verbatim.
- **Private value types.** Values are unexported structs behind interfaces in `internal/storage/types`; serialization must live in that package.
- **No `fork()`.** Go's runtime makes `fork()`-based copy-on-write (how Redis does `BGSAVE`) unsafe, so consistency comes from serializing on the loop goroutine.

## 4. Approach overview

```
event loop goroutine                       background goroutine
────────────────────                       ────────────────────
serverCron() (every 100ms tick)
  ├─ ActiveExpireCycle()
  ├─ poll saveDone channel → update state
  └─ if due && !saving:
        buf := store.Snapshot()   ──buf──▶  write dump.grdb.tmp
        saving = true                       fsync file
                                            rename → dump.grdb
                                            fsync dir
            ◀──── saveResult{err, size, dur} ──┘
```

Consistency: because `Snapshot()` runs to completion on the loop goroutine, no command interleaves with it — the buffer is an exact point-in-time image. After the buffer is handed off, the background goroutine never touches the store.

## 5. Configuration

Added to `config.Config` and `NewConfig()`:

| Field | Default | Flag | Meaning |
|---|---|---|---|
| `SnapshotEnabled` | `true` | `-snapshot` | Enable periodic snapshots |
| `SnapshotIntervalSec` | `300` | `-snapshot-interval` | Seconds between automatic snapshots |
| `SnapshotDir` | `"."` | `-snapshot-dir` | Directory holding the dump file |
| `SnapshotFilename` | `"dump.grdb"` | `-snapshot-file` | Dump file name |
| `SnapshotLoadOnStart` | `true` | `-snapshot-load` | Load the dump at startup if present |
| `SnapshotSaveOnShutdown` | `true` | — | Foreground save on SIGINT/SIGTERM |

Validation at startup: `SnapshotIntervalSec >= 1`; `SnapshotDir` must exist and be writable when enabled (fail fast otherwise).

`SnapshotLoadOnStart` is independent of `SnapshotEnabled`, so a read-only restore without further saves is possible.

## 6. File format (`.grdb`, version 1)

All integers little-endian. `uvarint` = `encoding/binary` unsigned varint. `bytes` = `uvarint len` + raw bytes.

```
File   := Header Entry* EOF Checksum
Header := "GREDIS" (6 bytes) | version uint16 (=1) | createdAtMs uint64
Entry  := opcode=0x01 | objType uint8 | encoding uint8 | expireAtMs uint64 (0 = none)
          | key bytes | valueLen uint64 | value (valueLen bytes)
EOF    := opcode=0xFF
Checksum := CRC-64/ECMA uint64 over every byte before it
```

- `objType` / `encoding` reuse the existing `ObjectType` / `ObjectEncoding` values. Those enums are now part of the on-disk format: **new values may only be appended, never reordered.** A comment saying so is added to `object.go`.
- The value payload is length-prefixed so a loader can report an unknown type precisely. The length is a fixed-width `uint64` so the encoder can reserve it and patch it after writing the value in place, avoiding a copy per key.
- Opcodes `0x02–0xFE` are reserved for future metadata (e.g. aux fields, key counts). The v1 loader rejects unknown opcodes and files whose `version` differs from the one it supports.
- LRU/LFU state is not persisted; loaded objects get a fresh `lru` exactly as newly created objects do.

## 7. Value encoding per type

Each concrete type in `internal/storage/types` implements `Encodable` (`Encode(e *codec.Encoder)`, appending to the shared snapshot buffer) and has a package-level `Decode<Type>(d *codec.Decoder) (<Interface>, error)` (`internal/storage/types/serialize.go`). The primitives live in `internal/codec`. Payload layouts:

| ObjType / Encoding | Payload |
|---|---|
| String / `EncRaw` | raw string bytes |
| String / `EncInt` | varint int64 |
| Set / `EncIntSet` | `uvarint n` + n × int64 (sorted, as stored) |
| Set / `EncHashTable` | `uvarint n` + n × `bytes` |
| List / `EncQuickList` | `uvarint n` + n × `bytes` (head→tail). Rebuilt via push so `ListMaxListpackSize` node splitting applies |
| Hash / `EncHashTable` | `uvarint n` + n × (`bytes` field, `bytes` value) |
| ZSet / `EncSortedSet` (incl. Geo) | `uvarint n` + n × (`bytes` member, float64 score) in rank order; skiplist rebuilt on load |
| BloomFilter | `initialCapacity, initialErrRate, expansionRate, tighteningRatio, totalItems`, `uvarint nFilters`, per sub-filter: `k, numBits, capacity, insertedItems, errorRate, uvarint nWords, words[]uint64` |
| CuckooFilter | `initialCapacity, initialBucketSize, maxIterations, expansionRate, totalItems, totalDeletes`, `uvarint nFilters`, per sub-filter: `bucketSize, numBuckets, fingerprintSize, maxKicks, insertedItems, deletedItems`, then `numBuckets × bucketSize` uint16 fingerprints (fixed slots, 0 = empty) |
| HyperLogLog | `uvarint len` + registers `[]uint8`; `cachedCount` dropped, `dirty=true` on load |
| CountMinSketch | `width, depth, totalCount`, then `depth × width` uint64 counters row-major |

Requirement: probabilistic structures must round-trip **bit-exact** so `BF.EXISTS`, `CF.COUNT`, `PFCOUNT`, `CMS.QUERY` answer identically before and after restart.

## 8. Components and code changes

| Area | Change |
|---|---|
| `internal/config/config.go` | New fields + defaults (§5) |
| `cmd/main.go` | New flags |
| `internal/codec` (new) | Binary `Encoder` / `Decoder` primitives with sticky errors and allocation guards |
| `internal/storage/types/serialize.go` (new) | `Encode` / `Decode*` per type (§7) |
| `internal/storage/snapshot.go` (new) | `Snapshot() ([]byte, error)`, `LoadSnapshot([]byte) error`, `KeyCount()`, format constants, CRC |
| `internal/storage/iobject.go` | New `PersistenceStore` interface embedded in `Store` |
| `internal/persistence/` (new) | `Manager`: owns file path, load, atomic write, in-progress flag, result channel, `lastSave`/`lastSaveErr` |
| `internal/server/server.go` | Timer handler → `serverCron()`; load before listening; final save on shutdown |
| `internal/command/persistence.go` (new) | `SAVE`, `BGSAVE`, `LASTSAVE` |
| `internal/wiring` | Provide `Manager`; update `wire_gen.go` |
| `Dockerfile` / `README.md` | `VOLUME /data`, `-snapshot-dir /data`, document flags |
| `.gitignore` | `*.grdb`, `*.grdb.tmp` |

### 8.1 `Snapshot` / `LoadSnapshot`
- `Snapshot` iterates `data`; for each key, looks up `expires`, **skips keys whose deadline has passed**, writes an entry. Iteration uses the existing `keys` slice of `dict`, requiring a small `ForEach(fn func(K, V))` on `Dict`.
- `LoadSnapshot` requires an empty store. The file is read fully and magic and CRC are verified before any entry is decoded, then the version is checked. Entries are decoded into fresh dicts that replace the store's only on success, so a failed load leaves the store empty. Keys already expired at load time are dropped. Insertion goes through `dict.Set` so `usedMemory` stays accurate.

### 8.2 Scheduling (`serverCron`)
Reusing the single existing timer avoids touching the epoll/kqueue implementations:

```go
func (s *Server) serverCron() {
    s.redis.ActiveExpireCycle()
    // Collects a finished background write (non-blocking), then calls
    // BackgroundSave() if enabled && interval elapsed && !inProgress
    s.persistence.Cron(time.Now())
}
```

The interval is measured from the **end of the last successful or failed attempt**, so a slow disk can't cause back-to-back saves.

### 8.3 Atomic write
1. Create `<dir>/<file>.tmp` (`O_CREATE|O_TRUNC|O_WRONLY`, 0644).
2. Write buffer, `f.Sync()`, `f.Close()`.
3. `os.Rename(tmp, final)`.
4. Open `<dir>`, `Sync()` it (so the rename is durable), close.
Any error removes the tmp file and is reported on the result channel. The previous `dump.grdb` is untouched on failure.

### 8.4 Commands
- `SAVE`: foreground snapshot + write on the loop goroutine. Returns `ERR Background save already in progress` if one is running.
- `BGSAVE`: same path as the cron trigger; returns `Background saving started`.
- `LASTSAVE`: Unix seconds of the last successful save.

### 8.5 Startup and shutdown
- Startup (`Server.Start`, before `createServerSocket`): if `SnapshotLoadOnStart` and the file exists → load. Missing file → log and continue. Corrupt / bad CRC / unsupported version → **refuse to start** with a clear error (operator must move the file aside). Load time and key count are logged.
- Shutdown: `WaitingForSignals` used to close the epoll/kqueue fd from another goroutine, which did not wake the blocked `Wait` (the server never exited on SIGTERM). It now only sets an atomic `stopping` flag; the event loop checks it after every `Wait`, and the 100ms timer guarantees `Wait` returns. Once the loop has stopped, `Start` waits for any in-flight background write, performs a final foreground save if enabled, and returns.

## 9. Failure handling

| Situation | Behaviour |
|---|---|
| Background write fails (disk full, permissions) | Log error, keep old dump, `lastSaveErr` set, retry at next interval. Server keeps serving |
| Save requested while one in progress | Cron: skip. `SAVE`/`BGSAVE`: error reply |
| Crash during write | Only the `.tmp` is affected; stale `.tmp` is deleted on next startup |
| Corrupt dump at startup | Refuse to start |
| Unknown objType/encoding in dump | Refuse to start (written by a newer build) |

## 10. Performance and memory

- **Loop pause** ≈ serialization time, linear in dataset size. Logged per save (`snapshot: 1.2M keys, 340MB, encode 410ms, write 1.1s`). This is the main cost of Option B.
- **Memory**: one extra buffer roughly equal to the encoded dataset size while the write is in flight. It is *not* counted in `usedMemory` (which tracks the keyspace only); documented as headroom operators must leave below `MaxmemoryLimit`.
- Serialization uses a pre-sized `bytes.Buffer` and `binary.AppendUvarint`-style appends to minimize allocations.
- Future: if pauses become a problem, move to storage-level copy-on-write so the walk happens off-loop (§11).

## 11. Alternatives considered

- **Option A — fully synchronous save in the loop.** Simplest, but blocks on disk I/O and fsync, which dominate on large datasets. Kept only as the `SAVE` command.
- **Option C — copy-on-write at the storage layer.** Near-zero pause, but every mutation path must check a snapshot epoch and clone before writing. Large blast radius; deferred.
- **`fork()`** — unsafe with the Go runtime.
- **`encoding/gob`** — can't see unexported fields, slower, and couples the format to Go struct layout.
- **Change-count save rules** (`save <sec> <changes>`) — requires a dirty counter in every write path. Can be layered onto `Manager.Cron()` later without format changes.

## 12. Testing

- **Unit (types):** round-trip every type and encoding, including empty, large, intset→hashtable converted sets, multi-node quicklists, scaled bloom/cuckoo filters (multiple sub-filters), CF with deletes. Assert query results are identical after round-trip.
- **Unit (store):** populate all types + TTLs → `Snapshot` to `bytes.Buffer` → `LoadSnapshot` into a fresh store → compare keys, values, TTLs, `usedMemory`. Expired keys are dropped on both save and load.
- **Corruption:** truncated file, flipped byte (CRC fail), bad magic, future version → all rejected.
- **Atomic write:** write into a read-only dir → error, previous dump intact, no `.tmp` left.
- **Integration (`test/`):** start server with temp dir and 1s interval, write data, wait for save, restart, verify data. `BGSAVE` + `LASTSAVE`.
- `go test -race ./...` must pass (verifies background goroutine never touches the store).
- Existing tests construct `storage.NewStore` directly and are unaffected by the default-on setting; integration tests always pass an explicit temp `SnapshotDir`.

## 13. Rollout plan (PRs)

1. **Type serialization** — `Encode`/`Decode*` for all types + round-trip tests. No behaviour change.
2. **Store snapshot/load** — `snapshot.go`, `Dict.ForEach`, format + CRC, `PersistenceStore`, corruption tests. No behaviour change.
3. **Persistence manager + server integration** — config/flags (default on), `serverCron`, atomic background write, load on start, save on shutdown, `SAVE`/`BGSAVE`/`LASTSAVE`, integration tests.
4. **Docs & packaging** — README section, Dockerfile `VOLUME /data`, `.gitignore`.

Each PR is independently reviewable and mergeable; only PR 3 changes runtime behaviour.

## 14. Open questions

1. Default interval: is 300s right, or should it be shorter (e.g. 60s) given there's no AOF?
2. Should a failed background save make the server reject writes (Redis' `stop-writes-on-bgsave-error`, default yes) or keep accepting them (proposed: keep accepting, log loudly)?
3. File name/extension: `dump.grdb` OK?
4. Is a per-save loop pause of a few hundred ms acceptable for target dataset sizes, or should Option C be scheduled right after?
