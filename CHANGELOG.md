# Changelog

## v1.0.0

Top-to-bottom rewrite. The previous release was a JSON-cache + WAL +
ad-hoc tree; this release is a compressed radix index, a self-balancing
scanner, and a polished ncdu-style TUI. Everything below is new
relative to the prior tag.

### Storage / data model

- **Compressed radix tree** over full S3 keys. Edges hold multi-byte
  path fragments. Logical `/` boundaries are reconstructed at query
  time — no per-segment node bloat.
- **Per-directory aggregates** pre-computed at every internal: object
  count + per-class byte totals. `ListDirectory(prefix)` is O(depth);
  no rescan.
- **Two-arena layout**: `internal` nodes carry children + aggregates,
  `leaf` nodes are 24 B each (edge ref + packed sizeClass uint64).
  Child IDs use a high-bit tag to discriminate arenas.
- **Edge arena**: contiguous chunked byte slab. Edge references are an
  8-byte `(off uint32, length uint32)`. Eliminates per-edge allocator
  padding and the per-string allocation that used to dominate scan
  memory.
- **Dir-marker side map**: S3 zero-byte objects pinned at directory
  boundaries live in a sparse `map[uint32]ClassByte` — no 8-byte
  pointer field tax on the ~99.99% of internals that don't have one.
- **Inline single-class fast path** scaffolding for aggregates (most
  internals carry exactly one storage class).

Measured on a 50M-object bucket: **174 → 101 B/object** in memory,
~42% reduction.

### On-disk snapshot

- **Binary format** (`*.snap`): single contiguous ID space, one
  record per alive node, written via atomic `*.tmp + rename`.
- **Save / Load with progress UI** that knows the file size and
  reports ETA on load.
- Snapshot path defaults to `$XDG_CACHE_HOME/s3du/<bucket>@<region>/
  tree.snap` (or `~/Library/Caches/...` on macOS).
- `-load -snapshot <path>` re-opens an existing index. TUI, `-stats`,
  exports all work against a loaded snapshot — no re-listing.

### Scanner

- **Adaptive worker pool with delimiter probing**. One goroutine per
  worker pulls `(prefix, depth)` items, probes with `/` delimiter,
  fans out CommonPrefixes into the queue. Self-balancing across
  uneven branching factors.
- **Defaults**: `-workers` is `runtime.NumCPU() × 2` (clamped to ≥4),
  `-max-depth` is `3`. Both overridable on the CLI.
- **Inline-fallback on full queue** prevents producer-consumer
  deadlock when fan-out is faster than consumption.
- **HeadBucket-based region auto-discovery** — `-region` is optional.
- **Adaptive SDK retry** via `aws-sdk-go-v2`'s retry/adaptive mode;
  SDK retry events surface through slog when `-log` is enabled.
- **Effective parallelism EWMA** (Little's Law: total request time
  over elapsed wall) live-displayed during the scan.

### TUI

ncdu-inspired keys, beautiful by default (lipgloss, bubbletea).

- **Navigation**: `↑/k`, `↓/j`, `Home`, `End/G`, `PgUp/PgDn`,
  `Enter/l` descend, `Backspace/h` ascend, `q` quit.
- **Sort columns**: `s` size, `n` name, `C` object count, `$`
  monthly cost. Press the same key again to flip direction.
- **Bar widget cycle** (`g`): off → bar → bar + % → % only. Bar is
  normalised to the listing total so entries sum to 100%.
- **Dirs-first toggle** (`t`).
- **Help modal** (`?`) overlaid on the listing — any keystroke
  dismisses.
- **Name-column cursor highlight** instead of full-row reverse, so
  metrics stay readable on the focused row.
- **Per-directory totals + class breakdown** in the status line.
- **`-debug-tree`**: raw radix-internals browser. CIDs, kinds,
  edges, dir-markers, aggregate sizes — for debugging.

### Progress dashboards

- **Scan dashboard** with: inflight workers, EWMA effective
  parallelism, requests/s, objects/s, queue depth, **obj/list**
  efficiency ratio, list-cost (regional pricing), monthly storage
  cost projection.
- **Snapshot load dashboard** with proper progress bar (driven off
  the file's on-disk size), live byte counter, EWMA rate, **ETA**.
- **Snapshot save dashboard** with rate / elapsed (no bar, total
  size isn't known up front).
- **Non-TTY fallback** to a plain `\r`-line reporter for log
  redirection / CI.

### Logging

- **Structured slog**, off by default — pass `-log <path>` to write
  to a file. Stderr stays clean for the TUI.
- **SDK retry / throttling events** flow through the file logger
  (smithy-go adapter) so SlowDown cascades and adaptive backoff
  decisions are debuggable.
- **`-debug`** mirrors logs to stderr (disables the dashboard).
- **SIGUSR1** writes a full goroutine dump next to the snapshot —
  useful for diagnosing a scan that goes quiet without killing it.

### Pricing

- **Per-region LIST-request pricing** (per 1000) for the
  cost-of-scan estimate.
- **Per-region per-class storage pricing** for the monthly storage
  projection. Falls back to us-east-1 for unknown regions.
- Tables include US, EU, AP, Middle East, Africa, South America,
  Mexico, GovCloud — all 30+ commercial S3 regions.

### Tools

- **s3du** — the browser (root binary).
- **cmd/s3rmrf** — parallel batched `DeleteObjects` under a single
  prefix. The `rm -rf` for S3. Refuses to operate on versioning-
  Enabled buckets (delete markers don't free storage). Dry-run by
  default; real deletion requires `-yes` + stdin prefix confirmation.

### Stats / observability

- **`-stats`** flag: after load, print arena occupancy, per-class
  histograms, heap accounting in allocator-rounded bytes. Drove the
  memory layout decisions above.

### Install

```bash
go install github.com/ochaton/s3du@latest
go install github.com/ochaton/s3du/cmd/s3rmrf@latest
```
