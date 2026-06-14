# IDEA

Let's write a simple CLI which accounts S3DU of objects inside single S3 bucket.
  Golang, but we abuse parallel requests as hell, we will do ListObjectsV2 + Prefix.
  Usually humans organizes the first 2 directory levels human-readable, so we can safely run parallel queries within prefixes.
  We must use aws-sdk-go-v2, and enviromental credentials chain.
  We force user to tells bucket name they want to DU.

  I want progress bar: number of listRequests performed, number of objects accounted, total size already accounted. It should be beautifull.
  The strategy should be the following: Run single ListObjectsV2 delimiter=/ (ls <bucket>/) then list one more time each sublevel. Thus we should have N entrypoints that we can
  list in-parallel.

  We reserve N parallel workers, each of them spams ListObjectsV2 + prefix + ContinuationToken and accounts everything into statistics.

  Better mental model: workers push their stats to the outgoing channel (backpressure included). Organize it in golang pipeline-model, exchange with channels (abuse
  uni-directional channels when applicable).

  Simple CLI, no cobra, just golang std, but should be pretty.

  As a result, we should have groupped by statistics by storage class and by our levels we've found (Count of objects, Size of objects)

## Discovery speedup — adaptive worker pool

### Problem

Static -parallel-depth=3 discover + worker-leaf-scan split causes
parallelism collapse at the long tail. Measured on a 50M-object
snapshot: scan completed in 28m10s · eff parallelism: avg 17.8/32
(55%), final 1.4/32 (4%). One big sub-prefix below depth 3 lands on
a single worker while 31 sit idle.

### Plan (Option X — unified pool with adaptive probing)

Replace discover/worker split with one pool of N workers all processing
workItem{prefix, depth} from a single queue.

Worker loop:
- if depth >= maxDepth: paginated recursive scan (delimiter-less),
  emit batches, done.
- else: probe with delimiter, drain all pages, emit Contents as
  batches, enqueue each CommonPrefix as workItem{sub, depth+1}.

Termination: atomic.Int64 pending counter. Each enqueue ++, each
processed item --. Worker that decrements to 0 closes a `done` channel;
siblings exit via select.

Flag: rename -parallel-depth → -max-depth, default 8. workQ buffer
65536 (worst-case headroom for fan-out).

### Why X over Y

Option Y (adaptive BFS that stops deepening at target × workers leaves)
can't react to outlier prefix sizes AFTER discover finishes. If among
320 collected leaves one has 30M objects and others <100K, the long
tail returns. X probes ANY prefix as workers pick it up — big-prefix
workers discover branching, fan out, other workers absorb.

### Caveats

- Probe of "foo/" sees CommonPrefixes=["foo/sub/"] AND Contents=
  ["foo/sub/"] (dir-marker). Sub-prefix worker re-emits the marker;
  AddBatch's three-way merge dedupes. Negligible cost.
- Probe at every level = +1 list request per non-leaf prefix. For
  maxDepth=8 with 26-way branching, ~3K probes — tiny vs ~50K paginated
  recursive scans.

## Memory layout follow-ups (post-A3)

### A2: edge arena

Replace `edge string` (16 B header + per-edge backing with allocator padding)
with `(offset uint32, length uint32)` = 8 B + one big contiguous slab.

Eliminates 1.97 → 1.53 GiB allocator overhead and shaves 8 B per node
(~590 MB on the 74 M-node tree).

### Pack size + class once edges are off-heap

After A2, `leaf` becomes 8 B (edge ref) + 8 B (sizeClass) = 16 B.

User asked: can we save more by using 4 B size + 4 B class?

- 4 B size = 4 GB max. S3 single-object cap is 5 TB → too small.
- 5–6 B size + 1 B class = 7 B. With 1 B of leftover padding the struct still
  rounds up unless we also shrink the edge ref or drop alignment.
- Conclusion: 8 B packed sizeClass is already paid for by alignment.
  Real win only available if we drop the edge ref to ≤ 4 B (e.g. arena offset
  + variable-length suffix lookup). Track separately.

### Inline `Aggregate` into `internal`

`*Aggregate` header + slice backing costs ~1.2 GiB at 50 M. Average internal
has 1.25 distinct classes — inlining `Objects int64 + Bytes ClassBytes` (32 B)
into the `internal` struct would save the per-internal pointer + header alloc.

Trade-off: every internal grows by 32 B even when empty. Net win ~700 MiB.
Lower priority than A2.
