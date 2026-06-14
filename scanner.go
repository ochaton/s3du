package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ochaton/s3du/radix"
)

// slashStr is the single-byte delimiter passed to ListObjectsV2; hoisted to
// avoid one *string allocation per paginator init.
var slashStr = aws.String("/")

// workQueueCapacity bounds the in-flight queue. Sized for worst-case fan-out:
// the deepest layer can briefly hold every just-discovered sub-prefix. At
// 32 workers and ~10³ branching factor per probe, 64 K is comfortable.
const workQueueCapacity = 65536

// Scanner walks an S3 bucket and ingests every object into a radix.Tree.
//
// One unified worker pool processes a single queue of work items. Each item
// is (prefix, depth):
//
//   - depth >= maxDepth → paginated recursive ListObjectsV2 (no delimiter)
//     emits every page as a radix.Batch and exits.
//   - depth < maxDepth → probe ListObjectsV2 with delimiter='/'. Each
//     page's Contents go out as a batch; each CommonPrefix is enqueued
//     back as a new work item one level deeper.
//
// Self-balancing: a worker that picks up a huge prefix probes it, finds
// its branching structure, and fans the sub-prefixes back into the queue.
// The idle workers pick up those sub-prefixes — no long-tail collapse.
//
// Termination: a single atomic counter tracks queued-but-unprocessed items.
// Each enqueue increments BEFORE send; each worker decrements AFTER process
// (after any sub-enqueues it makes have completed their Add). The worker
// whose decrement drops the counter to zero closes a `done` channel;
// siblings observe done in select and exit.
//
// No key overlap concerns: sub-prefixes returned by a delimiter probe are
// mutually exclusive byte ranges. Per-batch StartFrom is the prefix with
// any trailing '/' stripped, which is strictly less than every key in the
// batch and strictly greater than any key in any sibling prefix.
type Scanner struct {
	s3       *s3.Client
	bucket   string
	maxDepth int
	workers  int

	tree     *radix.Tree
	progress *Progress

	// firstErr captures the first error reported by any goroutine. Set via
	// CompareAndSwap so writers stay race-free; reads see a stable value
	// once Wait completes.
	firstErr atomic.Pointer[error]
	cancel   context.CancelFunc

	ran atomic.Bool // guards against repeat Run calls
}

// workItem is one prefix scheduled for either probing or recursive scanning.
type workItem struct {
	prefix string
	depth  int
}

// NewScanner constructs a Scanner. The caller owns tree and progress.
func NewScanner(client *s3.Client, bucket string, workers, maxDepth int, tree *radix.Tree, progress *Progress) *Scanner {
	return &Scanner{
		s3:       client,
		bucket:   bucket,
		maxDepth: maxDepth,
		workers:  workers,
		tree:     tree,
		progress: progress,
	}
}

// Run starts the worker pool and writer goroutine, seeds the root prefix,
// and blocks until every queued item has been processed (or the context
// has been cancelled by an error). Single-use; subsequent calls panic.
func (s *Scanner) Run(ctx context.Context) error {
	if !s.ran.CompareAndSwap(false, true) {
		panic("radix: Scanner.Run called more than once; construct a fresh Scanner")
	}
	ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	workQ := make(chan workItem, workQueueCapacity)
	batchQ := make(chan radix.Batch, 256)
	done := make(chan struct{})

	var pending atomic.Int64

	// tryEnqueue tries to publish a work item to workQ without blocking. If
	// the queue is full the caller is told (false return) to process the
	// item inline instead; this avoids the deadlock where every worker is
	// blocked in send to a full queue with no goroutine left to drain it.
	// The seed item uses the blocking variant via enqueueBlocking below.
	tryEnqueue := func(item workItem) bool {
		if ctx.Err() != nil {
			return false
		}
		pending.Add(1)
		select {
		case workQ <- item:
			return true
		default:
			pending.Add(-1)
			return false
		}
	}
	// enqueueBlocking is used only for the seed item, where there is no
	// alternative path to take. Workers always go through tryEnqueue.
	enqueueBlocking := func(item workItem) bool {
		pending.Add(1)
		select {
		case workQ <- item:
			return true
		case <-ctx.Done():
			pending.Add(-1)
			return false
		}
	}

	// dispatch is what workers call to schedule a sub-prefix: try to publish
	// to workQ; if the queue is full, process inline in the same goroutine.
	// The recursive reference uses a forward var because closures can't see
	// their own name during initialisation.
	var dispatch func(workItem) error
	dispatch = func(item workItem) error {
		if tryEnqueue(item) {
			return nil
		}
		return s.processItem(ctx, item, dispatch, batchQ)
	}

	var wgWriter, wgWorkers sync.WaitGroup

	wgWriter.Go(func() {
		for b := range batchQ {
			if err := s.tree.AddBatch(b); err != nil {
				slog.Error("AddBatch failed",
					"err", err,
					"startFrom", b.StartFrom,
					"len", len(b.Objects),
					"first", firstKey(b.Objects),
					"last", lastKey(b.Objects),
				)
				s.recordErr(fmt.Errorf("AddBatch: %w", err))
			}
		}
	})

	for range s.workers {
		wgWorkers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				case item := <-workQ:
					if err := s.processItem(ctx, item, dispatch, batchQ); err != nil {
						s.recordErr(fmt.Errorf("process %q: %w", item.prefix, err))
					}
					if pending.Add(-1) == 0 {
						close(done)
						return
					}
				}
			}
		})
	}

	if !enqueueBlocking(workItem{prefix: "", depth: 0}) {
		// ctx cancelled before the seed landed; workers will exit via
		// ctx.Done and the writer below.
	}

	wgWorkers.Wait()
	close(batchQ)
	wgWriter.Wait()

	if p := s.firstErr.Load(); p != nil {
		return *p
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// processItem dispatches one work item to either the recursive scanner
// (at the depth cap) or the delimiter-probe routine. The dispatch callback
// is how sub-prefixes get scheduled; the same callback may invoke
// processItem recursively for inline fallback when the queue is full.
func (s *Scanner) processItem(ctx context.Context, item workItem, dispatch func(workItem) error, batchQ chan<- radix.Batch) error {
	if item.depth >= s.maxDepth {
		return s.listRecursive(ctx, batchQ, item.prefix)
	}
	return s.probeAndFanOut(ctx, item, dispatch, batchQ)
}

// probeAndFanOut lists item.prefix with delimiter='/', emits every page's
// Contents as a batch, and dispatches each discovered CommonPrefix one
// level deeper. dispatch publishes to workQ when there's room, or runs
// the sub-prefix inline otherwise — this is what prevents the deadlock
// where every worker is blocked sending to a full queue.
func (s *Scanner) probeAndFanOut(ctx context.Context, item workItem, dispatch func(workItem) error, batchQ chan<- radix.Batch) error {
	slog.Debug("probe", "prefix", item.prefix, "depth", item.depth)
	p := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(item.prefix),
		Delimiter: slashStr,
	})
	// prevKey: the byte string strictly less than any key returned for this
	// prefix. Stripping a trailing '/' gives us that — the byte after '/'
	// (0x2F) cannot start any sibling prefix or any unrelated key range.
	// Also correctly admits the S3 dir-marker object whose key equals the
	// scanned prefix verbatim.
	prevKey := strings.TrimSuffix(item.prefix, "/")
	for p.HasMorePages() {
		start := s.progress.beginRequest()
		page, err := p.NextPage(ctx)
		s.progress.endRequest(start)
		if err != nil {
			return err
		}
		if len(page.Contents) > 0 {
			batch := makeBatch(prevKey, page.Contents)
			logBatch("probe.batch", item.prefix, batch)
			s.progress.observeBatch(batch.Objects)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case batchQ <- batch:
			}
			prevKey = batch.Objects[len(batch.Objects)-1].Key
		}
		for _, cp := range page.CommonPrefixes {
			sub := aws.ToString(cp.Prefix)
			if sub == "" {
				continue
			}
			if err := dispatch(workItem{prefix: sub, depth: item.depth + 1}); err != nil {
				return err
			}
		}
	}
	return nil
}

// listRecursive performs a paginated, delimiter-less ListObjectsV2 over a
// single prefix and forwards each page as a radix.Batch. Used when probing
// has hit the depth cap — covers the entire remaining sub-tree.
func (s *Scanner) listRecursive(ctx context.Context, batchQ chan<- radix.Batch, prefix string) error {
	slog.Debug("listRecursive", "prefix", prefix)
	p := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	prevKey := strings.TrimSuffix(prefix, "/")
	for p.HasMorePages() {
		start := s.progress.beginRequest()
		page, err := p.NextPage(ctx)
		s.progress.endRequest(start)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}
		batch := makeBatch(prevKey, page.Contents)
		logBatch("listRecursive.batch", prefix, batch)
		s.progress.observeBatch(batch.Objects)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batchQ <- batch:
		}
		prevKey = batch.Objects[len(batch.Objects)-1].Key
	}
	return nil
}

// logBatch emits a Debug-level structured log entry summarising a batch
// destined for the writer. Cheap when the slog level is above Debug.
func logBatch(event, prefix string, b radix.Batch) {
	if len(b.Objects) == 0 {
		return
	}
	slog.Debug(event,
		"prefix", prefix,
		"startFrom", b.StartFrom,
		"first", b.Objects[0].Key,
		"last", b.Objects[len(b.Objects)-1].Key,
		"len", len(b.Objects),
	)
}

// recordErr captures the first non-nil error and cancels the shared
// context so peer goroutines see ctx.Done() and exit promptly.
func (s *Scanner) recordErr(err error) {
	if err == nil {
		return
	}
	if s.firstErr.CompareAndSwap(nil, &err) {
		s.cancel()
	}
}

// firstKey / lastKey are small helpers used only by the AddBatch error log.
func firstKey(objs []radix.Object) string {
	if len(objs) == 0 {
		return ""
	}
	return objs[0].Key
}

func lastKey(objs []radix.Object) string {
	if len(objs) == 0 {
		return ""
	}
	return objs[len(objs)-1].Key
}

// makeBatch converts an S3 page's Contents into a radix.Batch with the
// supplied StartFrom (the lex predecessor of the page's first key). The
// loop indexes by pointer to avoid copying the ~80-byte types.Object
// values; do not "simplify" to a value-range form.
func makeBatch(startFrom string, contents []types.Object) radix.Batch {
	objs := make([]radix.Object, 0, len(contents))
	for i := range contents {
		c := &contents[i]
		objs = append(objs, radix.Object{
			Key:   aws.ToString(c.Key),
			Size:  aws.ToInt64(c.Size),
			Class: radix.ParseClass(string(c.StorageClass)),
		})
	}
	return radix.Batch{StartFrom: startFrom, Objects: objs}
}
