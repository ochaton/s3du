package main

import (
	"context"
	"errors"
	"fmt"
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

// Scanner walks an S3 bucket and ingests every object into a radix.Tree.
//
// The pipeline has three stages connected by channels:
//
//  1. discover — one goroutine. Walks the bucket using delimiter='/' down
//     to parallelDepth, emitting "leaf" prefixes (deep enough to be worth
//     parallelising) into workQ. At each level it also emits the directly
//     visible files (Contents) as batches so files lying above the leaf
//     depth are not missed.
//
//  2. workers — N goroutines. Each pulls a leaf prefix from workQ and does
//     a paginated, recursive ListObjectsV2 (no delimiter) over that prefix.
//     Every page becomes one radix.Batch sent to batchQ.
//
//  3. writer — one goroutine. Drains batchQ and calls tree.AddBatch. Single
//     consumer ⇒ no mutex needed around the tree.
//
// On any goroutine returning a non-nil error, Scanner cancels the shared
// context so the other stages observe ctx.Done() and unwind promptly;
// without that, a worker error would leave the buffered workQ filling up
// and deadlock discover's send.
//
// Why no key overlap across workers:
//   - parallelDepth leaf prefixes are mutually exclusive (each is a distinct
//     branch of the delimiter walk).
//   - discover's intermediate-level Contents are at depths strictly above
//     parallelDepth and never overlap any worker's scan.
//   - Within a worker's prefix scan, pages are sequentially non-overlapping
//     and the AddBatch StartFrom contract is satisfied page-to-page.
//   - Across workers, each batch's lex range is contained in its own prefix's
//     subtree; AddBatch's cursor finds nothing in other workers' ranges.
type Scanner struct {
	s3            *s3.Client
	bucket        string
	parallelDepth int
	workers       int

	tree     *radix.Tree
	progress *Progress

	// firstErr captures the first error reported by any goroutine. It is
	// set via CompareAndSwap so writers race-free and reads see a stable
	// value once Wait completes.
	firstErr atomic.Pointer[error]
	cancel   context.CancelFunc

	ran atomic.Bool // guards against repeat Run calls
}

// NewScanner constructs a Scanner. The caller owns tree and progress.
func NewScanner(client *s3.Client, bucket string, workers, parallelDepth int, tree *radix.Tree, progress *Progress) *Scanner {
	return &Scanner{
		s3:            client,
		bucket:        bucket,
		parallelDepth: parallelDepth,
		workers:       workers,
		tree:          tree,
		progress:      progress,
	}
}

// Run starts the discover/worker/writer goroutines and blocks until all
// stages drain. The first error from any goroutine is returned and the
// shared context is cancelled so the remaining stages exit promptly.
//
// Run is single-use: subsequent calls panic.
func (s *Scanner) Run(ctx context.Context) error {
	if !s.ran.CompareAndSwap(false, true) {
		panic("radix: Scanner.Run called more than once; construct a fresh Scanner")
	}
	ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	// Channels are local to Run; owning them on the struct invites misuse
	// from callers that hold a Scanner reference.
	workQ := make(chan string, 1024)
	batchQ := make(chan radix.Batch, 256)

	var wgWriter, wgWorkers, wgDiscover sync.WaitGroup

	wgWriter.Go(func() {
		for b := range batchQ {
			if err := s.tree.AddBatch(b); err != nil {
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
				case prefix, ok := <-workQ:
					if !ok {
						return
					}
					if err := s.listRecursive(ctx, batchQ, prefix); err != nil {
						s.recordErr(fmt.Errorf("listRecursive %q: %w", prefix, err))
						return
					}
				}
			}
		})
	}

	wgDiscover.Go(func() {
		if err := s.discover(ctx, workQ, batchQ, "", 0); err != nil {
			s.recordErr(fmt.Errorf("discover: %w", err))
		}
	})

	wgDiscover.Wait()
	close(workQ)
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

// discover walks the bucket with delimiter='/' down to parallelDepth. At
// each level it forwards Contents as batches and recurses into
// CommonPrefixes. At parallelDepth it enqueues the prefix for the workers.
func (s *Scanner) discover(ctx context.Context, workQ chan<- string, batchQ chan<- radix.Batch, prefix string, depth int) error {
	if depth >= s.parallelDepth {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case workQ <- prefix:
		}
		return nil
	}

	p := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: slashStr,
	})
	// First page's StartFrom: any key starting with this prefix is strictly
	// greater than the prefix string itself, so the prefix is a valid lex
	// lower bound for radix.Batch's exclusive StartFrom contract.
	prevKey := prefix
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		s.progress.observeList()

		if len(page.Contents) > 0 {
			batch := makeBatch(prevKey, page.Contents)
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
			if err := s.discover(ctx, workQ, batchQ, sub, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// listRecursive performs a paginated, delimiter-less ListObjectsV2 over a
// single leaf prefix and forwards each page as a radix.Batch.
func (s *Scanner) listRecursive(ctx context.Context, batchQ chan<- radix.Batch, prefix string) error {
	p := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	prevKey := prefix
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		s.progress.observeList()
		if len(page.Contents) == 0 {
			continue
		}
		batch := makeBatch(prevKey, page.Contents)
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
