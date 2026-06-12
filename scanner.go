package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ochaton/s3du/radix"
)

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
// Why this works without overlap:
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

	tree *radix.Tree
	prog *Progress

	workQ  chan string
	batchQ chan radix.Batch

	// First non-nil error captured by any goroutine; reported by Run.
	errMu sync.Mutex
	err   error
}

// NewScanner constructs a Scanner. The caller owns tree and prog.
func NewScanner(client *s3.Client, bucket string, workers, parallelDepth int, tree *radix.Tree, prog *Progress) *Scanner {
	return &Scanner{
		s3:            client,
		bucket:        bucket,
		parallelDepth: parallelDepth,
		workers:       workers,
		tree:          tree,
		prog:          prog,
	}
}

// Run starts the discover/worker/writer goroutines and blocks until all
// stages drain or ctx is cancelled. The first error from any goroutine is
// returned.
func (s *Scanner) Run(ctx context.Context) error {
	s.workQ = make(chan string, 1024)
	s.batchQ = make(chan radix.Batch, 256)

	var wgDiscover, wgWorkers, wgWriter sync.WaitGroup

	// Writer (single consumer of the tree).
	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		for b := range s.batchQ {
			if err := s.tree.AddBatch(b); err != nil {
				s.recordErr(fmt.Errorf("AddBatch: %w", err))
			}
		}
	}()

	// Workers.
	for range s.workers {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			for prefix := range s.workQ {
				if err := s.listRecursive(ctx, prefix); err != nil {
					s.recordErr(fmt.Errorf("listRecursive %q: %w", prefix, err))
					return
				}
			}
		}()
	}

	// Discover.
	wgDiscover.Add(1)
	go func() {
		defer wgDiscover.Done()
		if err := s.discover(ctx, "", 0); err != nil {
			s.recordErr(fmt.Errorf("discover: %w", err))
		}
	}()

	wgDiscover.Wait()
	close(s.workQ)
	wgWorkers.Wait()
	close(s.batchQ)
	wgWriter.Wait()

	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// discover walks the bucket with delimiter='/' down to parallelDepth. At
// each level it forwards Contents as batches and recurses into
// CommonPrefixes. At parallelDepth it enqueues the prefix for the workers.
func (s *Scanner) discover(ctx context.Context, prefix string, depth int) error {
	if depth >= s.parallelDepth {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.workQ <- prefix:
		}
		return nil
	}

	p := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	prevKey := prefix
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		s.prog.observeList()

		if len(page.Contents) > 0 {
			batch := makeBatch(prevKey, page.Contents)
			s.prog.observeBatch(batch.Objects)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.batchQ <- batch:
			}
			prevKey = batch.Objects[len(batch.Objects)-1].Key
		}
		for _, cp := range page.CommonPrefixes {
			sub := aws.ToString(cp.Prefix)
			if sub == "" {
				continue
			}
			if err := s.discover(ctx, sub, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// listRecursive performs a paginated, delimiter-less ListObjectsV2 over a
// single leaf prefix and forwards each page as a radix.Batch.
func (s *Scanner) listRecursive(ctx context.Context, prefix string) error {
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
		s.prog.observeList()
		if len(page.Contents) == 0 {
			continue
		}
		batch := makeBatch(prevKey, page.Contents)
		s.prog.observeBatch(batch.Objects)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.batchQ <- batch:
		}
		prevKey = batch.Objects[len(batch.Objects)-1].Key
	}
	return nil
}

func (s *Scanner) recordErr(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

// makeBatch converts an S3 page's Contents into a radix.Batch with the
// supplied StartFrom (the lex predecessor of the page's first key).
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
