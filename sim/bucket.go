// Package sim simulates an S3 bucket on top of a radix.Tree, implementing
// just enough of the ListObjectsV2 surface to drive discovery-strategy
// comparisons without round-tripping to AWS. Pagination, MaxKeys, delimiter
// grouping and StartAfter all match S3's documented semantics.
//
// The simulator's headline metrics are exact (every Bucket.List call
// increments a single counter, and the keys-per-request ratio is therefore
// authoritative). The wall-clock metric is approximate — optional per-call
// latency is simulated via time.Sleep so real goroutine scheduling
// reproduces real worker contention, but SDK retries, throttling, and
// TCP head-of-line blocking are out of scope.
package sim

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ochaton/s3du/radix"
)

// MaxKeysCap mirrors S3's hard ceiling: any ListObjectsV2 call returns at
// most 1 000 entries (Contents + CommonPrefixes combined) per page.
const MaxKeysCap = 1000

// maxS3KeyBytes is S3's hard limit on object key length. Used to construct
// continuation tokens that lex-exceed every key under a just-emitted
// CommonPrefix without lex-exceeding the next sibling outside the subtree.
const maxS3KeyBytes = 1024

// subtreePad is the worst-case pad of 0xFF bytes appended to a CommonPrefix
// to produce a continuation token strictly greater than every possible key
// in that subtree but strictly less than the smallest key beyond it. Pre-
// allocated once at startup.
var subtreePad = strings.Repeat("\xff", maxS3KeyBytes)

// Bucket is an in-memory S3 emulator backed by a radix.Tree. Safe for
// concurrent List calls.
type Bucket struct {
	tree    *radix.Tree
	latency time.Duration
	reqs    atomic.Int64
	objects atomic.Int64 // count of Object entries returned across all calls
}

// New wraps tree as an S3-like bucket. latency is the synthetic per-call
// delay imposed on every List call to mirror real network latency; pass 0
// to skip the sleep entirely (useful when only request counts matter).
func New(tree *radix.Tree, latency time.Duration) *Bucket {
	return &Bucket{tree: tree, latency: latency}
}

// ListReq mirrors the subset of ListObjectsV2Input the simulator honours.
// StartAfter doubles as the continuation token: pass the previous response's
// NextContinuationToken to resume.
type ListReq struct {
	Prefix     string
	Delimiter  string // "" or "/"
	StartAfter string
	MaxKeys    int // capped at MaxKeysCap
}

// ListResp mirrors the relevant fields of ListObjectsV2Output. CommonPrefixes
// is populated only when Delimiter == "/".
type ListResp struct {
	Contents              []radix.Object
	CommonPrefixes        []string
	NextContinuationToken string
	IsTruncated           bool
}

// List runs one ListObjectsV2 against the simulated bucket and returns one
// page of results.
func (b *Bucket) List(ctx context.Context, req ListReq) (ListResp, error) {
	b.reqs.Add(1)
	if b.latency > 0 {
		select {
		case <-time.After(b.latency):
		case <-ctx.Done():
			return ListResp{}, ctx.Err()
		}
	}
	maxKeys := req.MaxKeys
	if maxKeys <= 0 || maxKeys > MaxKeysCap {
		maxKeys = MaxKeysCap
	}

	var resp ListResp
	if req.Delimiter == "/" {
		resp = b.listWithDelimiter(req, maxKeys)
	} else {
		resp = b.listNoDelimiter(req, maxKeys)
	}
	b.objects.Add(int64(len(resp.Contents)))
	return resp, nil
}

// Requests returns the cumulative number of List calls the Bucket has
// served. Atomic, cheap; safe for the progress dashboard to poll.
func (b *Bucket) Requests() int64 {
	return b.reqs.Load()
}

// ObjectsServed returns the cumulative number of Object entries returned
// across every List call so far. CommonPrefixes are not counted — they are
// references, not objects.
func (b *Bucket) ObjectsServed() int64 {
	return b.objects.Load()
}

func (b *Bucket) listNoDelimiter(req ListReq, maxKeys int) ListResp {
	it := b.tree.NewIterator(req.StartAfter)
	resp := ListResp{Contents: make([]radix.Object, 0, maxKeys)}
	var lastKey string
	for len(resp.Contents) < maxKeys {
		obj, ok := it.Next()
		if !ok {
			return resp
		}
		if !strings.HasPrefix(obj.Key, req.Prefix) {
			return resp
		}
		resp.Contents = append(resp.Contents, obj)
		lastKey = obj.Key
	}
	// Page full; check if anything more under prefix exists.
	if more, ok := it.Next(); ok && strings.HasPrefix(more.Key, req.Prefix) {
		resp.IsTruncated = true
		resp.NextContinuationToken = lastKey
	}
	return resp
}

func (b *Bucket) listWithDelimiter(req ListReq, maxKeys int) ListResp {
	it := b.tree.NewIterator(req.StartAfter)
	resp := ListResp{
		Contents:       make([]radix.Object, 0, maxKeys/2),
		CommonPrefixes: make([]string, 0, maxKeys/2),
	}
	var lastEmitted string
	for len(resp.Contents)+len(resp.CommonPrefixes) < maxKeys {
		obj, ok := it.Next()
		if !ok {
			return resp
		}
		if !strings.HasPrefix(obj.Key, req.Prefix) {
			return resp
		}
		suffix := obj.Key[len(req.Prefix):]
		if idx := strings.IndexByte(suffix, '/'); idx >= 0 {
			groupKey := req.Prefix + suffix[:idx+1]
			resp.CommonPrefixes = append(resp.CommonPrefixes, groupKey)
			// The continuation token for a CommonPrefix must strictly
			// exceed every key beneath that prefix but stay strictly less
			// than the next sibling outside the subtree. Padding with
			// 0xFF up to S3's max key length satisfies both bounds and
			// also doubles as the SkipTo target for in-call pagination.
			lastEmitted = groupKey + subtreePad
			it.SkipTo(lastEmitted)
			continue
		}
		resp.Contents = append(resp.Contents, obj)
		lastEmitted = obj.Key
	}
	// Page full; check if anything more under prefix exists.
	if more, ok := it.Next(); ok && strings.HasPrefix(more.Key, req.Prefix) {
		resp.IsTruncated = true
		resp.NextContinuationToken = lastEmitted
	}
	return resp
}
