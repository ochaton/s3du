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
	"bytes"
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

// prefixLowerExclusive returns a string that lex-precedes every key with
// the given prefix and lex-succeeds every key strictly less than the
// prefix region. It is constructed by decrementing the last byte and
// padding with 0xFF, which:
//
//   - keeps the result strictly less than prefix (different lex-byte at
//     the boundary position),
//   - exceeds any key whose bytes diverge from prefix earlier on the path.
//
// Returns "" for an empty prefix (no lower bound to enforce) or for a
// prefix whose last byte is 0x00 (degenerate — drops the byte instead).
func prefixLowerExclusive(prefix string) string {
	if prefix == "" {
		return ""
	}
	b := []byte(prefix)
	last := b[len(b)-1]
	if last == 0 {
		return string(b[:len(b)-1])
	}
	b[len(b)-1] = last - 1
	return string(b) + subtreePad
}

// iteratorStart returns the exclusive lower bound to hand to NewIterator
// for a ListReq, taking the user-supplied StartAfter when it already sits
// inside or past the prefix region but advancing to the prefix's
// lex-predecessor otherwise so the iterator's first returned key is the
// first object actually under prefix.
func iteratorStart(prefix, startAfter string) string {
	if pred := prefixLowerExclusive(prefix); pred > startAfter {
		return pred
	}
	return startAfter
}

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

// ListResp is a count-and-metadata projection of ListObjectsV2Output. The
// simulator never materialises individual Content keys — strategies under
// test only need the count, the discovered CommonPrefixes (for fan-out),
// and a continuation token, so paying for 50 M string allocations on a
// 50 M-object scan is wasteful. Per-content metadata (sizes, classes,
// per-storage-class rollups) belongs in dedicated future helpers if a
// strategy ever needs them.
type ListResp struct {
	ContentsCount         int      // # of Contents entries this page would have returned
	CommonPrefixes        []string // CommonPrefix groupings, always materialised (small N per page)
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
	b.objects.Add(int64(resp.ContentsCount))
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
	it := b.tree.NewIterator(iteratorStart(req.Prefix, req.StartAfter))
	prefixBytes := []byte(req.Prefix)
	var resp ListResp
	var lastKey string

	// Iterator.Walk yields each key as a byte view into the cursor's path
	// buffer — zero-alloc per object. We only materialise a string at the
	// moment we decide to stop the page (either because MaxKeys reached or
	// because the prefix region is exhausted), and only for that one key.
	stopAtFollowing := false
	it.Walk(func(key []byte, _ radix.StorageClass, _ int64) bool {
		if stopAtFollowing {
			// We've already collected MaxKeys; one more candidate confirms
			// truncation. Capture it and stop.
			if len(key) >= len(prefixBytes) && bytes.HasPrefix(key, prefixBytes) {
				resp.IsTruncated = true
				resp.NextContinuationToken = lastKey
			}
			return false
		}
		if !bytes.HasPrefix(key, prefixBytes) {
			return false
		}
		resp.ContentsCount++
		if resp.ContentsCount >= maxKeys {
			lastKey = string(key)
			stopAtFollowing = true
			return true
		}
		return true
	})
	return resp
}

func (b *Bucket) listWithDelimiter(req ListReq, maxKeys int) ListResp {
	it := b.tree.NewIterator(iteratorStart(req.Prefix, req.StartAfter))
	prefixBytes := []byte(req.Prefix)
	resp := ListResp{CommonPrefixes: make([]string, 0, 32)}
	var lastEmitted string
	emitted := 0
	stopAtFollowing := false

	it.Walk(func(key []byte, _ radix.StorageClass, _ int64) bool {
		if stopAtFollowing {
			if len(key) >= len(prefixBytes) && bytes.HasPrefix(key, prefixBytes) {
				resp.IsTruncated = true
				resp.NextContinuationToken = lastEmitted
			}
			return false
		}
		if !bytes.HasPrefix(key, prefixBytes) {
			return false
		}
		suffix := key[len(prefixBytes):]
		if idx := bytes.IndexByte(suffix, '/'); idx >= 0 {
			groupKey := string(key[:len(prefixBytes)+idx+1])
			resp.CommonPrefixes = append(resp.CommonPrefixes, groupKey)
			lastEmitted = groupKey + subtreePad
			it.SkipTo(lastEmitted)
			emitted++
			if emitted >= maxKeys {
				stopAtFollowing = true
			}
			return true
		}
		resp.ContentsCount++
		emitted++
		if emitted >= maxKeys {
			lastEmitted = string(key)
			stopAtFollowing = true
		}
		return true
	})
	return resp
}
