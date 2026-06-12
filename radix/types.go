// Package radix implements an in-memory compressed radix tree (Patricia trie)
// over full S3 object keys, providing filesystem-style navigation by '/'
// boundary together with precomputed per-directory aggregates.
package radix

import "errors"

// Object is a single S3 object record.
type Object struct {
	Key   string
	Size  int64
	Class string
}

// Batch is a contiguous range of S3 objects to ingest.
//
// The caller guarantees that no object exists in the bucket whose key lies in
// (StartFrom, Objects[last].Key] other than those in Objects. Empty StartFrom
// means the beginning of the bucket. Objects must be sorted ascending by Key.
type Batch struct {
	StartFrom string
	Objects   []Object
}

// ClassBytes accumulates byte totals per S3 storage class. Each field maps to
// a documented S3 storage class identifier as returned by ListObjectsV2.
// Unrecognised class strings accumulate into Other so totals remain consistent
// without forcing a heap allocation per node.
type ClassBytes struct {
	Standard          int64 // "STANDARD"
	StandardIA        int64 // "STANDARD_IA"
	OneZoneIA         int64 // "ONEZONE_IA"
	IntelligentTier   int64 // "INTELLIGENT_TIERING"
	GlacierIR         int64 // "GLACIER_IR"
	GlacierFlexible   int64 // "GLACIER"
	DeepArchive       int64 // "DEEP_ARCHIVE"
	ReducedRedundancy int64 // "REDUCED_REDUNDANCY"
	ExpressOneZone    int64 // "EXPRESS_ONEZONE"
	Snow              int64 // "SNOW"
	Other             int64 // anything else, incl. empty string
}

// Add adds n bytes (possibly negative) to the bucket selected by class.
func (c *ClassBytes) Add(class string, n int64) {
	switch class {
	case "STANDARD":
		c.Standard += n
	case "STANDARD_IA":
		c.StandardIA += n
	case "ONEZONE_IA":
		c.OneZoneIA += n
	case "INTELLIGENT_TIERING":
		c.IntelligentTier += n
	case "GLACIER_IR":
		c.GlacierIR += n
	case "GLACIER":
		c.GlacierFlexible += n
	case "DEEP_ARCHIVE":
		c.DeepArchive += n
	case "REDUCED_REDUNDANCY":
		c.ReducedRedundancy += n
	case "EXPRESS_ONEZONE":
		c.ExpressOneZone += n
	case "SNOW":
		c.Snow += n
	default:
		c.Other += n
	}
}

// NonZero returns the populated buckets in canonical order. Useful for
// rendering and equality assertions.
func (c ClassBytes) NonZero() []ClassByte {
	out := make([]ClassByte, 0, 4)
	for _, kv := range []ClassByte{
		{"STANDARD", c.Standard},
		{"STANDARD_IA", c.StandardIA},
		{"ONEZONE_IA", c.OneZoneIA},
		{"INTELLIGENT_TIERING", c.IntelligentTier},
		{"GLACIER_IR", c.GlacierIR},
		{"GLACIER", c.GlacierFlexible},
		{"DEEP_ARCHIVE", c.DeepArchive},
		{"REDUCED_REDUNDANCY", c.ReducedRedundancy},
		{"EXPRESS_ONEZONE", c.ExpressOneZone},
		{"SNOW", c.Snow},
		{"OTHER", c.Other},
	} {
		if kv.Bytes != 0 {
			out = append(out, kv)
		}
	}
	return out
}

// ClassByte is a single (storage class, bytes) pair, used by ClassBytes.NonZero.
type ClassByte struct {
	Class string
	Bytes int64
}

// Aggregate is a recursive summary over a subtree of the radix tree.
type Aggregate struct {
	Objects int64
	Bytes   ClassBytes
}

// Entry is one element of a ListDirectory result. Either a subdirectory (IsDir
// true, Aggregate populated) or a direct file (IsDir false, Class+Size set).
type Entry struct {
	Name      string
	IsDir     bool
	Class     string
	Size      int64
	Aggregate Aggregate
}

var (
	ErrInvalidPrefix = errors.New("radix: prefix must be empty or end with '/'")
	ErrUnsortedBatch = errors.New("radix: batch objects must be sorted ascending and unique")
	ErrBatchRange    = errors.New("radix: batch contains key <= StartFrom")
)
