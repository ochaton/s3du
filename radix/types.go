// Package radix implements an in-memory compressed radix tree (Patricia trie)
// over full S3 object keys, providing filesystem-style navigation by '/'
// boundary together with precomputed per-directory aggregates.
package radix

import "errors"

// StorageClass enumerates the S3 storage classes returned by ListObjectsV2.
// Encoding as a uint8 keeps per-node memory tight; ClassByte.Class is one
// byte rather than the 16 bytes of a string header.
type StorageClass uint8

const (
	ClassUnknown StorageClass = iota
	ClassStandard
	ClassStandardIA
	ClassOneZoneIA
	ClassIntelligentTier
	ClassGlacierIR
	ClassGlacier
	ClassDeepArchive
	ClassReducedRedundancy
	ClassExpressOneZone
	ClassSnow
)

// ParseClass maps an S3 storage-class string to a [StorageClass]. Unknown
// strings (and the empty string) collapse to [ClassUnknown].
func ParseClass(s string) StorageClass {
	switch s {
	case "STANDARD":
		return ClassStandard
	case "STANDARD_IA":
		return ClassStandardIA
	case "ONEZONE_IA":
		return ClassOneZoneIA
	case "INTELLIGENT_TIERING":
		return ClassIntelligentTier
	case "GLACIER_IR":
		return ClassGlacierIR
	case "GLACIER":
		return ClassGlacier
	case "DEEP_ARCHIVE":
		return ClassDeepArchive
	case "REDUCED_REDUNDANCY":
		return ClassReducedRedundancy
	case "EXPRESS_ONEZONE":
		return ClassExpressOneZone
	case "SNOW":
		return ClassSnow
	default:
		return ClassUnknown
	}
}

// String returns the canonical S3 identifier for c.
func (c StorageClass) String() string {
	switch c {
	case ClassStandard:
		return "STANDARD"
	case ClassStandardIA:
		return "STANDARD_IA"
	case ClassOneZoneIA:
		return "ONEZONE_IA"
	case ClassIntelligentTier:
		return "INTELLIGENT_TIERING"
	case ClassGlacierIR:
		return "GLACIER_IR"
	case ClassGlacier:
		return "GLACIER"
	case ClassDeepArchive:
		return "DEEP_ARCHIVE"
	case ClassReducedRedundancy:
		return "REDUCED_REDUNDANCY"
	case ClassExpressOneZone:
		return "EXPRESS_ONEZONE"
	case ClassSnow:
		return "SNOW"
	default:
		return "UNKNOWN"
	}
}

// Object is a single S3 object record.
type Object struct {
	Key   string
	Size  int64
	Class StorageClass
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

// ClassByte is one (storage class, byte-count) pair. It does double duty: as
// a single file's metadata (storage class + size in bytes) and as one entry
// in a sparse aggregate over many files of the same class.
type ClassByte struct {
	Class StorageClass
	Size  int64
}

// ClassBytes is a sparse, unsorted accumulator keyed by [StorageClass]. The
// number of distinct classes per node is small (S3 has roughly a dozen
// classes, real buckets typically use 1–3), so linear scans beat ordered
// lookups while keeping per-node memory minimal.
type ClassBytes []ClassByte

// Add adds n bytes (possibly negative) to the bucket for class. Buckets that
// reach zero are removed; a new bucket with n == 0 is not appended.
func (c *ClassBytes) Add(class StorageClass, n int64) {
	s := *c
	for i := range s {
		if s[i].Class != class {
			continue
		}
		s[i].Size += n
		if s[i].Size == 0 {
			*c = append(s[:i], s[i+1:]...)
		}
		return
	}
	if n == 0 {
		return
	}
	*c = append(s, ClassByte{Class: class, Size: n})
}

// AddAll merges src into *c.
func (c *ClassBytes) AddAll(src ClassBytes) {
	for i := range src {
		c.Add(src[i].Class, src[i].Size)
	}
}

// NonZero returns the populated buckets. The receiver already excludes zero
// buckets, so the return value is the slice itself.
func (c ClassBytes) NonZero() []ClassByte { return c }

// Total returns the sum of Size across every bucket. It is the byte
// counterpart to [Aggregate.Objects] for callers that have already drilled
// down to a ClassBytes value.
func (c ClassBytes) Total() int64 {
	var t int64
	for i := range c {
		t += c[i].Size
	}
	return t
}

// Aggregate is a recursive summary over a subtree of the radix tree.
type Aggregate struct {
	Objects int64
	Bytes   ClassBytes
}

// Equal reports whether two aggregates carry the same data. Bytes is treated
// as an unordered multiset since [ClassBytes.Add] does not sort.
func (a Aggregate) Equal(b Aggregate) bool {
	if a.Objects != b.Objects || len(a.Bytes) != len(b.Bytes) {
		return false
	}
outer:
	for i := range a.Bytes {
		want := a.Bytes[i]
		for j := range b.Bytes {
			if b.Bytes[j] == want {
				continue outer
			}
		}
		return false
	}
	return true
}

// Entry is one element of a ListDirectory result. Either a subdirectory
// (IsDir true, Aggregate populated) or a direct file (IsDir false, Class+Size
// set).
type Entry struct {
	Name      string
	IsDir     bool
	Class     StorageClass
	Size      int64
	Aggregate Aggregate
}

var (
	ErrInvalidPrefix = errors.New("radix: prefix must be empty or end with '/'")
	ErrUnsortedBatch = errors.New("radix: batch objects must be sorted ascending and unique")
	ErrBatchRange    = errors.New("radix: batch contains key <= StartFrom")
)
