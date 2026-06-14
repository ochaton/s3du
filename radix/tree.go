package radix

import (
	"slices"
	"sort"
	"strings"
	"unsafe"
)

// internal is a radix-tree branch node — it has children and, optionally,
// a per-subtree Aggregate cache. A subset of internals also carry a file
// pointer for the rare case of an S3 directory-marker object whose key lies
// at the exact boundary of this node (107 out of 24 M internals on the
// 50 M-object <bucket> bucket).
//
//	edge:     compressed path fragment from the parent node (empty only on root).
//	children: child IDs, each tagged with leafTag if it points into the leaf arena.
//	          Sorted ascending by the first byte of each child's edge; Patricia
//	          invariant guarantees uniqueness among siblings.
//	agg:      aggregate over the subtree, materialised on first contribution.
//	file:     non-nil for S3 dir-marker objects pinned at this node.
type internal struct {
	edgeOff  uint32
	edgeLen  uint32
	children []uint32
	agg      *Aggregate
}

// leaf is a terminal radix-tree node — a single S3 object's metadata.
// sizeClass packs Size into bits 0..55 (max ~72 PB; S3 single-object cap is
// 5 TB) and StorageClass into bits 56..63. There is no children slice and no
// stored aggregate: the leaf's contribution to any ancestor aggregate is
// derived on demand from (class, size).
type leaf struct {
	edgeOff   uint32
	edgeLen   uint32
	sizeClass uint64
}

// Child IDs in internal.children are tagged uint32s: high bit set means the
// low 31 bits index into the leaf arena, otherwise into the internal arena.
// 2^31 nodes per arena is more than enough at any practical scale (1 G files
// at ~100 B/object on a single 100 GiB host).
const (
	leafTag uint32 = 1 << 31
	idMask  uint32 = leafTag - 1
)

// isLeafID reports whether cid points into the leaf arena.
func isLeafID(cid uint32) bool { return cid&leafTag != 0 }

const (
	leafClassShift          = 56
	leafSizeMask   uint64   = (1 << leafClassShift) - 1
)

func packSizeClass(class StorageClass, size int64) uint64 {
	return uint64(size)&leafSizeMask | uint64(class)<<leafClassShift
}

func (l *leaf) class() StorageClass { return StorageClass(l.sizeClass >> leafClassShift) }
func (l *leaf) size() int64         { return int64(l.sizeClass & leafSizeMask) }

// Edge arena geometry: 16 MB chunks, addressed by a 32-bit (chunkIdx, within)
// composite. Total addressable edge bytes = 256 chunks × 16 MB = 4 GiB, which
// exceeds the largest observed bucket-wide edge total (1.53 GiB).
//
// Allocated edges are append-only and never freed individually — small leaks
// from delete + merge operations are tolerated. After tens of millions of
// edits the arena can be compacted by re-allocating via export/import (out
// of scope for this iteration).
const (
	edgeChunkBits = 24
	edgeChunkSize = 1 << edgeChunkBits
	edgeChunkMask = edgeChunkSize - 1
)

// internalAgg returns the aggregate over the subtree rooted at the internal
// with the given untagged ID. When no Aggregate has been materialised the
// value is derived from the dir-marker file (if any) so freshly-created
// empty internals stay zero-cost.
func (t *Tree) internalAgg(id uint32) Aggregate {
	n := t.atInternal(id)
	if n.agg != nil {
		return *n.agg
	}
	if cb, ok := t.dirMarker(id); ok {
		a := Aggregate{Objects: 1}
		a.Bytes.Add(cb.Class, cb.Size)
		return a
	}
	return Aggregate{}
}

// leafAggregate returns the Aggregate a leaf contributes to its ancestors.
func (l *leaf) effectiveAgg() Aggregate {
	a := Aggregate{Objects: 1}
	a.Bytes.Add(l.class(), l.size())
	return a
}

// RootAggregate returns the aggregate over the whole tree (the radix root's
// aggregate). O(1) — the aggregate is maintained incrementally on every
// insert. Callers that just need the total object count or the per-class
// bytes should use this instead of walking Export.
func (t *Tree) RootAggregate() Aggregate {
	return t.internalAgg(rootID)
}

// Tree is an in-memory compressed radix tree over S3 object keys.
//
// Branch nodes (with children and per-subtree aggregates) and terminal
// leaves (single objects with no children) live in two separate chunked
// arenas. Each chunk is a fixed-size slab whose backing array never moves,
// so any pointer obtained from [Tree.atInternal] / [Tree.atLeaf] stays valid
// for the lifetime of the Tree. Children reference one another by uint32 ID:
// the high bit (leafTag) selects the arena and the low 31 bits index into it.
//
// The root is an internal at ID 0 and is never released. A freelist per arena
// reuses slots vacated by deletes.
//
// Concurrency: not safe for concurrent use. Callers must externally
// serialise AddBatch / ListDirectory calls.
type Tree struct {
	internals [][]internal
	nextInt   uint32
	freeInt   []uint32

	leaves   [][]leaf
	nextLeaf uint32
	freeLeaf []uint32

	// edgeArena holds the contiguous backing bytes for every node's edge.
	// Each chunk is exactly edgeChunkSize bytes once allocated. Edges are
	// addressed by a 32-bit value: (chunkIdx << edgeChunkBits) | within.
	edgeArena     [][]byte
	edgeWithin    uint32 // bytes written into the current (last) chunk
	edgeChunkBase uint32 // (chunkIdx << edgeChunkBits) of the current chunk

	// dirMarkers holds the (class, size) of S3 directory-marker objects
	// pinned at internal nodes. Sparse: typically 0–1000 entries even on
	// 50 M-object buckets. Keyed by internal arena ID.
	dirMarkers map[uint32]ClassByte
}

// dirMarker returns the dir-marker file pinned at the internal with the
// given untagged ID, if any.
func (t *Tree) dirMarker(id uint32) (ClassByte, bool) {
	cb, ok := t.dirMarkers[id]
	return cb, ok
}

// setDirMarker installs (or replaces) the dir-marker file at id.
func (t *Tree) setDirMarker(id uint32, cb ClassByte) {
	if t.dirMarkers == nil {
		t.dirMarkers = make(map[uint32]ClassByte)
	}
	t.dirMarkers[id] = cb
}

// clearDirMarker removes the dir-marker file at id, if present.
func (t *Tree) clearDirMarker(id uint32) {
	delete(t.dirMarkers, id)
}

// nodeChunkSize / chunkBits define the chunked arena geometry. Each chunk
// is a fixed-size slab; once allocated, the backing array never moves, which
// keeps pointers stable across [Tree.allocInternal] / [Tree.allocLeaf] calls.
// chunkBits is the binary log so that index decomposition uses shift + mask
// instead of a divide.
const (
	chunkBits     = 12
	nodeChunkSize = 1 << chunkBits
	chunkMask     = nodeChunkSize - 1
)

// rootID is the internal-arena ID reserved for the tree's root node.
const rootID uint32 = 0

// New returns an empty Tree with the root internal already allocated.
func New() *Tree {
	t := &Tree{}
	t.allocInternal(internal{}) // rootID at ID 0
	return t
}

// atInternal returns a pointer to the internal node with the given untagged ID.
func (t *Tree) atInternal(id uint32) *internal {
	return &t.internals[id>>chunkBits][id&chunkMask]
}

// atLeaf returns a pointer to the leaf with the given untagged ID.
func (t *Tree) atLeaf(id uint32) *leaf {
	return &t.leaves[id>>chunkBits][id&chunkMask]
}

// allocInternal stores init in the next free internal slot and returns its
// untagged ID. Children references must NOT have leafTag set when pointing
// at internal IDs.
func (t *Tree) allocInternal(init internal) uint32 {
	if n := len(t.freeInt); n > 0 {
		id := t.freeInt[n-1]
		t.freeInt = t.freeInt[:n-1]
		*t.atInternal(id) = init
		return id
	}
	if int(t.nextInt>>chunkBits) >= len(t.internals) {
		t.internals = append(t.internals, make([]internal, nodeChunkSize))
	}
	id := t.nextInt
	t.nextInt++
	*t.atInternal(id) = init
	return id
}

// allocLeaf stores init in the next free leaf slot and returns its TAGGED ID
// (high bit set). Callers can use the returned value as a child reference
// directly.
func (t *Tree) allocLeaf(init leaf) uint32 {
	if n := len(t.freeLeaf); n > 0 {
		id := t.freeLeaf[n-1]
		t.freeLeaf = t.freeLeaf[:n-1]
		*t.atLeaf(id) = init
		return id | leafTag
	}
	if int(t.nextLeaf>>chunkBits) >= len(t.leaves) {
		t.leaves = append(t.leaves, make([]leaf, nodeChunkSize))
	}
	id := t.nextLeaf
	t.nextLeaf++
	*t.atLeaf(id) = init
	return id | leafTag
}

// releaseInternal clears the internal slot at id and pushes the ID onto the
// freelist. Drops references so the GC can reclaim the edge string, agg, and
// children backings the node held.
func (t *Tree) releaseInternal(id uint32) {
	*t.atInternal(id) = internal{}
	t.freeInt = append(t.freeInt, id)
}

// releaseLeaf clears the leaf slot at the given untagged ID and pushes it on
// the leaf freelist.
func (t *Tree) releaseLeaf(id uint32) {
	*t.atLeaf(id) = leaf{}
	t.freeLeaf = append(t.freeLeaf, id)
}

// releaseChild releases whichever arena the tagged child ID points into.
func (t *Tree) releaseChild(cid uint32) {
	if isLeafID(cid) {
		t.releaseLeaf(cid & idMask)
	} else {
		t.releaseInternal(cid)
	}
}

// allocEdge copies the bytes of s into the edge arena and returns the
// (offset, length) pair that addresses them. Empty edges return (0, 0)
// without consuming arena space.
func (t *Tree) allocEdge(s string) (uint32, uint32) {
	n := len(s)
	if n == 0 {
		return 0, 0
	}
	if n > edgeChunkSize {
		panic("radix: edge larger than edgeChunkSize")
	}
	if len(t.edgeArena) == 0 || t.edgeWithin+uint32(n) > edgeChunkSize {
		// Need a new chunk.
		chunkIdx := uint32(len(t.edgeArena))
		t.edgeArena = append(t.edgeArena, make([]byte, edgeChunkSize))
		t.edgeChunkBase = chunkIdx << edgeChunkBits
		t.edgeWithin = 0
	}
	off := t.edgeChunkBase | t.edgeWithin
	copy(t.edgeArena[len(t.edgeArena)-1][t.edgeWithin:t.edgeWithin+uint32(n)], s)
	t.edgeWithin += uint32(n)
	return off, uint32(n)
}

// edgeStr returns a string view of the (off, length) edge bytes. The string
// shares its backing with the arena — no copy. Safe because the arena is
// append-only and never resized after allocation.
func (t *Tree) edgeStr(off, length uint32) string {
	if length == 0 {
		return ""
	}
	chunk := off >> edgeChunkBits
	within := off & edgeChunkMask
	return unsafe.String(&t.edgeArena[chunk][within], length)
}

// firstByteOf returns the first byte of the edge stored at the child cid.
// Used by binary search over children, which sorts on this byte.
func (t *Tree) firstByteOf(cid uint32) byte {
	if isLeafID(cid) {
		l := t.atLeaf(cid & idMask)
		return t.edgeArena[l.edgeOff>>edgeChunkBits][l.edgeOff&edgeChunkMask]
	}
	n := t.atInternal(cid)
	return t.edgeArena[n.edgeOff>>edgeChunkBits][n.edgeOff&edgeChunkMask]
}

// edgeOf returns the edge stored at the child cid.
func (t *Tree) edgeOf(cid uint32) string {
	if isLeafID(cid) {
		l := t.atLeaf(cid & idMask)
		return t.edgeStr(l.edgeOff, l.edgeLen)
	}
	n := t.atInternal(cid)
	return t.edgeStr(n.edgeOff, n.edgeLen)
}

// aggOfChild returns the Aggregate that the child cid contributes to its
// parent's subtree total.
func (t *Tree) aggOfChild(cid uint32) Aggregate {
	if isLeafID(cid) {
		return t.atLeaf(cid & idMask).effectiveAgg()
	}
	return t.internalAgg(cid)
}

// AddBatch ingests a contiguous range of objects. See [Batch] for the
// range-ownership contract. Behavior:
//   - keys in the existing tree in (StartFrom, Objects[last].Key] but not in
//     Objects are deleted.
//   - keys present in both with differing size/class are updated.
//   - keys only in Objects are inserted.
//
// An empty Objects slice is a no-op (range deletion via an empty batch is not
// supported yet).
//
// Implementation: a rangeCursor streams existing leaves in (StartFrom, hi]
// lock-step against the incoming batch via a three-way merge. The merge does
// not mutate the tree directly; it accumulates conflict-free insert/update/
// delete groups. Pure-insert batches (the dominant case during first-scan
// import) skip allocation entirely by handing the input slice straight to
// [Tree.insertBatch].
func (t *Tree) AddBatch(b Batch) error {
	if len(b.Objects) == 0 {
		return nil
	}
	if err := validateBatch(b); err != nil {
		return err
	}
	hi := b.Objects[len(b.Objects)-1].Key

	cur := t.newRangeCursor(b.StartFrom, hi)
	existing, hasExisting := cur.Next()
	if !hasExisting {
		t.insertBatch(b.Objects)
		return nil
	}

	var (
		inserts []Object
		updates []Object
		deletes []string
	)
	j := 0
	for hasExisting || j < len(b.Objects) {
		switch {
		case j >= len(b.Objects):
			deletes = append(deletes, existing.Key)
			existing, hasExisting = cur.Next()
		case !hasExisting:
			inserts = append(inserts, b.Objects[j])
			j++
		case existing.Key < b.Objects[j].Key:
			deletes = append(deletes, existing.Key)
			existing, hasExisting = cur.Next()
		case existing.Key > b.Objects[j].Key:
			inserts = append(inserts, b.Objects[j])
			j++
		default:
			if existing.Size != b.Objects[j].Size || existing.Class != b.Objects[j].Class {
				updates = append(updates, b.Objects[j])
			}
			existing, hasExisting = cur.Next()
			j++
		}
	}
	t.deleteBatch(deletes)
	t.updateBatch(updates)
	t.insertBatch(inserts)
	return nil
}

func validateBatch(b Batch) error {
	for i := range b.Objects {
		if b.Objects[i].Key <= b.StartFrom {
			return ErrBatchRange
		}
		if i > 0 && b.Objects[i].Key <= b.Objects[i-1].Key {
			return ErrUnsortedBatch
		}
	}
	return nil
}

// ListDirectory returns the immediate children of the logical directory
// identified by prefix. The prefix must be either "" (root) or end with '/'.
// Entries are sorted lexicographically.
func (t *Tree) ListDirectory(prefix string) ([]Entry, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, ErrInvalidPrefix
	}

	id := rootID
	rem := prefix
	for {
		n := t.atInternal(id)
		if rem == "" {
			return t.listAtInternal(id), nil
		}
		idx, ok := t.findChild(n.children, rem[0])
		if !ok {
			return nil, nil
		}
		childCID := n.children[idx]
		childEdge := t.edgeOf(childCID)
		if len(rem) >= len(childEdge) && rem[:len(childEdge)] == childEdge {
			if isLeafID(childCID) {
				// rem extends past a leaf's edge: prefix matches more bytes
				// than the leaf has; nothing lives below a leaf.
				if len(rem) > len(childEdge) {
					return nil, nil
				}
				return t.listAtLeaf(t.atLeaf(childCID&idMask)), nil
			}
			id = childCID
			rem = rem[len(childEdge):]
			continue
		}
		if len(rem) < len(childEdge) && childEdge[:len(rem)] == rem {
			// prefix ends inside this compressed edge: single chain — emit at most
			// one entry (either a subdirectory whose name extends to the next '/'
			// in the edge, or a file if no '/' remains).
			return t.listInsideEdge(childCID, rem), nil
		}
		return nil, nil
	}
}

// listAtInternal lists entries reachable from the internal at id without
// crossing '/'. When the requested prefix lands exactly on the node
// boundary, the dir-marker (if any) is emitted with an empty name so
// callers may distinguish or filter it.
func (t *Tree) listAtInternal(id uint32) []Entry {
	n := t.atInternal(id)
	out := make([]Entry, 0, len(n.children)+1)
	if cb, ok := t.dirMarker(id); ok {
		out = append(out, Entry{
			Name:  "",
			IsDir: false,
			Class: cb.Class,
			Size:  cb.Size,
		})
	}
	for _, cid := range n.children {
		out = t.emitFromChild(cid, "", out)
	}
	return out
}

// listAtLeaf handles the edge case where the requested prefix lands exactly
// on a leaf boundary — only the leaf's file itself is reachable.
func (t *Tree) listAtLeaf(l *leaf) []Entry {
	return []Entry{{Name: "", IsDir: false, Class: l.class(), Size: l.size()}}
}

// listInsideEdge handles the case where the prefix lands strictly inside a
// compressed edge. The residual edge bytes after the prefix form the suffix
// that all entries in this logical directory share.
func (t *Tree) listInsideEdge(cid uint32, consumed string) []Entry {
	edge := t.edgeOf(cid)
	residual := edge[len(consumed):]
	if i := strings.IndexByte(residual, '/'); i >= 0 {
		return []Entry{{
			Name:      residual[:i+1],
			IsDir:     true,
			Aggregate: t.aggOfChild(cid),
		}}
	}
	if isLeafID(cid) {
		l := t.atLeaf(cid & idMask)
		return []Entry{{Name: residual, IsDir: false, Class: l.class(), Size: l.size()}}
	}
	n := t.atInternal(cid)
	out := make([]Entry, 0, len(n.children)+1)
	if cb, ok := t.dirMarker(cid); ok {
		out = append(out, Entry{
			Name:  residual,
			IsDir: false,
			Class: cb.Class,
			Size:  cb.Size,
		})
	}
	for _, c := range n.children {
		out = t.emitFromChild(c, residual, out)
	}
	return out
}

// emitFromChild appends all logical-dir entries reachable from the child cid
// without crossing a '/' to out and returns the resulting slice. acc is the
// bytes accumulated from the parent boundary node down to (but not including)
// the child's edge.
func (t *Tree) emitFromChild(cid uint32, acc string, out []Entry) []Entry {
	edge := t.edgeOf(cid)
	if i := strings.IndexByte(edge, '/'); i >= 0 {
		return append(out, Entry{
			Name:      acc + edge[:i+1],
			IsDir:     true,
			Aggregate: t.aggOfChild(cid),
		})
	}
	fullName := acc + edge
	if isLeafID(cid) {
		l := t.atLeaf(cid & idMask)
		return append(out, Entry{
			Name:  fullName,
			IsDir: false,
			Class: l.class(),
			Size:  l.size(),
		})
	}
	n := t.atInternal(cid)
	if cb, ok := t.dirMarker(cid); ok {
		out = append(out, Entry{
			Name:  fullName,
			IsDir: false,
			Class: cb.Class,
			Size:  cb.Size,
		})
	}
	for _, c := range n.children {
		out = t.emitFromChild(c, fullName, out)
	}
	return out
}

// findChild binary-searches children for a child whose edge starts with b.
// Returns (index, found). When found is false, index is the insertion point.
func (t *Tree) findChild(children []uint32, b byte) (int, bool) {
	i := sort.Search(len(children), func(i int) bool {
		return t.firstByteOf(children[i]) >= b
	})
	if i < len(children) && t.firstByteOf(children[i]) == b {
		return i, true
	}
	return i, false
}

// insertChildAt inserts id into children at index idx, preserving order.
func insertChildAt(children []uint32, idx int, id uint32) []uint32 {
	children = append(children, 0)
	copy(children[idx+1:], children[idx:])
	children[idx] = id
	return children
}

// longestCommonPrefix returns the length of the longest common byte prefix.
func longestCommonPrefix(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// framePath is one entry in a descent stack. consumed counts how many bytes
// of the active key have been "covered" by the path from root to and
// including this node's edge — used by [Tree.insertBatch]'s LCP fast-path
// to compare against the next key without re-descending from the root.
type framePath struct {
	id       uint32
	consumed int
}

// insertBatch inserts a sorted, conflict-free slice of objects, reusing a
// descent path between consecutive inserts so shared key prefixes are walked
// only once.
//
// The idea: a Patricia tree's ancestors of any key are exactly the nodes
// whose edges concatenate to a prefix of that key. If two consecutive sorted
// keys share K bytes (their longest common prefix), the first K bytes of
// their descent paths are identical too. So after inserting the previous
// key, the path stack already points down into the tree to where the new
// key still agrees; we only have to pop the frames that lie *below* the
// divergence and continue from there.
//
// path[0] is always the root frame so popping cannot leave the stack empty.
// path[i].consumed is the byte-position of frame i's edge end in the
// previous key — comparing it to lcp(prev, new) tells us how deep the new
// key still belongs.
func (t *Tree) insertBatch(objs []Object) {
	path := make([]framePath, 1, 16)
	path[0] = framePath{id: rootID, consumed: 0}
	prevKey := ""
	for i := range objs {
		obj := objs[i]
		lcp := longestCommonPrefix(prevKey, obj.Key)
		for len(path) > 1 && path[len(path)-1].consumed > lcp {
			path = path[:len(path)-1]
		}
		path = t.insertFromPath(path, obj)
		prevKey = obj.Key
	}
}

// insertFromPath resumes the descent of obj.Key from the top of path. It
// mutates the tree as needed (creating leaves, splitting compressed edges)
// and extends path with each visited internal node. The returned path
// terminates at obj's parent so the caller can reuse it for the next insert.
//
// path frames hold INTERNAL IDs only — leaves can have no descendants, so
// they never appear on a descent path. The bookkeeping invariant is: every
// frame in path is an internal ancestor whose Aggregate must reflect obj.
//
// Because both arenas are chunked, *internal and *leaf pointers stay valid
// across allocation calls — no refresh dance needed.
func (t *Tree) insertFromPath(path []framePath, obj Object) []framePath {
	for _, f := range path {
		t.bumpAgg(f.id, obj)
	}

	id := path[len(path)-1].id
	consumed := path[len(path)-1].consumed
	key := obj.Key[consumed:]

	for {
		n := t.atInternal(id)
		if key == "" {
			// key terminates exactly at internal n: it becomes (or remains)
			// a directory-marker node. n.agg was already bumped above.
			t.setDirMarker(id, ClassByte{Class: obj.Class, Size: obj.Size})
			return path
		}

		idx, ok := t.findChild(n.children, key[0])
		if !ok {
			// No sibling shares the next byte — attach the remainder of
			// the key as a brand-new leaf in the leaf arena. The edge
			// arena owns the bytes, so the caller's batch key string is
			// not pinned for the lifetime of the tree.
			lOff, lLen := t.allocEdge(key)
			leafCID := t.allocLeaf(leaf{
				edgeOff:   lOff,
				edgeLen:   lLen,
				sizeClass: packSizeClass(obj.Class, obj.Size),
			})
			t.atInternal(id).children = insertChildAt(t.atInternal(id).children, idx, leafCID)
			return path
		}

		childCID := n.children[idx]
		if isLeafID(childCID) {
			return t.insertAgainstLeafChild(path, id, idx, childCID, obj, key, consumed)
		}
		childID := childCID
		child := t.atInternal(childID)
		childEdge := t.edgeStr(child.edgeOff, child.edgeLen)
		lcp := longestCommonPrefix(childEdge, key)
		if lcp == len(childEdge) {
			// The whole edge is a prefix of the remaining key — descend
			// into the internal child and continue one level deeper.
			t.bumpAgg(childID, obj)
			id = childID
			consumed += lcp
			key = key[lcp:]
			path = append(path, framePath{id: childID, consumed: consumed})
			continue
		}

		// Internal child's edge and the new key diverge after lcp bytes.
		// Build an intermediate internal carrying the shared prefix (a
		// pointer into the existing edge bytes — no copy), trim the
		// existing child to the remainder by shifting its (off, len), and
		// place obj either at the intermediate (if the new key ends at
		// lcp) or as a brand-new sibling leaf.
		intAgg := t.aggForSplitInternalChild(childID, obj)
		intID := t.allocInternal(internal{
			edgeOff: child.edgeOff,
			edgeLen: uint32(lcp),
			agg:     intAgg,
		})
		t.atInternal(childID).edgeOff += uint32(lcp)
		t.atInternal(childID).edgeLen -= uint32(lcp)

		if lcp == len(key) {
			inter := t.atInternal(intID)
			t.setDirMarker(intID, ClassByte{Class: obj.Class, Size: obj.Size})
			inter.children = []uint32{childCID}
			t.atInternal(id).children[idx] = intID
			return append(path, framePath{id: intID, consumed: consumed + lcp})
		}

		lOff, lLen := t.allocEdge(key[lcp:])
		leafCID := t.allocLeaf(leaf{
			edgeOff:   lOff,
			edgeLen:   lLen,
			sizeClass: packSizeClass(obj.Class, obj.Size),
		})
		t.atInternal(intID).children = t.sortChildren(childCID, leafCID)
		t.atInternal(id).children[idx] = intID
		return append(path, framePath{id: intID, consumed: consumed + lcp})
	}
}

// insertAgainstLeafChild handles the descent step when the next child is a
// leaf. It dispatches between three shapes:
//
//   - leaf and key diverge after lcp: new intermediate at lcp, leaf becomes
//     child with trimmed edge, new leaf for key remainder.
//   - leaf is exact prefix of key: convert leaf to internal-with-file (the
//     internal carries the leaf's edge and its old class/size as a dir-marker)
//     and attach a brand-new leaf for the key remainder.
//   - exact match: caller invariant (no duplicates) violated; overwrite
//     class/size as a best-effort recovery.
func (t *Tree) insertAgainstLeafChild(path []framePath, parentID uint32, childSlot int, childCID uint32, obj Object, key string, consumed int) []framePath {
	leafID := childCID & idMask
	lf := t.atLeaf(leafID)
	lfEdge := t.edgeStr(lf.edgeOff, lf.edgeLen)
	lcp := longestCommonPrefix(lfEdge, key)

	if lcp == len(lfEdge) && lcp == len(key) {
		// Exact match — caller should have filtered as an update, not an insert.
		// Overwrite class/size; ancestor aggs are now slightly inconsistent
		// (the bumpAgg pre-walk added a fresh contribution). This branch matches
		// the legacy behaviour for the internal-with-file overwrite case.
		lf.sizeClass = packSizeClass(obj.Class, obj.Size)
		return path
	}

	if lcp == len(lfEdge) {
		// Leaf's edge is a strict prefix of key: convert leaf into an
		// internal-with-file and attach a new leaf for the key suffix.
		oldClass := lf.class()
		oldSize := lf.size()
		oldOff := lf.edgeOff
		oldLen := lf.edgeLen

		var ag Aggregate
		ag.Objects = 2
		ag.Bytes.Add(oldClass, oldSize)
		ag.Bytes.Add(obj.Class, obj.Size)
		intID := t.allocInternal(internal{
			edgeOff: oldOff,
			edgeLen: oldLen,
			agg:     &ag,
		})
		t.setDirMarker(intID, ClassByte{Class: oldClass, Size: oldSize})
		t.releaseLeaf(leafID)

		nOff, nLen := t.allocEdge(key[lcp:])
		newLeafCID := t.allocLeaf(leaf{
			edgeOff:   nOff,
			edgeLen:   nLen,
			sizeClass: packSizeClass(obj.Class, obj.Size),
		})
		t.atInternal(intID).children = []uint32{newLeafCID}
		t.atInternal(parentID).children[childSlot] = intID
		return append(path, framePath{id: intID, consumed: consumed + lcp})
	}

	// lcp < len(lfEdge): leaf and key diverge after lcp.
	// New intermediate internal carries the lcp-prefix view of the leaf's
	// existing arena bytes; the existing leaf advances its (off, len) by
	// lcp and stays a leaf.
	var ag Aggregate
	ag.Objects = 2
	ag.Bytes.Add(lf.class(), lf.size())
	ag.Bytes.Add(obj.Class, obj.Size)
	intID := t.allocInternal(internal{
		edgeOff: lf.edgeOff,
		edgeLen: uint32(lcp),
		agg:     &ag,
	})

	if lcp == len(key) {
		// key ends exactly at the new internal — it picks up obj as a
		// dir-marker. The existing leaf gets trimmed and lives on as the
		// sole child.
		lf.edgeOff += uint32(lcp)
		lf.edgeLen -= uint32(lcp)
		inter := t.atInternal(intID)
		t.setDirMarker(intID, ClassByte{Class: obj.Class, Size: obj.Size})
		inter.children = []uint32{childCID}
		t.atInternal(parentID).children[childSlot] = intID
		return append(path, framePath{id: intID, consumed: consumed + lcp})
	}

	lf.edgeOff += uint32(lcp)
	lf.edgeLen -= uint32(lcp)
	nOff, nLen := t.allocEdge(key[lcp:])
	newLeafCID := t.allocLeaf(leaf{
		edgeOff:   nOff,
		edgeLen:   nLen,
		sizeClass: packSizeClass(obj.Class, obj.Size),
	})
	t.atInternal(intID).children = t.sortChildren(childCID, newLeafCID)
	t.atInternal(parentID).children[childSlot] = intID
	return append(path, framePath{id: intID, consumed: consumed + lcp})
}

// bumpAgg adds obj's contribution to the aggregate of the internal at id.
// If the internal has no materialised aggregate yet, one is allocated from
// its effective view (file alone, or zero).
func (t *Tree) bumpAgg(id uint32, obj Object) {
	n := t.atInternal(id)
	if n.agg == nil {
		a := t.internalAgg(id)
		n.agg = &a
	}
	n.agg.Objects++
	n.agg.Bytes.Add(obj.Class, obj.Size)
}

// aggForSplitInternalChild builds the aggregate that the new intermediate
// produced by splitting an existing internal child should carry. It is the
// child's existing aggregate plus one new object from obj.
//
// child.agg.Bytes cannot be aliased — the in-place Add below would corrupt
// child.agg.Bytes via the shared backing.
func (t *Tree) aggForSplitInternalChild(childID uint32, obj Object) *Aggregate {
	child := t.atInternal(childID)
	var a Aggregate
	if child.agg != nil {
		a.Objects = child.agg.Objects + 1
		if k := len(child.agg.Bytes); k > 0 {
			a.Bytes = append(make(ClassBytes, 0, k+1), child.agg.Bytes...)
		}
	} else {
		a = t.internalAgg(childID)
		a.Objects++
	}
	a.Bytes.Add(obj.Class, obj.Size)
	return &a
}

// sortChildren returns a 2-element child slice sorted ascending by the first
// byte of each child's edge. Either argument may be tagged or untagged.
func (t *Tree) sortChildren(a, b uint32) []uint32 {
	if t.firstByteOf(a) < t.firstByteOf(b) {
		return []uint32{a, b}
	}
	return []uint32{b, a}
}

// deletePop is one entry in the descent record used by deleteKey to drive
// bottom-up compression after a key is removed.
type deletePop struct {
	parentID uint32
	childIdx int
}

// deleteKey removes key from the tree and propagates the negative aggregate
// delta. Empty branches are pruned and single-child compression is restored.
// Pruned arena slots return to their respective freelists.
func (t *Tree) deleteKey(key string) {
	pops := make([]deletePop, 0, 8)
	path := make([]framePath, 1, 8)
	path[0] = framePath{id: rootID, consumed: 0}
	curID := rootID
	rem := key
	consumed := 0

	for rem != "" {
		cur := t.atInternal(curID)
		idx, ok := t.findChild(cur.children, rem[0])
		if !ok {
			return
		}
		childCID := cur.children[idx]
		childEdge := t.edgeOf(childCID)
		if !strings.HasPrefix(rem, childEdge) {
			return
		}
		consumed += len(childEdge)
		rem = rem[len(childEdge):]
		if isLeafID(childCID) {
			// Terminal: leaf is the file we want to delete. Must match the
			// full key — anything remaining means the key isn't in the tree.
			if rem != "" {
				return
			}
			lf := t.atLeaf(childCID & idMask)
			class, size := lf.class(), lf.size()
			t.applyDelta(path, class, -size, -1)
			// Detach from parent and release the leaf slot.
			cur.children = append(cur.children[:idx], cur.children[idx+1:]...)
			t.releaseLeaf(childCID & idMask)
			t.compressAfterDelete(pops)
			return
		}
		pops = append(pops, deletePop{parentID: curID, childIdx: idx})
		curID = childCID
		path = append(path, framePath{id: curID, consumed: consumed})
	}

	// rem == "" terminated at an internal node — the file we want is the
	// dir-marker at curID (if any).
	cb, ok := t.dirMarker(curID)
	if !ok {
		return
	}
	t.applyDelta(path, cb.Class, -cb.Size, -1)
	t.clearDirMarker(curID)
	t.compressAfterDelete(pops)
}

// compressAfterDelete walks the parents recorded on the descent and prunes /
// merges single-child no-file internals to restore radix compression.
func (t *Tree) compressAfterDelete(pops []deletePop) {
	for i := len(pops) - 1; i >= 0; i-- {
		fr := pops[i]
		parent := t.atInternal(fr.parentID)
		childCID := parent.children[fr.childIdx]
		// Leaf children cannot become unreferenced via compression — they
		// only disappear via direct deletion. Stop the compression walk.
		if isLeafID(childCID) {
			return
		}
		child := t.atInternal(childCID)
		_, hasMarker := t.dirMarker(childCID)
		switch {
		case !hasMarker && len(child.children) == 0:
			parent.children = append(parent.children[:fr.childIdx], parent.children[fr.childIdx+1:]...)
			t.releaseInternal(childCID)
		case !hasMarker && len(child.children) == 1:
			onlyCID := child.children[0]
			merged := t.edgeStr(child.edgeOff, child.edgeLen) + t.edgeOf(onlyCID)
			mOff, mLen := t.allocEdge(merged)
			if isLeafID(onlyCID) {
				only := t.atLeaf(onlyCID & idMask)
				only.edgeOff = mOff
				only.edgeLen = mLen
			} else {
				only := t.atInternal(onlyCID)
				only.edgeOff = mOff
				only.edgeLen = mLen
			}
			parent.children[fr.childIdx] = onlyCID
			t.releaseInternal(childCID)
		default:
			return
		}
	}
}

// updateKey changes the metadata for an existing key and adjusts aggregates
// by the difference. Assumes the key currently exists (caller filters).
func (t *Tree) updateKey(obj Object) {
	path := make([]framePath, 1, 8)
	path[0] = framePath{id: rootID, consumed: 0}
	curID := rootID
	rem := obj.Key
	consumed := 0
	for rem != "" {
		cur := t.atInternal(curID)
		idx, ok := t.findChild(cur.children, rem[0])
		if !ok {
			return
		}
		childCID := cur.children[idx]
		childEdge := t.edgeOf(childCID)
		if !strings.HasPrefix(rem, childEdge) {
			return
		}
		consumed += len(childEdge)
		rem = rem[len(childEdge):]
		if isLeafID(childCID) {
			if rem != "" {
				return
			}
			lf := t.atLeaf(childCID & idMask)
			oldClass, oldSize := lf.class(), lf.size()
			if oldClass == obj.Class {
				if d := obj.Size - oldSize; d != 0 {
					t.applyDelta(path, obj.Class, d, 0)
				}
				lf.sizeClass = packSizeClass(obj.Class, obj.Size)
				return
			}
			t.applyDelta(path, oldClass, -oldSize, -1)
			lf.sizeClass = packSizeClass(obj.Class, obj.Size)
			t.applyDelta(path, obj.Class, obj.Size, +1)
			return
		}
		curID = childCID
		path = append(path, framePath{id: curID, consumed: consumed})
	}
	cb, ok := t.dirMarker(curID)
	if !ok {
		return
	}
	oldClass, oldSize := cb.Class, cb.Size
	if oldClass == obj.Class {
		if d := obj.Size - oldSize; d != 0 {
			t.applyDelta(path, obj.Class, d, 0)
		}
		t.setDirMarker(curID, ClassByte{Class: obj.Class, Size: obj.Size})
		return
	}
	t.applyDelta(path, oldClass, -oldSize, -1)
	t.setDirMarker(curID, ClassByte{Class: obj.Class, Size: obj.Size})
	t.applyDelta(path, obj.Class, obj.Size, +1)
}

// updateBatch applies updateKey to each object in the slice. Conflicts are
// assumed to be pre-resolved by the caller (all keys present in the tree).
func (t *Tree) updateBatch(objs []Object) {
	for i := range objs {
		t.updateKey(objs[i])
	}
}

// deleteBatch applies deleteKey to each key in the slice. Conflicts are
// assumed to be pre-resolved (all keys present).
func (t *Tree) deleteBatch(keys []string) {
	for _, k := range keys {
		t.deleteKey(k)
	}
}

// applyDelta walks the descent path applying the per-class byte delta and
// the object-count delta to each internal's aggregate. The path contains
// internal IDs only (leaves cannot have descendants and so never appear).
func (t *Tree) applyDelta(path []framePath, class StorageClass, sizeDelta, objDelta int64) {
	for i := range path {
		n := t.atInternal(path[i].id)
		if n.agg == nil {
			a := t.internalAgg(path[i].id)
			n.agg = &a
		}
		n.agg.Objects += objDelta
		if sizeDelta != 0 {
			n.agg.Bytes.Add(class, sizeDelta)
		}
	}
}

// rangeCursor is a stack-based iterator over leaves whose key lies in
// (lo, hi]. It allocates only the cursor itself and its stack slice (sized
// to tree depth) regardless of how many leaves the range contains. The
// caller drives iteration via repeated [rangeCursor.Next] calls; the cursor
// terminates either when the stack drains or when subtree pruning rules out
// the remaining tree.
//
// rangeCursor reads the tree only — mutations made by the caller between
// Next calls (e.g. AddBatch's downstream insertBatch/updateBatch/deleteBatch
// passes) are not safe to interleave with iteration. AddBatch arranges its
// callers so the cursor is fully drained before any mutation happens.
type rangeCursor struct {
	t      *Tree
	stack  []cursorFrame
	lo, hi string
}

type cursorFrame struct {
	id          uint32
	prefix      string // bytes from root including this node's edge
	nextChild   int    // index of next unvisited child
	fileEmitted bool
}

func (t *Tree) newRangeCursor(lo, hi string) *rangeCursor {
	rc := &rangeCursor{
		t:     t,
		lo:    lo,
		hi:    hi,
		stack: make([]cursorFrame, 1, 16),
	}
	rc.stack[0] = cursorFrame{id: rootID}
	return rc
}

// Next returns the next object whose key falls in (lo, hi], in ascending lex
// order. Returns (_, false) once the iteration is exhausted; subsequent calls
// continue to return false.
//
// Stack frames hold INTERNAL IDs only; leaf children are emitted inline as
// the descent visits each child slot without pushing a new frame, since
// leaves have no further descent.
func (rc *rangeCursor) Next() (Object, bool) {
	for len(rc.stack) > 0 {
		top := &rc.stack[len(rc.stack)-1]
		n := rc.t.atInternal(top.id)
		if !top.fileEmitted {
			top.fileEmitted = true
			if cb, ok := rc.t.dirMarker(top.id); ok && top.prefix > rc.lo && top.prefix <= rc.hi {
				return Object{Key: top.prefix, Size: cb.Size, Class: cb.Class}, true
			}
		}
		if top.nextChild >= len(n.children) {
			rc.stack = rc.stack[:len(rc.stack)-1]
			continue
		}
		cid := n.children[top.nextChild]
		top.nextChild++
		childEdge := rc.t.edgeOf(cid)
		childKey := top.prefix + childEdge
		// All subsequent siblings have a strictly greater first byte, hence
		// strictly greater childKey — once one exceeds hi, pop the frame.
		if childKey > rc.hi {
			rc.stack = rc.stack[:len(rc.stack)-1]
			continue
		}
		if isLeafID(cid) {
			if childKey > rc.lo && childKey <= rc.hi {
				lf := rc.t.atLeaf(cid & idMask)
				return Object{Key: childKey, Size: lf.size(), Class: lf.class()}, true
			}
			continue
		}
		if childKey < rc.lo && !strings.HasPrefix(rc.lo, childKey) {
			continue
		}
		rc.stack = append(rc.stack, cursorFrame{id: cid, prefix: childKey})
	}
	return Object{}, false
}

// TreeStats is a memory-accounting snapshot of the tree's in-arena state.
// All counters are derived by a single linear walk over the chunked arena;
// no allocations beyond the freelist copy.
//
// EstHeap* fields are best-effort: edge / children-slice / agg-bytes sizes
// are rounded up to the nearest Go small-object size class so the totals
// reflect what the allocator actually charged, not the raw byte count.
type TreeStats struct {
	AliveNodes        int64
	AliveInternals    int64 // count from the internal arena
	AliveLeaves       int64 // count from the leaf arena
	Leaves            int64 // alias of AliveLeaves (kept for output compatibility)
	Internals         int64 // internals with children, no dir-marker file
	InternalsWithFile int64 // internals carrying an S3 dir-marker file
	Empty             int64 // root or detached internals with no children and no file

	NodesWithAgg     int64
	ClassBytePtrs    int64 // == count of nodes with file != nil (each is one *ClassByte heap alloc)
	ClassBytesLen    int64 // sum of len(agg.Bytes)
	ClassBytesCap    int64 // sum of cap(agg.Bytes)
	EdgeBytes        int64 // sum of len(edge)
	ChildrenSlots    int64 // sum of cap(children)
	MaxEdgeLen       int
	MaxChildrenCount int

	EdgeLenHist  [9]int64 // 0,1,2,3-4,5-8,9-16,17-32,33-64,65+
	ChildrenHist [9]int64 // 0,1,2,3,4,5-7,8-15,16-31,32+

	EstHeapInternals  int64 // AliveInternals * sizeof(internal)
	EstHeapLeaves     int64 // AliveLeaves * sizeof(leaf)
	EstHeapNodes      int64 // EstHeapInternals + EstHeapLeaves
	EstHeapEdges      int64 // sum over edges of sizeClass(len(edge))
	EstHeapChildren   int64 // sum over internals of sizeClass(cap(children)*4)
	EstHeapClassBytes int64 // ClassBytePtrs * sizeClass(sizeof(ClassByte))
	EstHeapAggHeaders int64 // NodesWithAgg * sizeClass(sizeof(Aggregate))
	EstHeapAggBytes   int64 // sum over aggs of sizeClass(cap(Bytes)*sizeof(ClassByte))
	EstHeapTotal      int64
}

// Stats returns a TreeStats over every alive arena slot.
func (t *Tree) Stats() TreeStats {
	var s TreeStats
	internalSize := int(unsafe.Sizeof(internal{}))
	leafSize := int(unsafe.Sizeof(leaf{}))
	classByteSize := int(unsafe.Sizeof(ClassByte{}))
	aggSize := int(unsafe.Sizeof(Aggregate{}))

	// Internal arena walk.
	sortedFreeInt := append([]uint32(nil), t.freeInt...)
	slices.Sort(sortedFreeInt)
	fi := 0
	for id := uint32(0); id < t.nextInt; id++ {
		if fi < len(sortedFreeInt) && sortedFreeInt[fi] == id {
			fi++
			continue
		}
		n := t.atInternal(id)
		s.AliveNodes++
		s.AliveInternals++

		_, hasFile := t.dirMarker(id)
		nc := len(n.children)
		switch {
		case hasFile && nc > 0:
			s.InternalsWithFile++
		case !hasFile && nc > 0:
			s.Internals++
		default:
			s.Empty++ // pure internals with no children (root in empty tree etc.)
		}
		if hasFile {
			s.ClassBytePtrs++
		}
		if n.agg != nil {
			s.NodesWithAgg++
			s.ClassBytesLen += int64(len(n.agg.Bytes))
			s.ClassBytesCap += int64(cap(n.agg.Bytes))
			s.EstHeapAggBytes += int64(sizeClass(cap(n.agg.Bytes) * classByteSize))
		}
		el := int(n.edgeLen)
		s.EdgeBytes += int64(el)
		if el > s.MaxEdgeLen {
			s.MaxEdgeLen = el
		}
		s.EdgeLenHist[edgeLenBucket(el)]++

		cc := cap(n.children)
		s.ChildrenSlots += int64(cc)
		s.EstHeapChildren += int64(sizeClass(cc * 4))
		if nc > s.MaxChildrenCount {
			s.MaxChildrenCount = nc
		}
		s.ChildrenHist[childrenBucket(nc)]++
	}

	// Leaf arena walk.
	sortedFreeLeaf := append([]uint32(nil), t.freeLeaf...)
	slices.Sort(sortedFreeLeaf)
	fl := 0
	for id := uint32(0); id < t.nextLeaf; id++ {
		if fl < len(sortedFreeLeaf) && sortedFreeLeaf[fl] == id {
			fl++
			continue
		}
		l := t.atLeaf(id)
		s.AliveNodes++
		s.AliveLeaves++
		s.Leaves++

		el := int(l.edgeLen)
		s.EdgeBytes += int64(el)
		if el > s.MaxEdgeLen {
			s.MaxEdgeLen = el
		}
		s.EdgeLenHist[edgeLenBucket(el)]++
		s.ChildrenHist[0]++ // leaves have no children
	}

	s.EstHeapInternals = s.AliveInternals * int64(internalSize)
	s.EstHeapLeaves = s.AliveLeaves * int64(leafSize)
	s.EstHeapNodes = s.EstHeapInternals + s.EstHeapLeaves
	// Edges live in the arena: charged at full chunk granularity.
	s.EstHeapEdges = int64(len(t.edgeArena)) * edgeChunkSize
	s.EstHeapClassBytes = s.ClassBytePtrs * int64(sizeClass(classByteSize))
	s.EstHeapAggHeaders = s.NodesWithAgg * int64(sizeClass(aggSize))
	s.EstHeapTotal = s.EstHeapNodes + s.EstHeapEdges + s.EstHeapChildren +
		s.EstHeapClassBytes + s.EstHeapAggHeaders + s.EstHeapAggBytes
	return s
}

// sizeClass approximates the Go runtime's small-object size classes. The
// allocator rounds requests up to the nearest size class, so a 10-byte
// string actually occupies 16 bytes of heap. Beyond the table we round to
// the next power of two.
func sizeClass(n int) int {
	if n == 0 {
		return 0
	}
	classes := [...]int{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512}
	for _, c := range classes {
		if n <= c {
			return c
		}
	}
	// beyond 512 B: round up to nearest 64 B.
	return (n + 63) &^ 63
}

func edgeLenBucket(n int) int {
	switch {
	case n == 0:
		return 0
	case n == 1:
		return 1
	case n == 2:
		return 2
	case n <= 4:
		return 3
	case n <= 8:
		return 4
	case n <= 16:
		return 5
	case n <= 32:
		return 6
	case n <= 64:
		return 7
	default:
		return 8
	}
}

func childrenBucket(n int) int {
	switch {
	case n == 0:
		return 0
	case n == 1:
		return 1
	case n == 2:
		return 2
	case n == 3:
		return 3
	case n == 4:
		return 4
	case n <= 7:
		return 5
	case n <= 15:
		return 6
	case n <= 31:
		return 7
	default:
		return 8
	}
}
