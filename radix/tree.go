package radix

import (
	"sort"
	"strings"
)

// node is a single radix-tree node.
//
//	edge:     compressed path fragment from the parent node (empty only on root).
//	children: child node IDs (indices into Tree.nodes). Sorted ascending by
//	          the first byte of each child's edge; Patricia invariant
//	          guarantees uniqueness among siblings.
//	file:     non-nil iff a key terminates exactly at this node.
//	agg:      non-nil for nodes that have ever been internal; nil for pure
//	          leaves whose aggregate is derived on demand (see effectiveAgg).
type node struct {
	edge     string
	children []uint32
	file     *ClassByte
	agg      *Aggregate
}

// effectiveAgg returns the aggregate over the subtree rooted at n. Pure leaves
// (those whose agg pointer was never materialised) have their contribution
// derived on the fly from file to keep their in-memory footprint small.
func (n *node) effectiveAgg() Aggregate {
	if n.agg != nil {
		return *n.agg
	}
	if n.file == nil {
		return Aggregate{}
	}
	a := Aggregate{Objects: 1}
	a.Bytes.Add(n.file.Class, n.file.Size)
	return a
}

// RootAggregate returns the aggregate over the whole tree (the radix root's
// aggregate). O(1) — the aggregate is maintained incrementally on every
// insert. Callers that just need the total object count or the per-class
// bytes should use this instead of walking Export.
func (t *Tree) RootAggregate() Aggregate {
	return t.at(rootID).effectiveAgg()
}

// Tree is an in-memory compressed radix tree over S3 object keys.
//
// Nodes live in a chunked arena (Tree.chunks). Each chunk is a fixed-size
// []node whose backing array never moves, so any *node obtained from
// [Tree.at] is stable for the lifetime of the Tree even as more nodes are
// allocated. Children reference each other by uint32 ID (the linear index
// into the conceptual concatenation of all chunks): a child ID survives
// snapshot save/load unchanged because IDs are arena slot numbers, not
// memory addresses. A freelist of released IDs is reused by [Tree.alloc]
// so deletes do not permanently waste arena slots.
//
// The root is always at ID 0 and never released.
//
// Concurrency: not safe for concurrent use. Callers must externally
// serialize AddBatch / ListDirectory calls.
type Tree struct {
	chunks [][]node // each entry is a fixed-size slab of nodeChunkSize nodes
	nextID uint32   // next free ID assuming no freelist hits
	free   []uint32
}

// nodeChunkSize / chunkBits define the chunked arena geometry. Each chunk
// is a fixed-size []node slab; once allocated, the backing array never
// moves, which keeps *node pointers stable across [Tree.alloc] calls.
// chunkBits is the binary log so that index decomposition uses shift + mask
// instead of a divide.
const (
	chunkBits     = 12
	nodeChunkSize = 1 << chunkBits
	chunkMask     = nodeChunkSize - 1
)

// rootID is the arena ID reserved for the tree's root node.
const rootID uint32 = 0

// New returns an empty Tree.
func New() *Tree {
	t := &Tree{}
	t.alloc(node{}) // rootID at ID 0
	return t
}

// at returns a pointer to the node with the given ID. The pointer is stable
// for the lifetime of the Tree: subsequent [Tree.alloc] calls may extend the
// arena with new chunks but never move existing ones.
func (t *Tree) at(id uint32) *node {
	return &t.chunks[id>>chunkBits][id&chunkMask]
}

// alloc places init in the next available arena slot (popped from the
// freelist if one is available, otherwise carved from the current or a
// freshly created chunk) and returns the slot ID.
func (t *Tree) alloc(init node) uint32 {
	if n := len(t.free); n > 0 {
		id := t.free[n-1]
		t.free = t.free[:n-1]
		*t.at(id) = init
		return id
	}
	if int(t.nextID>>chunkBits) >= len(t.chunks) {
		t.chunks = append(t.chunks, make([]node, nodeChunkSize))
	}
	id := t.nextID
	t.nextID++
	*t.at(id) = init
	return id
}

// release clears the slot at id and pushes the ID onto the freelist for
// future reuse. The cleared slot drops references so the GC can reclaim any
// edge / file / agg / children backings the node held.
func (t *Tree) release(id uint32) {
	*t.at(id) = node{}
	t.free = append(t.free, id)
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
		n := t.at(id)
		if rem == "" {
			return t.listAtNode(n), nil
		}
		idx, ok := t.findChild(n.children, rem[0])
		if !ok {
			return nil, nil
		}
		childID := n.children[idx]
		child := t.at(childID)
		if len(rem) >= len(child.edge) && rem[:len(child.edge)] == child.edge {
			id = childID
			rem = rem[len(child.edge):]
			continue
		}
		if len(rem) < len(child.edge) && child.edge[:len(rem)] == rem {
			// prefix ends inside this compressed edge: single chain — emit at most
			// one entry (either a subdirectory whose name extends to the next '/'
			// in the edge, or a file if no '/' remains).
			return t.listInsideEdge(child, rem), nil
		}
		return nil, nil
	}
}

// listAtNode lists the entries reachable from n without crossing a '/'.
// When the requested prefix lands exactly on a node boundary, n.file (if any)
// is the directory-marker object at that prefix and is emitted with an empty
// name so callers may distinguish or filter it.
func (t *Tree) listAtNode(n *node) []Entry {
	out := make([]Entry, 0, len(n.children)+1)
	if n.file != nil {
		out = append(out, Entry{
			Name:  "",
			IsDir: false,
			Class: n.file.Class,
			Size:  n.file.Size,
		})
	}
	for _, cid := range n.children {
		out = t.emitFromNode(cid, "", out)
	}
	return out
}

// listInsideEdge handles the case where the prefix lands strictly inside a
// compressed edge. The residual edge bytes after the prefix form the suffix
// that all entries in this logical directory share.
func (t *Tree) listInsideEdge(child *node, consumed string) []Entry {
	residual := child.edge[len(consumed):]
	if i := strings.IndexByte(residual, '/'); i >= 0 {
		return []Entry{{
			Name:      residual[:i+1],
			IsDir:     true,
			Aggregate: child.effectiveAgg(),
		}}
	}
	out := make([]Entry, 0, len(child.children)+1)
	if child.file != nil {
		out = append(out, Entry{
			Name:  residual,
			IsDir: false,
			Class: child.file.Class,
			Size:  child.file.Size,
		})
	}
	for _, cid := range child.children {
		out = t.emitFromNode(cid, residual, out)
	}
	return out
}

// emitFromNode appends all logical-dir entries reachable from the node at id
// without crossing a '/' to out and returns the resulting slice. acc is the
// bytes accumulated from the parent boundary node down to (but not including)
// the node's edge.
func (t *Tree) emitFromNode(id uint32, acc string, out []Entry) []Entry {
	n := t.at(id)
	if i := strings.IndexByte(n.edge, '/'); i >= 0 {
		return append(out, Entry{
			Name:      acc + n.edge[:i+1],
			IsDir:     true,
			Aggregate: n.effectiveAgg(),
		})
	}
	fullName := acc + n.edge
	if n.file != nil {
		out = append(out, Entry{
			Name:  fullName,
			IsDir: false,
			Class: n.file.Class,
			Size:  n.file.Size,
		})
	}
	for _, cid := range n.children {
		out = t.emitFromNode(cid, fullName, out)
	}
	return out
}

// findChild binary-searches children for a child whose edge starts with b.
// Returns (index, found). When found is false, index is the insertion point.
func (t *Tree) findChild(children []uint32, b byte) (int, bool) {
	i := sort.Search(len(children), func(i int) bool {
		return t.at(children[i]).edge[0] >= b
	})
	if i < len(children) && t.at(children[i]).edge[0] == b {
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
// and extends path with each visited node. The returned path terminates at
// obj's destination so the caller can reuse it for the next insert.
//
// Aggregate bookkeeping is top-down: obj contributes one object and Size
// bytes to every ancestor on its descent path. The frames already in path
// when this function runs are exactly the ancestors carried over from the
// previous insert that are still ancestors of obj (the others were popped
// against the LCP). We bump them once up-front, then bump each new node we
// step into during the descent — never the same node twice.
//
// Because the arena is chunked, *node pointers stay valid across [Tree.alloc]
// calls — no refresh dance needed.
func (t *Tree) insertFromPath(path []framePath, obj Object) []framePath {
	for _, f := range path {
		t.bumpAgg(f.id, obj)
	}

	id := path[len(path)-1].id
	consumed := path[len(path)-1].consumed
	key := obj.Key[consumed:]

	for {
		n := t.at(id)
		if key == "" {
			// The key terminates exactly at n; n becomes a "file leaf"
			// (or gains its file marker if it was a pure intermediate).
			// n.agg was already bumped via the path pre-walk.
			n.file = &ClassByte{Class: obj.Class, Size: obj.Size}
			return path
		}

		idx, ok := t.findChild(n.children, key[0])
		if !ok {
			// No sibling shares the next byte — attach the remainder of
			// the key as a brand-new leaf. The leaf's own aggregate is
			// derived on demand from its file (effectiveAgg).
			//
			// strings.Clone is critical here (and at every other edge
			// assignment in this function): without it the edge would be
			// a slice header pointing into the caller's batch key string,
			// keeping the whole multi-hundred-byte source string alive
			// for the lifetime of the tree. At ten-of-millions-of-objects
			// scale that retention dwarfs the radix nodes themselves.
			leafID := t.alloc(node{
				edge: strings.Clone(key),
				file: &ClassByte{Class: obj.Class, Size: obj.Size},
			})
			n.children = insertChildAt(n.children, idx, leafID)
			return append(path, framePath{id: leafID, consumed: consumed + len(key)})
		}

		childID := n.children[idx]
		child := t.at(childID)
		lcp := longestCommonPrefix(child.edge, key)
		if lcp == len(child.edge) {
			// The whole edge is a prefix of the remaining key — descend
			// into child and continue the loop one level deeper.
			t.bumpAgg(childID, obj)
			id = childID
			consumed += lcp
			key = key[lcp:]
			path = append(path, framePath{id: childID, consumed: consumed})
			continue
		}

		// child.edge and key diverge after lcp bytes. Build an intermediate
		// node carrying the shared prefix, shrink child to its remainder,
		// and place obj either at the intermediate itself (if its key ends
		// exactly at lcp) or as a brand-new sibling leaf. child.edge is
		// already owned (was cloned on its own insert) — slicing it stays
		// on its private backing.
		intAgg := t.aggForSplit(childID, obj)
		intID := t.alloc(node{edge: child.edge[:lcp], agg: intAgg})
		child.edge = child.edge[lcp:]

		if lcp == len(key) {
			inter := t.at(intID)
			inter.file = &ClassByte{Class: obj.Class, Size: obj.Size}
			inter.children = []uint32{childID}
			n.children[idx] = intID
			return append(path, framePath{id: intID, consumed: consumed + lcp})
		}

		leafID := t.alloc(node{
			edge: strings.Clone(key[lcp:]),
			file: &ClassByte{Class: obj.Class, Size: obj.Size},
		})
		t.at(intID).children = t.sortChildren(childID, leafID)
		n.children[idx] = intID
		return append(path,
			framePath{id: intID, consumed: consumed + lcp},
			framePath{id: leafID, consumed: consumed + len(key)},
		)
	}
}

// bumpAgg adds obj's contribution (one object, one Size-byte entry under
// obj.Class) to the aggregate of the node at id. If the node is a pure leaf
// that has never carried a materialised aggregate, one is allocated here from
// its file metadata.
func (t *Tree) bumpAgg(id uint32, obj Object) {
	n := t.at(id)
	if n.agg == nil {
		a := n.effectiveAgg()
		n.agg = &a
	}
	n.agg.Objects++
	n.agg.Bytes.Add(obj.Class, obj.Size)
}

// aggForSplit builds the aggregate that the new intermediate produced by a
// split should carry. It is child's existing aggregate plus one new object
// from obj.
//
// If child is already internal we cannot simply alias its Bytes slice — the
// in-place Add below would also overwrite child.agg.Bytes via the shared
// backing. effectiveAgg returns a fresh Aggregate for pure leaves, so in
// that case we adopt it directly without copying.
func (t *Tree) aggForSplit(childID uint32, obj Object) *Aggregate {
	child := t.at(childID)
	var a Aggregate
	if child.agg != nil {
		a.Objects = child.agg.Objects + 1
		if k := len(child.agg.Bytes); k > 0 {
			a.Bytes = append(make(ClassBytes, 0, k+1), child.agg.Bytes...)
		}
	} else {
		a = child.effectiveAgg()
		a.Objects++
	}
	a.Bytes.Add(obj.Class, obj.Size)
	return &a
}

// sortChildren returns a 2-element child slice sorted ascending by the
// first byte of each child's edge.
func (t *Tree) sortChildren(a, b uint32) []uint32 {
	if t.at(a).edge[0] < t.at(b).edge[0] {
		return []uint32{a, b}
	}
	return []uint32{b, a}
}

// deleteKey removes key from the tree and propagates the negative aggregate
// delta. Empty branches are pruned and single-child compression is restored.
// Pruned nodes are released back to the arena freelist for reuse.
func (t *Tree) deleteKey(key string) {
	type pop struct {
		parentID uint32
		childIdx int
	}
	pops := make([]pop, 0, 8)
	path := make([]framePath, 1, 8)
	path[0] = framePath{id: rootID, consumed: 0}
	curID := rootID
	rem := key
	consumed := 0

	for rem != "" {
		cur := t.at(curID)
		idx, ok := t.findChild(cur.children, rem[0])
		if !ok {
			return
		}
		childID := cur.children[idx]
		child := t.at(childID)
		if !strings.HasPrefix(rem, child.edge) {
			return
		}
		pops = append(pops, pop{parentID: curID, childIdx: idx})
		curID = childID
		consumed += len(child.edge)
		rem = rem[len(child.edge):]
		path = append(path, framePath{id: childID, consumed: consumed})
	}
	cur := t.at(curID)
	if cur.file == nil {
		return
	}
	// applyDelta materialises leaf aggregates via effectiveAgg(), which reads
	// cur.file. Subtract before clearing the file pointer.
	class, size := cur.file.Class, cur.file.Size
	t.applyDelta(path, class, -size, -1)
	t.at(curID).file = nil

	// Bottom-up cleanup: prune empty leaves; merge single-child no-file nodes
	// back into a single compressed edge. Pruned IDs return to the freelist.
	for i := len(pops) - 1; i >= 0; i-- {
		fr := pops[i]
		parent := t.at(fr.parentID)
		childID := parent.children[fr.childIdx]
		child := t.at(childID)
		switch {
		case child.file == nil && len(child.children) == 0:
			parent.children = append(parent.children[:fr.childIdx], parent.children[fr.childIdx+1:]...)
			t.release(childID)
		case child.file == nil && len(child.children) == 1:
			onlyID := child.children[0]
			only := t.at(onlyID)
			only.edge = child.edge + only.edge
			t.at(fr.parentID).children[fr.childIdx] = onlyID
			t.release(childID)
		default:
			return
		}
	}
}

// updateKey changes the file metadata for an existing key and adjusts
// aggregates by the difference. Assumes the key currently exists.
func (t *Tree) updateKey(obj Object) {
	path := make([]framePath, 1, 8)
	path[0] = framePath{id: rootID, consumed: 0}
	curID := rootID
	rem := obj.Key
	consumed := 0
	for rem != "" {
		cur := t.at(curID)
		idx, ok := t.findChild(cur.children, rem[0])
		if !ok {
			return
		}
		childID := cur.children[idx]
		child := t.at(childID)
		if !strings.HasPrefix(rem, child.edge) {
			return
		}
		curID = childID
		consumed += len(child.edge)
		rem = rem[len(child.edge):]
		path = append(path, framePath{id: curID, consumed: consumed})
	}
	cur := t.at(curID)
	if cur.file == nil {
		return
	}
	oldClass, oldSize := cur.file.Class, cur.file.Size
	// applyDelta lazily materialises a leaf's aggregate via effectiveAgg,
	// which reads cur.file. The old contribution must be subtracted before
	// the file is overwritten.
	if oldClass == obj.Class {
		if d := obj.Size - oldSize; d != 0 {
			t.applyDelta(path, obj.Class, d, 0)
		}
		t.at(curID).file.Size = obj.Size
		return
	}
	t.applyDelta(path, oldClass, -oldSize, -1)
	t.at(curID).file = &ClassByte{Class: obj.Class, Size: obj.Size}
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

// applyDelta walks the path from root to leaf applying the per-class byte
// delta and the object-count delta to each node's aggregate. Aggregates are
// materialised lazily — leaves that previously omitted their agg field get it
// allocated here on first touch.
func (t *Tree) applyDelta(path []framePath, class StorageClass, sizeDelta, objDelta int64) {
	for i := range path {
		n := t.at(path[i].id)
		if n.agg == nil {
			a := n.effectiveAgg()
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
func (rc *rangeCursor) Next() (Object, bool) {
	for len(rc.stack) > 0 {
		top := &rc.stack[len(rc.stack)-1]
		n := rc.t.at(top.id)
		if !top.fileEmitted {
			top.fileEmitted = true
			if n.file != nil && top.prefix > rc.lo && top.prefix <= rc.hi {
				return Object{Key: top.prefix, Size: n.file.Size, Class: n.file.Class}, true
			}
		}
		if top.nextChild >= len(n.children) {
			rc.stack = rc.stack[:len(rc.stack)-1]
			continue
		}
		cid := n.children[top.nextChild]
		top.nextChild++
		c := rc.t.at(cid)
		childKey := top.prefix + c.edge
		// All subsequent siblings have a strictly greater first byte, hence
		// strictly greater childKey — once one exceeds hi, pop the frame.
		if childKey > rc.hi {
			rc.stack = rc.stack[:len(rc.stack)-1]
			continue
		}
		if childKey < rc.lo && !strings.HasPrefix(rc.lo, childKey) {
			continue
		}
		rc.stack = append(rc.stack, cursorFrame{id: cid, prefix: childKey})
	}
	return Object{}, false
}
