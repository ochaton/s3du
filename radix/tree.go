package radix

import (
	"sort"
	"strings"
)

// node is a single radix-tree node.
//
//	edge:     compressed path fragment from the parent node (empty only on root).
//	children: sorted ascending by the first byte of their edge; Patricia
//	          invariant guarantees each first byte is unique among siblings.
//	file:     non-nil iff a key terminates exactly at this node.
//	agg:      non-nil for nodes that have ever been internal; nil for pure
//	          leaves whose aggregate is derived on demand (see effectiveAgg).
type node struct {
	edge     string
	children []*node
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

// Tree is an in-memory compressed radix tree over S3 object keys.
//
// Concurrency: not safe for concurrent use. Callers must externally serialize
// AddBatch and ListDirectory calls.
type Tree struct {
	root *node
}

// New returns an empty Tree.
func New() *Tree {
	return &Tree{root: &node{}}
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

	cur := t.root
	rem := prefix
	for {
		if rem == "" {
			return listAtNode(cur), nil
		}
		idx, ok := findChild(cur.children, rem[0])
		if !ok {
			return nil, nil
		}
		child := cur.children[idx]
		if len(rem) >= len(child.edge) && rem[:len(child.edge)] == child.edge {
			cur = child
			rem = rem[len(child.edge):]
			continue
		}
		if len(rem) < len(child.edge) && child.edge[:len(rem)] == rem {
			// prefix ends inside this compressed edge: single chain — emit at most
			// one entry (either a subdirectory whose name extends to the next '/'
			// in the edge, or a file if no '/' remains).
			return listInsideEdge(child, rem), nil
		}
		return nil, nil
	}
}

// listAtNode lists the entries reachable from n without crossing a '/'.
// When the requested prefix lands exactly on a node boundary, n.file (if any)
// is the directory-marker object at that prefix and is emitted with an empty
// name so callers may distinguish or filter it.
func listAtNode(n *node) []Entry {
	out := make([]Entry, 0, len(n.children)+1)
	if n.file != nil {
		out = append(out, Entry{
			Name:  "",
			IsDir: false,
			Class: n.file.Class,
			Size:  n.file.Size,
		})
	}
	for _, c := range n.children {
		out = emitFromNode(c, "", out)
	}
	return out
}

// listInsideEdge handles the case where the prefix lands strictly inside a
// compressed edge. The residual edge bytes after the prefix form the suffix
// that all entries in this logical directory share.
func listInsideEdge(child *node, consumed string) []Entry {
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
	for _, c := range child.children {
		out = emitFromNode(c, residual, out)
	}
	return out
}

// emitFromNode appends all logical-dir entries reachable from n without
// crossing a '/' to out and returns the resulting slice. acc is the bytes
// accumulated from the parent boundary node down to (but not including)
// n.edge. A node whose edge contains '/' produces one dir entry covering its
// full subtree; a node without '/' may produce a file entry (if it terminates
// a key) plus recursive expansion through its children.
func emitFromNode(n *node, acc string, out []Entry) []Entry {
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
	for _, c := range n.children {
		out = emitFromNode(c, fullName, out)
	}
	return out
}

// findChild binary-searches n.children for the child whose edge starts with b.
// Returns (index, found). When found is false, index is the insertion point.
func findChild(children []*node, b byte) (int, bool) {
	i := sort.Search(len(children), func(i int) bool {
		return children[i].edge[0] >= b
	})
	if i < len(children) && children[i].edge[0] == b {
		return i, true
	}
	return i, false
}

// insertChildAt inserts c into children at index idx, preserving order.
func insertChildAt(children []*node, idx int, c *node) []*node {
	children = append(children, nil)
	copy(children[idx+1:], children[idx:])
	children[idx] = c
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
// including n.edge — used by [Tree.insertBatch]'s LCP fast-path to compare
// against the next key without re-descending from the root.
type framePath struct {
	n        *node
	consumed int
}

// insertBatch inserts a sorted, conflict-free slice of objects.
func (t *Tree) insertBatch(objs []Object) {
	for i := range objs {
		t.insert(t.root, objs[i].Key, objs[i])
	}
}

// insert places obj into the subtree rooted at n, descending iteratively
// until n's path equals obj.Key. n.agg is bumped on every descent step, so
// each ancestor's aggregate is correct without a separate fix-up pass. The
// first visit to a pure leaf along the descent path materialises its agg.
func (t *Tree) insert(n *node, key string, obj Object) {
	for {
		if n.agg == nil {
			a := n.effectiveAgg()
			n.agg = &a
		}
		n.agg.Objects++
		n.agg.Bytes.Add(obj.Class, obj.Size)

		if key == "" {
			n.file = &ClassByte{Class: obj.Class, Size: obj.Size}
			return
		}
		idx, ok := findChild(n.children, key[0])
		if !ok {
			leaf := &node{
				edge: key,
				file: &ClassByte{Class: obj.Class, Size: obj.Size},
			}
			n.children = insertChildAt(n.children, idx, leaf)
			return
		}
		child := n.children[idx]
		lcp := longestCommonPrefix(child.edge, key)
		if lcp == len(child.edge) {
			n = child
			key = key[lcp:]
			continue
		}
		// Split: child.edge[:lcp] is shared with key; create new intermediate.
		intermediate := &node{edge: child.edge[:lcp]}
		var a Aggregate
		if child.agg != nil {
			// Child is already internal; its agg.Bytes is live. Deep-copy
			// to avoid corrupting it via the in-place Add below.
			a.Objects = child.agg.Objects + 1
			if n := len(child.agg.Bytes); n > 0 {
				a.Bytes = append(make(ClassBytes, 0, n+1), child.agg.Bytes...)
			}
		} else {
			// Pure leaf or empty: effectiveAgg returns a freshly constructed
			// Aggregate, no aliasing — adopt it directly.
			a = child.effectiveAgg()
			a.Objects++
		}
		a.Bytes.Add(obj.Class, obj.Size)
		intermediate.agg = &a
		child.edge = child.edge[lcp:]
		if lcp == len(key) {
			intermediate.file = &ClassByte{Class: obj.Class, Size: obj.Size}
			intermediate.children = []*node{child}
		} else {
			leaf := &node{
				edge: key[lcp:],
				file: &ClassByte{Class: obj.Class, Size: obj.Size},
			}
			intermediate.children = sortChildren(child, leaf)
		}
		n.children[idx] = intermediate
		return
	}
}

func sortChildren(a, b *node) []*node {
	if a.edge[0] < b.edge[0] {
		return []*node{a, b}
	}
	return []*node{b, a}
}

// deleteKey removes key from the tree and propagates the negative aggregate
// delta. Empty branches are pruned and single-child compression is restored.
func (t *Tree) deleteKey(key string) {
	type pop struct {
		parent   *node
		childIdx int
	}
	pops := make([]pop, 0, 8)
	path := make([]framePath, 1, 8)
	path[0] = framePath{n: t.root, consumed: 0}
	cur := t.root
	rem := key
	consumed := 0

	for rem != "" {
		idx, ok := findChild(cur.children, rem[0])
		if !ok {
			return
		}
		child := cur.children[idx]
		if !strings.HasPrefix(rem, child.edge) {
			return
		}
		pops = append(pops, pop{parent: cur, childIdx: idx})
		cur = child
		consumed += len(child.edge)
		rem = rem[len(child.edge):]
		path = append(path, framePath{n: cur, consumed: consumed})
	}
	if cur.file == nil {
		return
	}
	// applyDelta materialises leaf aggregates via effectiveAgg(), which reads
	// cur.file. Subtract before clearing the file pointer.
	class, size := cur.file.Class, cur.file.Size
	applyDelta(path, class, -size, -1)
	cur.file = nil

	// Bottom-up cleanup: prune empty leaves; merge single-child no-file nodes
	// back into a single compressed edge.
	for i := len(pops) - 1; i >= 0; i-- {
		fr := pops[i]
		n := fr.parent.children[fr.childIdx]
		switch {
		case n.file == nil && len(n.children) == 0:
			fr.parent.children = append(fr.parent.children[:fr.childIdx], fr.parent.children[fr.childIdx+1:]...)
		case n.file == nil && len(n.children) == 1:
			only := n.children[0]
			only.edge = n.edge + only.edge
			fr.parent.children[fr.childIdx] = only
		default:
			return
		}
	}
}

// updateKey changes the file metadata for an existing key and adjusts
// aggregates by the difference. Assumes the key currently exists.
func (t *Tree) updateKey(obj Object) {
	path := make([]framePath, 1, 8)
	path[0] = framePath{n: t.root, consumed: 0}
	cur := t.root
	rem := obj.Key
	consumed := 0
	for rem != "" {
		idx, ok := findChild(cur.children, rem[0])
		if !ok {
			return
		}
		child := cur.children[idx]
		if !strings.HasPrefix(rem, child.edge) {
			return
		}
		cur = child
		consumed += len(child.edge)
		rem = rem[len(child.edge):]
		path = append(path, framePath{n: cur, consumed: consumed})
	}
	if cur.file == nil {
		return
	}
	oldClass, oldSize := cur.file.Class, cur.file.Size
	// applyDelta lazily materialises a leaf's aggregate via effectiveAgg,
	// which reads cur.file. The old contribution must be subtracted before
	// the file is overwritten.
	if oldClass == obj.Class {
		if d := obj.Size - oldSize; d != 0 {
			applyDelta(path, obj.Class, d, 0)
		}
		cur.file.Size = obj.Size
		return
	}
	applyDelta(path, oldClass, -oldSize, -1)
	cur.file = &ClassByte{Class: obj.Class, Size: obj.Size}
	applyDelta(path, obj.Class, obj.Size, +1)
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
func applyDelta(path []framePath, class StorageClass, sizeDelta, objDelta int64) {
	for i := range path {
		n := path[i].n
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
	stack  []cursorFrame
	lo, hi string
}

type cursorFrame struct {
	n           *node
	prefix      string // bytes from root including n.edge
	nextChild   int    // index of next unvisited child
	fileEmitted bool
}

func (t *Tree) newRangeCursor(lo, hi string) *rangeCursor {
	rc := &rangeCursor{
		lo:    lo,
		hi:    hi,
		stack: make([]cursorFrame, 1, 16),
	}
	rc.stack[0] = cursorFrame{n: t.root}
	return rc
}

// Next returns the next object whose key falls in (lo, hi], in ascending lex
// order. Returns (_, false) once the iteration is exhausted; subsequent calls
// continue to return false.
func (rc *rangeCursor) Next() (Object, bool) {
	for len(rc.stack) > 0 {
		top := &rc.stack[len(rc.stack)-1]
		if !top.fileEmitted {
			top.fileEmitted = true
			if top.n.file != nil && top.prefix > rc.lo && top.prefix <= rc.hi {
				return Object{Key: top.prefix, Size: top.n.file.Size, Class: top.n.file.Class}, true
			}
		}
		if top.nextChild >= len(top.n.children) {
			rc.stack = rc.stack[:len(rc.stack)-1]
			continue
		}
		c := top.n.children[top.nextChild]
		top.nextChild++
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
		rc.stack = append(rc.stack, cursorFrame{n: c, prefix: childKey})
	}
	return Object{}, false
}

