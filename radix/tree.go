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
//	agg:      recursive aggregate over the entire subtree rooted at this node.
type node struct {
	edge     string
	children []*node
	file     *fileMeta
	agg      Aggregate
}

type fileMeta struct {
	Class string
	Size  int64
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
func (t *Tree) AddBatch(b Batch) error {
	if len(b.Objects) == 0 {
		return nil
	}
	if err := validateBatch(b); err != nil {
		return err
	}
	hi := b.Objects[len(b.Objects)-1].Key

	existing := t.collectRange(b.StartFrom, hi)

	i, j := 0, 0
	for i < len(existing) || j < len(b.Objects) {
		switch {
		case j >= len(b.Objects):
			t.deleteKey(existing[i].Key)
			i++
		case i >= len(existing):
			t.insertKey(b.Objects[j])
			j++
		case existing[i].Key < b.Objects[j].Key:
			t.deleteKey(existing[i].Key)
			i++
		case existing[i].Key > b.Objects[j].Key:
			t.insertKey(b.Objects[j])
			j++
		default:
			if existing[i].Size != b.Objects[j].Size || existing[i].Class != b.Objects[j].Class {
				t.updateKey(b.Objects[j])
			}
			i++
			j++
		}
	}
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
			Aggregate: child.agg,
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
			Aggregate: n.agg,
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

// insertKey inserts obj into the tree, creating/splitting nodes as needed, and
// propagates the aggregate delta along the resulting path.
func (t *Tree) insertKey(obj Object) {
	path := make([]*node, 0, 8)
	cur := t.root
	path = append(path, cur)
	rem := obj.Key

	for {
		if rem == "" {
			cur.file = &fileMeta{Class: obj.Class, Size: obj.Size}
			applyDelta(path, obj.Class, obj.Size, +1)
			return
		}
		idx, ok := findChild(cur.children, rem[0])
		if !ok {
			leaf := &node{
				edge: rem,
				file: &fileMeta{Class: obj.Class, Size: obj.Size},
			}
			cur.children = insertChildAt(cur.children, idx, leaf)
			path = append(path, leaf)
			applyDelta(path, obj.Class, obj.Size, +1)
			return
		}
		child := cur.children[idx]
		lcp := longestCommonPrefix(child.edge, rem)
		switch {
		case lcp == len(child.edge):
			cur = child
			rem = rem[lcp:]
			path = append(path, cur)
		case lcp == len(rem):
			// rem is a strict prefix of child.edge: split so that the new key
			// terminates at the new intermediate node.
			intermediate := &node{
				edge: child.edge[:lcp],
				file: &fileMeta{Class: obj.Class, Size: obj.Size},
				agg:  child.agg,
			}
			child.edge = child.edge[lcp:]
			intermediate.children = []*node{child}
			cur.children[idx] = intermediate
			path = append(path, intermediate)
			applyDelta(path, obj.Class, obj.Size, +1)
			return
		default:
			// generic split: child.edge and rem diverge after lcp bytes.
			intermediate := &node{
				edge: child.edge[:lcp],
				agg:  child.agg,
			}
			child.edge = child.edge[lcp:]
			leaf := &node{
				edge: rem[lcp:],
				file: &fileMeta{Class: obj.Class, Size: obj.Size},
			}
			intermediate.children = sortChildren(child, leaf)
			cur.children[idx] = intermediate
			path = append(path, intermediate, leaf)
			applyDelta(path, obj.Class, obj.Size, +1)
			return
		}
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
	type frame struct {
		parent   *node
		childIdx int
	}
	stack := make([]frame, 0, 8)
	path := make([]*node, 0, 8)
	cur := t.root
	path = append(path, cur)
	rem := key

	for rem != "" {
		idx, ok := findChild(cur.children, rem[0])
		if !ok {
			return
		}
		child := cur.children[idx]
		if !strings.HasPrefix(rem, child.edge) {
			return
		}
		stack = append(stack, frame{parent: cur, childIdx: idx})
		cur = child
		path = append(path, cur)
		rem = rem[len(child.edge):]
	}
	if cur.file == nil {
		return
	}
	class, size := cur.file.Class, cur.file.Size
	cur.file = nil
	applyDelta(path, class, -size, -1)

	// Bottom-up cleanup: prune empty leaves; merge single-child no-file nodes
	// back into a single compressed edge.
	for i := len(stack) - 1; i >= 0; i-- {
		fr := stack[i]
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
	path := make([]*node, 0, 8)
	cur := t.root
	path = append(path, cur)
	rem := obj.Key
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
		path = append(path, cur)
		rem = rem[len(child.edge):]
	}
	if cur.file == nil {
		return
	}
	oldClass, oldSize := cur.file.Class, cur.file.Size
	cur.file = &fileMeta{Class: obj.Class, Size: obj.Size}
	if oldClass == obj.Class {
		if d := obj.Size - oldSize; d != 0 {
			applyDelta(path, obj.Class, d, 0)
		}
		return
	}
	applyDelta(path, oldClass, -oldSize, -1)
	applyDelta(path, obj.Class, obj.Size, +1)
}

// applyDelta walks the path from root to leaf applying the per-class byte
// delta and the object-count delta to each node's aggregate.
func applyDelta(path []*node, class string, sizeDelta, objDelta int64) {
	for _, n := range path {
		n.agg.Objects += objDelta
		if sizeDelta != 0 {
			n.agg.Bytes.Add(class, sizeDelta)
		}
	}
}

// collectRange returns all objects in the tree with key in (lo, hi], in
// ascending lex order. Prunes subtrees that cannot overlap the range.
func (t *Tree) collectRange(lo, hi string) []Object {
	return walkRange(t.root, "", lo, hi, make([]Object, 0, 64))
}

func walkRange(n *node, key, lo, hi string, out []Object) []Object {
	if n.file != nil && key > lo && key <= hi {
		out = append(out, Object{Key: key, Size: n.file.Size, Class: n.file.Class})
	}
	for _, c := range n.children {
		childKey := key + c.edge
		if childKey > hi {
			// All subsequent siblings have a strictly greater first byte, hence
			// strictly greater childKey; safe to break.
			break
		}
		if childKey < lo && !strings.HasPrefix(lo, childKey) {
			continue
		}
		out = walkRange(c, childKey, lo, hi, out)
	}
	return out
}

