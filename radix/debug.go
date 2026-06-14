package radix

// NodeKind discriminates a NodeView between the two arena types.
type NodeKind uint8

const (
	NodeInternal NodeKind = iota
	NodeLeaf
)

// String returns "internal" or "leaf" for human display.
func (k NodeKind) String() string {
	switch k {
	case NodeInternal:
		return "internal"
	case NodeLeaf:
		return "leaf"
	default:
		return "?"
	}
}

// NodeView is a read-only snapshot of one radix node, suitable for debug or
// visualisation tools. Both internals and leaves share this type. Fields not
// applicable to the node's Kind are left at their zero values.
type NodeView struct {
	// CID is the tagged child reference as it appears in a parent's
	// children slice. The root internal carries CID=0 (no tag, no parent).
	CID  uint32
	Kind NodeKind
	Edge string

	// Internal-only fields.
	NumChildren  int
	HasDirMarker bool
	DirMarker    ClassByte
	Aggregate    Aggregate

	// Leaf-only fields.
	Class StorageClass
	Size  int64
}

// RootView returns the root internal as a NodeView. The root's CID is 0 (it
// has no parent and is the always-allocated entry point into the tree).
func (t *Tree) RootView() NodeView {
	return t.viewInternal(rootID)
}

// ChildrenOf returns the children of an internal node addressed by the
// tagged child ID cid. Returns nil for leaf cids (they have no children).
// Order matches the in-tree storage: ascending by first byte of child edge.
func (t *Tree) ChildrenOf(cid uint32) []NodeView {
	if isLeafID(cid) {
		return nil
	}
	n := t.atInternal(cid)
	out := make([]NodeView, 0, len(n.children))
	for _, c := range n.children {
		if isLeafID(c) {
			out = append(out, t.viewLeaf(c))
			continue
		}
		out = append(out, t.viewInternal(c))
	}
	return out
}

func (t *Tree) viewInternal(id uint32) NodeView {
	n := t.atInternal(id)
	v := NodeView{
		CID:         id,
		Kind:        NodeInternal,
		Edge:        t.edgeStr(n.edge),
		NumChildren: len(n.children),
		Aggregate:   t.internalAgg(id),
	}
	if cb, ok := t.dirMarker(id); ok {
		v.HasDirMarker = true
		v.DirMarker = cb
	}
	return v
}

func (t *Tree) viewLeaf(cid uint32) NodeView {
	l := t.atLeaf(cid & idMask)
	return NodeView{
		CID:   cid,
		Kind:  NodeLeaf,
		Edge:  t.edgeStr(l.edge),
		Class: l.class(),
		Size:  l.size(),
	}
}
