package radix

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// On-disk snapshot format (little-endian throughout).
//
//	header (20 bytes, fixed)
//	  magic        uint32  = "RDXT" (0x54584452 little-endian == 'R','D','X','T')
//	  version      uint16  = 1
//	  flags        uint16  = 0
//	  max_id       uint32  // largest alive ID; arena size on load = max_id+1
//	  alive_count  uint32  // number of node records that follow
//	  root_id      uint32  = 0
//
//	node record (variable length, one per alive arena slot)
//	  id          uint32
//	  flags       uint8         // bit 0 hasFile, bit 1 hasAgg
//	  edge_len    uint16
//	  edge_bytes  [edge_len]byte
//	  children_n  uint16
//	  children    [children_n]uint32
//	  (if hasFile) file_class  uint8
//	               file_size   int64
//	  (if hasAgg)  agg_objects int64
//	               agg_n       uint8
//	               [agg_n]{ class uint8; size int64 }
//
// Records are written in ascending ID order; slots that were on the freelist
// at save time are simply omitted. On load the freelist is rebuilt from the
// gaps in [0, max_id]. The root is always alive at ID 0.
const (
	snapshotMagic   uint32 = 0x54584452 // 'R'|'D'<<8|'X'<<16|'T'<<24
	snapshotVersion uint16 = 1
	snapshotHdrSize        = 20
)

const (
	snapFlagHasFile uint8 = 1 << 0
	snapFlagHasAgg  uint8 = 1 << 1
)

var (
	// ErrSnapshotMagic is returned by Load when the file does not begin with
	// the expected magic identifier.
	ErrSnapshotMagic = errors.New("radix: snapshot magic mismatch")
	// ErrSnapshotVersion is returned by Load when the snapshot's format
	// version is not supported by this build.
	ErrSnapshotVersion = errors.New("radix: snapshot version unsupported")
	// ErrSnapshotCorrupt is returned by Load when a record's ID is out of
	// range or repeats one already seen.
	ErrSnapshotCorrupt = errors.New("radix: snapshot corrupt")
)

// Save writes the tree's binary snapshot to w. The on-disk representation
// keyed by arena ID means children references are byte-identical before save
// and after Load — no pointer fixup, no second pass.
func (t *Tree) Save(w io.Writer) error {
	bw := bufio.NewWriter(w)

	// Mark live slots; the freelist tells us which IDs to skip.
	alive := make([]bool, t.nextID)
	for i := range alive {
		alive[i] = true
	}
	for _, id := range t.free {
		alive[id] = false
	}
	var aliveCount uint32
	for _, ok := range alive {
		if ok {
			aliveCount++
		}
	}

	var maxID uint32
	if t.nextID > 0 {
		maxID = t.nextID - 1
	}
	if err := writeHeader(bw, maxID, aliveCount); err != nil {
		return err
	}
	for id := uint32(0); id < t.nextID; id++ {
		if !alive[id] {
			continue
		}
		if err := writeNodeRecord(bw, id, t.at(id)); err != nil {
			return fmt.Errorf("radix: write node %d: %w", id, err)
		}
	}
	return bw.Flush()
}

// Load reads a snapshot previously produced by Save and reconstructs the
// in-memory tree. Arena chunks are sized exactly to fit max_id+1 nodes; gaps
// between alive IDs are recovered into the freelist so future inserts reuse
// them.
func Load(r io.Reader) (*Tree, error) {
	br := bufio.NewReader(r)
	maxID, aliveCount, err := readHeader(br)
	if err != nil {
		return nil, err
	}

	nextID := maxID + 1
	t := &Tree{nextID: nextID}
	chunkCount := int((uint32(nextID) + nodeChunkSize - 1) >> chunkBits)
	t.chunks = make([][]node, chunkCount)
	for i := range t.chunks {
		t.chunks[i] = make([]node, nodeChunkSize)
	}

	seen := make([]bool, nextID)
	for i := range aliveCount {
		id, n, err := readNodeRecord(br)
		if err != nil {
			return nil, fmt.Errorf("radix: read record %d: %w", i, err)
		}
		if id >= nextID || seen[id] {
			return nil, ErrSnapshotCorrupt
		}
		seen[id] = true
		*t.at(id) = n
	}
	for id := range nextID {
		if !seen[id] {
			t.free = append(t.free, id)
		}
	}
	return t, nil
}

// Export walks every alive leaf in lex order, calling yield with each
// Object. Returning false from yield stops iteration early. Directory-marker
// objects (keys terminating at internal nodes) are emitted too.
func (t *Tree) Export(yield func(Object) bool) {
	t.exportFromNode(rootID, "", yield)
}

func (t *Tree) exportFromNode(id uint32, prefix string, yield func(Object) bool) bool {
	n := t.at(id)
	fullKey := prefix + n.edge
	if n.file != nil {
		if !yield(Object{Key: fullKey, Size: n.file.Size, Class: n.file.Class}) {
			return false
		}
	}
	for _, cid := range n.children {
		if !t.exportFromNode(cid, fullKey, yield) {
			return false
		}
	}
	return true
}

func writeHeader(w io.Writer, maxID, aliveCount uint32) error {
	var buf [snapshotHdrSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], snapshotMagic)
	binary.LittleEndian.PutUint16(buf[4:6], snapshotVersion)
	binary.LittleEndian.PutUint16(buf[6:8], 0) // flags
	binary.LittleEndian.PutUint32(buf[8:12], maxID)
	binary.LittleEndian.PutUint32(buf[12:16], aliveCount)
	binary.LittleEndian.PutUint32(buf[16:20], rootID)
	_, err := w.Write(buf[:])
	return err
}

func readHeader(r io.Reader) (maxID, aliveCount uint32, err error) {
	var buf [snapshotHdrSize]byte
	if _, err = io.ReadFull(r, buf[:]); err != nil {
		return 0, 0, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != snapshotMagic {
		return 0, 0, ErrSnapshotMagic
	}
	if binary.LittleEndian.Uint16(buf[4:6]) != snapshotVersion {
		return 0, 0, ErrSnapshotVersion
	}
	maxID = binary.LittleEndian.Uint32(buf[8:12])
	aliveCount = binary.LittleEndian.Uint32(buf[12:16])
	if binary.LittleEndian.Uint32(buf[16:20]) != rootID {
		return 0, 0, ErrSnapshotCorrupt
	}
	return maxID, aliveCount, nil
}

// writeNodeRecord serialises a single node into w. Layout matches the
// per-record block documented at the top of this file.
func writeNodeRecord(w *bufio.Writer, id uint32, n *node) error {
	var flags uint8
	if n.file != nil {
		flags |= snapFlagHasFile
	}
	if n.agg != nil {
		flags |= snapFlagHasAgg
	}

	// id + flags + edge_len + children_n
	var hdr [9]byte
	binary.LittleEndian.PutUint32(hdr[0:4], id)
	hdr[4] = flags
	binary.LittleEndian.PutUint16(hdr[5:7], uint16(len(n.edge)))
	binary.LittleEndian.PutUint16(hdr[7:9], uint16(len(n.children)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.WriteString(n.edge); err != nil {
		return err
	}
	if len(n.children) > 0 {
		var tmp [4]byte
		for _, cid := range n.children {
			binary.LittleEndian.PutUint32(tmp[:], cid)
			if _, err := w.Write(tmp[:]); err != nil {
				return err
			}
		}
	}
	if n.file != nil {
		var fb [9]byte
		fb[0] = uint8(n.file.Class)
		binary.LittleEndian.PutUint64(fb[1:9], uint64(n.file.Size))
		if _, err := w.Write(fb[:]); err != nil {
			return err
		}
	}
	if n.agg != nil {
		var ah [9]byte
		binary.LittleEndian.PutUint64(ah[0:8], uint64(n.agg.Objects))
		ah[8] = uint8(len(n.agg.Bytes))
		if _, err := w.Write(ah[:]); err != nil {
			return err
		}
		var eb [9]byte
		for _, cb := range n.agg.Bytes {
			eb[0] = uint8(cb.Class)
			binary.LittleEndian.PutUint64(eb[1:9], uint64(cb.Size))
			if _, err := w.Write(eb[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// readNodeRecord parses a single record from r and returns its ID and the
// reconstructed node.
func readNodeRecord(r *bufio.Reader) (uint32, node, error) {
	var hdr [9]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, node{}, err
	}
	id := binary.LittleEndian.Uint32(hdr[0:4])
	flags := hdr[4]
	edgeLen := binary.LittleEndian.Uint16(hdr[5:7])
	childrenN := binary.LittleEndian.Uint16(hdr[7:9])

	var n node
	if edgeLen > 0 {
		edgeBuf := make([]byte, edgeLen)
		if _, err := io.ReadFull(r, edgeBuf); err != nil {
			return 0, node{}, err
		}
		n.edge = string(edgeBuf)
	}
	if childrenN > 0 {
		n.children = make([]uint32, childrenN)
		var tmp [4]byte
		for i := range n.children {
			if _, err := io.ReadFull(r, tmp[:]); err != nil {
				return 0, node{}, err
			}
			n.children[i] = binary.LittleEndian.Uint32(tmp[:])
		}
	}
	if flags&snapFlagHasFile != 0 {
		var fb [9]byte
		if _, err := io.ReadFull(r, fb[:]); err != nil {
			return 0, node{}, err
		}
		n.file = &ClassByte{
			Class: StorageClass(fb[0]),
			Size:  int64(binary.LittleEndian.Uint64(fb[1:9])),
		}
	}
	if flags&snapFlagHasAgg != 0 {
		var ah [9]byte
		if _, err := io.ReadFull(r, ah[:]); err != nil {
			return 0, node{}, err
		}
		agg := &Aggregate{Objects: int64(binary.LittleEndian.Uint64(ah[0:8]))}
		aggN := ah[8]
		if aggN > 0 {
			agg.Bytes = make(ClassBytes, aggN)
			var eb [9]byte
			for i := range aggN {
				if _, err := io.ReadFull(r, eb[:]); err != nil {
					return 0, node{}, err
				}
				agg.Bytes[i] = ClassByte{
					Class: StorageClass(eb[0]),
					Size:  int64(binary.LittleEndian.Uint64(eb[1:9])),
				}
			}
		}
		n.agg = agg
	}
	return id, n, nil
}
