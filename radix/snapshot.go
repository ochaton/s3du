package radix

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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

// Save writes the tree's binary snapshot to w.
//
// The on-disk format is unchanged from v1: a single contiguous ID space, one
// record per alive node, internals expressed as nodes-with-children, leaves
// expressed as nodes-with-file-and-no-children. Internals serialise first
// (so the root keeps disk ID 0), leaves second. Child references in internal
// records are rewritten via per-arena disk-ID mappings.
//
// To skip free slots without allocating O(nextX) presence bitmaps, Save
// makes a sorted copy of each freelist (typically tiny — only grows on
// prune-heavy deletes) and walks it in lock-step with the slot iteration.
func (t *Tree) Save(w io.Writer) error {
	bw := bufio.NewWriter(w)

	sortedFreeInt := append([]uint32(nil), t.freeInt...)
	slices.Sort(sortedFreeInt)
	sortedFreeLeaf := append([]uint32(nil), t.freeLeaf...)
	slices.Sort(sortedFreeLeaf)

	nInt := t.nextInt - uint32(len(sortedFreeInt))
	nLeaf := t.nextLeaf - uint32(len(sortedFreeLeaf))
	total := nInt + nLeaf

	// Build arena-slot → disk-ID maps. Internals occupy [0, nInt); leaves
	// occupy [nInt, total). Root (internal arena slot 0) maps to disk 0.
	intDiskID := make([]uint32, t.nextInt)
	{
		fi := 0
		var next uint32
		for id := uint32(0); id < t.nextInt; id++ {
			if fi < len(sortedFreeInt) && sortedFreeInt[fi] == id {
				fi++
				continue
			}
			intDiskID[id] = next
			next++
		}
	}
	leafDiskID := make([]uint32, t.nextLeaf)
	{
		fl := 0
		next := nInt
		for id := uint32(0); id < t.nextLeaf; id++ {
			if fl < len(sortedFreeLeaf) && sortedFreeLeaf[fl] == id {
				fl++
				continue
			}
			leafDiskID[id] = next
			next++
		}
	}

	var maxID uint32
	if total > 0 {
		maxID = total - 1
	}
	if err := writeHeader(bw, maxID, total); err != nil {
		return err
	}

	mapChild := func(cid uint32) uint32 {
		if isLeafID(cid) {
			return leafDiskID[cid&idMask]
		}
		return intDiskID[cid]
	}

	fi := 0
	for id := uint32(0); id < t.nextInt; id++ {
		if fi < len(sortedFreeInt) && sortedFreeInt[fi] == id {
			fi++
			continue
		}
		if err := t.writeInternalRecord(bw, intDiskID[id], id, t.atInternal(id), mapChild); err != nil {
			return fmt.Errorf("radix: write internal %d: %w", id, err)
		}
	}
	fl := 0
	for id := uint32(0); id < t.nextLeaf; id++ {
		if fl < len(sortedFreeLeaf) && sortedFreeLeaf[fl] == id {
			fl++
			continue
		}
		if err := t.writeLeafRecord(bw, leafDiskID[id], t.atLeaf(id)); err != nil {
			return fmt.Errorf("radix: write leaf %d: %w", id, err)
		}
	}
	return bw.Flush()
}

// loadReadBuffer is the bufio buffer size used by Load. 64 KB amortises the
// per-call overhead of bufio's underlying Read across many record bytes.
const loadReadBuffer = 64 << 10

// Load reads a snapshot previously produced by Save (or any prior v1 writer)
// and reconstructs the two-arena in-memory tree.
//
// Each record is classified into the internal or leaf arena as it is read
// (a record is a leaf iff disk-ID != 0, file != nil, no children, no agg).
// Children references are stored temporarily as disk IDs and rewritten in a
// second pass via diskToCID once every record has been placed.
//
// Memory cost during load: arenas + diskToCID (4 B × maxID+1). At 50 M
// objects diskToCID is ~300 MB, an order of magnitude smaller than the tree
// itself.
func Load(r io.Reader) (*Tree, error) {
	br := bufio.NewReaderSize(r, loadReadBuffer)
	maxID, aliveCount, err := readHeader(br)
	if err != nil {
		return nil, err
	}

	t := &Tree{}
	// Reserve internal slot 0 for the root. The root's actual edge/agg/file
	// come from the disk-id=0 record below (which will overwrite the slot).
	t.allocInternal(internal{})

	nextID := maxID + 1
	diskToCID := make([]uint32, nextID)
	seen := make([]bool, nextID)
	rootSeen := false

	for i := range aliveCount {
		rec, err := readNodeRecord(br)
		if err != nil {
			return nil, fmt.Errorf("radix: read record %d: %w", i, err)
		}
		if rec.id >= nextID || seen[rec.id] {
			return nil, ErrSnapshotCorrupt
		}
		seen[rec.id] = true

		isLeaf := rec.id != 0 && rec.file != nil && len(rec.children) == 0 && rec.agg == nil
		if isLeaf {
			cid := t.allocLeaf(leaf{
				edge:      t.allocEdge(rec.edge),
				sizeClass: packSizeClass(rec.file.Class, rec.file.Size),
			})
			diskToCID[rec.id] = cid
			continue
		}

		var memID uint32
		if rec.id == 0 {
			memID = rootID
			rootSeen = true
		} else {
			memID = t.allocInternal(internal{})
		}
		n := t.atInternal(memID)
		n.edge = t.allocEdge(rec.edge)
		n.agg = rec.agg
		n.children = rec.children // disk IDs; rewritten below
		if rec.file != nil {
			t.setDirMarker(memID, *rec.file)
		}
		diskToCID[rec.id] = memID
	}

	if aliveCount > 0 && !rootSeen {
		return nil, ErrSnapshotCorrupt
	}

	// Second pass: rewrite each internal.children using diskToCID.
	for cIdx := range t.internals {
		chunk := t.internals[cIdx]
		for slot := range chunk {
			id := uint32(cIdx<<chunkBits | slot)
			if id >= t.nextInt {
				break
			}
			n := &chunk[slot]
			for j, c := range n.children {
				if c >= nextID || !seen[c] {
					return nil, ErrSnapshotCorrupt
				}
				n.children[j] = diskToCID[c]
			}
		}
	}

	return t, nil
}

// SaveFile is a convenience wrapper around [Tree.Save] that writes to path
// atomically: the snapshot is first written to path+".tmp" and renamed
// into place on success, so an interrupted run never leaves a half-written
// file at the destination. Parent directories are created with mode 0755.
func (t *Tree) SaveFile(path string) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if err = t.Save(f); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadFile is a convenience wrapper around [Load] that opens path, parses
// the snapshot, and returns the resulting tree. The file is closed before
// returning regardless of success.
func LoadFile(path string) (*Tree, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

// Export walks every alive object (regular leaves plus S3 directory-marker
// dir-marker files held on internals) in lex order, calling yield with each
// Object. Returning false from yield stops iteration early.
func (t *Tree) Export(yield func(Object) bool) {
	t.exportFromInternal(rootID, "", yield)
}

func (t *Tree) exportFromInternal(id uint32, prefix string, yield func(Object) bool) bool {
	n := t.atInternal(id)
	fullKey := prefix + t.edgeStr(n.edge)
	if cb, ok := t.dirMarker(id); ok {
		if !yield(Object{Key: fullKey, Size: cb.Size, Class: cb.Class}) {
			return false
		}
	}
	for _, cid := range n.children {
		if isLeafID(cid) {
			lf := t.atLeaf(cid & idMask)
			if !yield(Object{Key: fullKey + t.edgeStr(lf.edge), Size: lf.size(), Class: lf.class()}) {
				return false
			}
			continue
		}
		if !t.exportFromInternal(cid, fullKey, yield) {
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

// writeInternalRecord serialises an internal node as a v1-format record,
// rewriting child references through mapChild so they refer to disk IDs.
// arenaID is the in-memory arena slot — used to look up any dir-marker
// pinned at this internal.
func (t *Tree) writeInternalRecord(w *bufio.Writer, diskID uint32, arenaID uint32, n *internal, mapChild func(uint32) uint32) error {
	marker, hasMarker := t.dirMarker(arenaID)
	var flags uint8
	if hasMarker {
		flags |= snapFlagHasFile
	}
	if n.agg != nil {
		flags |= snapFlagHasAgg
	}
	var hdr [9]byte
	binary.LittleEndian.PutUint32(hdr[0:4], diskID)
	hdr[4] = flags
	binary.LittleEndian.PutUint16(hdr[5:7], uint16(n.edge.length))
	binary.LittleEndian.PutUint16(hdr[7:9], uint16(len(n.children)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if n.edge.length > 0 {
		chunk := n.edge.off >> edgeChunkBits
		within := n.edge.off & edgeChunkMask
		if _, err := w.Write(t.edgeArena[chunk][within : within+n.edge.length]); err != nil {
			return err
		}
	}
	if len(n.children) > 0 {
		var tmp [4]byte
		for _, cid := range n.children {
			binary.LittleEndian.PutUint32(tmp[:], mapChild(cid))
			if _, err := w.Write(tmp[:]); err != nil {
				return err
			}
		}
	}
	if hasMarker {
		var fb [9]byte
		fb[0] = uint8(marker.Class)
		binary.LittleEndian.PutUint64(fb[1:9], uint64(marker.Size))
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

// writeLeafRecord serialises a leaf as a v1-format record (no children, no
// agg, hasFile). The packed sizeClass is unpacked back into (class, size)
// for on-disk compatibility.
func (t *Tree) writeLeafRecord(w *bufio.Writer, id uint32, l *leaf) error {
	var hdr [9]byte
	binary.LittleEndian.PutUint32(hdr[0:4], id)
	hdr[4] = snapFlagHasFile
	binary.LittleEndian.PutUint16(hdr[5:7], uint16(l.edge.length))
	binary.LittleEndian.PutUint16(hdr[7:9], 0)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if l.edge.length > 0 {
		chunk := l.edge.off >> edgeChunkBits
		within := l.edge.off & edgeChunkMask
		if _, err := w.Write(t.edgeArena[chunk][within : within+l.edge.length]); err != nil {
			return err
		}
	}
	var fb [9]byte
	fb[0] = uint8(l.class())
	binary.LittleEndian.PutUint64(fb[1:9], uint64(l.size()))
	_, err := w.Write(fb[:])
	return err
}

// loadedRecord is the parsed form of a single v1 node record. Its children
// references are still disk IDs at parse time; the caller of readNodeRecord
// classifies the record into an internal or a leaf and (for internals)
// rewrites children via the diskToCID map after every record is in place.
type loadedRecord struct {
	id       uint32
	edge     string
	children []uint32
	file     *ClassByte
	agg      *Aggregate
}

// readNodeRecord parses a single v1-format record from r.
//
// Variable-sized sub-blocks (edge bytes, children IDs, agg entries) are
// read via bufio.Reader.Peek so the decoder operates directly on the
// reader's internal buffer — no scratch []byte allocations per record. The
// reader must therefore be sized large enough to hold any single sub-block
// (loadReadBuffer = 64 KB covers the uint16-limited edge length).
func readNodeRecord(r *bufio.Reader) (loadedRecord, error) {
	var rec loadedRecord
	var hdr [9]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return rec, err
	}
	rec.id = binary.LittleEndian.Uint32(hdr[0:4])
	flags := hdr[4]
	edgeLen := binary.LittleEndian.Uint16(hdr[5:7])
	childrenN := binary.LittleEndian.Uint16(hdr[7:9])

	if edgeLen > 0 {
		buf, err := r.Peek(int(edgeLen))
		if err != nil {
			return rec, err
		}
		rec.edge = string(buf) // copies bytes into the string's own backing
		if _, err := r.Discard(int(edgeLen)); err != nil {
			return rec, err
		}
	}
	if childrenN > 0 {
		nb := int(childrenN) * 4
		buf, err := r.Peek(nb)
		if err != nil {
			return rec, err
		}
		rec.children = make([]uint32, childrenN)
		for i := range rec.children {
			rec.children[i] = binary.LittleEndian.Uint32(buf[i*4:])
		}
		if _, err := r.Discard(nb); err != nil {
			return rec, err
		}
	}
	if flags&snapFlagHasFile != 0 {
		var fb [9]byte
		if _, err := io.ReadFull(r, fb[:]); err != nil {
			return rec, err
		}
		rec.file = &ClassByte{
			Class: StorageClass(fb[0]),
			Size:  int64(binary.LittleEndian.Uint64(fb[1:9])),
		}
	}
	if flags&snapFlagHasAgg != 0 {
		var ah [9]byte
		if _, err := io.ReadFull(r, ah[:]); err != nil {
			return rec, err
		}
		agg := &Aggregate{Objects: int64(binary.LittleEndian.Uint64(ah[0:8]))}
		aggN := ah[8]
		if aggN > 0 {
			nb := int(aggN) * 9
			buf, err := r.Peek(nb)
			if err != nil {
				return rec, err
			}
			agg.Bytes = make(ClassBytes, aggN)
			for i := range agg.Bytes {
				off := i * 9
				agg.Bytes[i] = ClassByte{
					Class: StorageClass(buf[off]),
					Size:  int64(binary.LittleEndian.Uint64(buf[off+1:])),
				}
			}
			if _, err := r.Discard(nb); err != nil {
				return rec, err
			}
		}
		rec.agg = agg
	}
	return rec, nil
}
