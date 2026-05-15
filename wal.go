package main

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

const sortChunkBytes = 256 * 1024 * 1024 // 256 MB per sort chunk

// ── WAL record binary format ──────────────────────────────────────────────────
// [2] parentPrefixLen
// [n] parentPrefix
// [2] nameLen
// [n] name
// [8] sizeBytes (little-endian)
// [1] scLen
// [n] storageClass

type walRecord struct {
	parentPrefix string
	name         string
	sizeBytes    int64
	storageClass string
}

func (r walRecord) approxBytes() int {
	return 2 + len(r.parentPrefix) + 2 + len(r.name) + 8 + 1 + len(r.storageClass)
}

// WriteWAL drains filesChan and writes every record to walPath sequentially.
// Zero in-memory accumulation — data goes directly to disk.
func WriteWAL(walPath string, filesChan <-chan taggedFile) error {
	f, err := os.Create(walPath)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20) // 1 MB write buffer
	le := binary.LittleEndian
	b2 := make([]byte, 2)
	b8 := make([]byte, 8)

	for tf := range filesChan {
		p, fe := tf.ParentPrefix, tf.FileEntry
		le.PutUint16(b2, uint16(len(p)))
		w.Write(b2)
		w.WriteString(p)
		le.PutUint16(b2, uint16(len(fe.Name)))
		w.Write(b2)
		w.WriteString(fe.Name)
		le.PutUint64(b8, uint64(fe.SizeBytes))
		w.Write(b8)
		w.WriteByte(byte(len(fe.StorageClass)))
		w.WriteString(fe.StorageClass)
	}
	return w.Flush()
}

// BuildObjectsBin reads walPath, externally sorts by parentPrefix,
// and writes the indexed objects.bin to outPath.
// Peak RAM = one sort chunk (sortChunkBytes) + one record per chunk during merge.
func BuildObjectsBin(walPath, outPath string) error {
	chunkPaths, err := splitAndSort(walPath)
	if err != nil {
		return fmt.Errorf("sort: %w", err)
	}
	defer func() {
		for _, p := range chunkPaths {
			os.Remove(p)
		}
	}()
	return mergeToObjectsBin(chunkPaths, outPath)
}

// ── Phase 1: split WAL into sorted chunks ────────────────────────────────────

func splitAndSort(walPath string) ([]string, error) {
	f, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	var chunks []string
	var buf []walRecord
	bufBytes := 0

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		sort.Slice(buf, func(i, j int) bool {
			return buf[i].parentPrefix < buf[j].parentPrefix
		})
		tmp, err := os.CreateTemp("", "s3du-chunk-*.bin")
		if err != nil {
			return err
		}
		w := bufio.NewWriterSize(tmp, 1<<20)
		for _, rec := range buf {
			if err := writeWALRecord(w, rec); err != nil {
				tmp.Close()
				return err
			}
		}
		if err := w.Flush(); err != nil {
			tmp.Close()
			return err
		}
		tmp.Close()
		chunks = append(chunks, tmp.Name())
		buf = buf[:0]
		bufBytes = 0
		return nil
	}

	for {
		rec, err := readWALRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		buf = append(buf, rec)
		bufBytes += rec.approxBytes()
		if bufBytes >= sortChunkBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return chunks, nil
}

// ── Phase 2: k-way merge → objects.bin ───────────────────────────────────────

func mergeToObjectsBin(chunkPaths []string, outPath string) error {
	// Open all chunk readers
	readers := make([]*chunkReader, 0, len(chunkPaths))
	for _, p := range chunkPaths {
		cr, err := newChunkReader(p)
		if err != nil {
			return err
		}
		if cr != nil {
			readers = append(readers, cr)
		}
	}
	defer func() {
		for _, cr := range readers {
			cr.f.Close()
		}
	}()

	h := &mergeHeap{}
	heap.Init(h)
	for _, cr := range readers {
		if !cr.done {
			heap.Push(h, cr)
		}
	}

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	return writeIndexedObjectsBin(out, h)
}

// writeIndexedObjectsBin pulls records from the merge heap in sorted order
// and writes objects.bin with a full index.
func writeIndexedObjectsBin(out *os.File, h *mergeHeap) error {
	le := binary.LittleEndian

	// First pass: collect all records grouped by parentPrefix to build index.
	// We stream through the sorted heap once and buffer per-prefix groups.
	// Since records arrive sorted, a group is complete once prefix changes.
	// We write groups sequentially and record offsets.

	type section struct {
		prefix string
		count  uint32
		offset int64 // relative to data section start
	}

	// Placeholder: write header (12 bytes) + index later (we don't know size yet).
	// Strategy: collect index in memory (small: one entry per unique prefix),
	// buffer data in a temp file, then write final output = header + index + data.

	dataTmp, err := os.CreateTemp("", "s3du-data-*.bin")
	if err != nil {
		return err
	}
	defer os.Remove(dataTmp.Name())
	defer dataTmp.Close()

	dw := bufio.NewWriterSize(dataTmp, 1<<20)
	b2 := make([]byte, 2)
	b8 := make([]byte, 8)

	var sections []section
	var dataOffset int64
	var curPrefix string
	var curCount uint32
	var curStart int64

	commitSection := func() {
		if curPrefix != "" || curCount > 0 {
			sections = append(sections, section{
				prefix: curPrefix,
				count:  curCount,
				offset: curStart,
			})
		}
	}

	writeFileEntry := func(rec walRecord) error {
		le.PutUint16(b2, uint16(len(rec.name)))
		dw.Write(b2)
		dw.WriteString(rec.name)
		le.PutUint64(b8, uint64(rec.sizeBytes))
		dw.Write(b8)
		dw.WriteByte(byte(len(rec.storageClass)))
		dw.WriteString(rec.storageClass)
		n := int64(2 + len(rec.name) + 8 + 1 + len(rec.storageClass))
		dataOffset += n
		curCount++
		return nil
	}

	for h.Len() > 0 {
		cr := heap.Pop(h).(*chunkReader)
		rec := cr.current

		if rec.parentPrefix != curPrefix {
			commitSection()
			curPrefix = rec.parentPrefix
			curCount = 0
			curStart = dataOffset
		}
		writeFileEntry(rec)

		if err := cr.advance(); err == nil && !cr.done {
			heap.Push(h, cr)
		}
	}
	commitSection()

	if err := dw.Flush(); err != nil {
		return err
	}

	// Compute index size
	indexSize := int64(0)
	for _, s := range sections {
		indexSize += int64(2 + len(s.prefix) + 4 + 8)
	}

	// Write header + index to output
	ow := bufio.NewWriterSize(out, 1<<20)
	ow.WriteString(objectsMagic)
	b4 := make([]byte, 4)
	le.PutUint32(b4, objectsVersion)
	ow.Write(b4)
	le.PutUint32(b4, uint32(len(sections)))
	ow.Write(b4)

	dataStart := int64(12) + indexSize
	for _, s := range sections {
		le.PutUint16(b2, uint16(len(s.prefix)))
		ow.Write(b2)
		ow.WriteString(s.prefix)
		le.PutUint32(b4, s.count)
		ow.Write(b4)
		le.PutUint64(b8, uint64(s.offset))
		ow.Write(b8)
	}
	if err := ow.Flush(); err != nil {
		return err
	}

	// Append data from temp file
	if _, err := dataTmp.Seek(0, 0); err != nil {
		return err
	}
	if _, err := io.Copy(out, dataTmp); err != nil {
		return err
	}
	_ = dataStart
	return nil
}

// ── WAL record read/write helpers ─────────────────────────────────────────────

func writeWALRecord(w *bufio.Writer, rec walRecord) error {
	le := binary.LittleEndian
	b2 := make([]byte, 2)
	b8 := make([]byte, 8)
	le.PutUint16(b2, uint16(len(rec.parentPrefix)))
	w.Write(b2)
	w.WriteString(rec.parentPrefix)
	le.PutUint16(b2, uint16(len(rec.name)))
	w.Write(b2)
	w.WriteString(rec.name)
	le.PutUint64(b8, uint64(rec.sizeBytes))
	w.Write(b8)
	w.WriteByte(byte(len(rec.storageClass)))
	w.WriteString(rec.storageClass)
	return nil
}

func readWALRecord(r *bufio.Reader) (walRecord, error) {
	le := binary.LittleEndian
	b2 := make([]byte, 2)
	b8 := make([]byte, 8)

	if _, err := io.ReadFull(r, b2); err != nil {
		if err == io.ErrUnexpectedEOF {
			return walRecord{}, io.EOF
		}
		return walRecord{}, err
	}
	pBuf := make([]byte, le.Uint16(b2))
	if _, err := io.ReadFull(r, pBuf); err != nil {
		return walRecord{}, err
	}
	if _, err := io.ReadFull(r, b2); err != nil {
		return walRecord{}, err
	}
	nBuf := make([]byte, le.Uint16(b2))
	if _, err := io.ReadFull(r, nBuf); err != nil {
		return walRecord{}, err
	}
	if _, err := io.ReadFull(r, b8); err != nil {
		return walRecord{}, err
	}
	size := int64(le.Uint64(b8))
	scLen := make([]byte, 1)
	if _, err := io.ReadFull(r, scLen); err != nil {
		return walRecord{}, err
	}
	scBuf := make([]byte, scLen[0])
	if _, err := io.ReadFull(r, scBuf); err != nil {
		return walRecord{}, err
	}
	return walRecord{
		parentPrefix: string(pBuf),
		name:         string(nBuf),
		sizeBytes:    size,
		storageClass: string(scBuf),
	}, nil
}

// ── Merge heap ────────────────────────────────────────────────────────────────

type chunkReader struct {
	f       *os.File
	r       *bufio.Reader
	current walRecord
	done    bool
}

func newChunkReader(path string) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	cr := &chunkReader{f: f, r: bufio.NewReaderSize(f, 1<<20)}
	if err := cr.advance(); err != nil {
		f.Close()
		if err == io.EOF {
			return nil, nil // empty chunk
		}
		return nil, err
	}
	return cr, nil
}

func (cr *chunkReader) advance() error {
	rec, err := readWALRecord(cr.r)
	if err == io.EOF {
		cr.done = true
		return io.EOF
	}
	if err != nil {
		return err
	}
	cr.current = rec
	return nil
}

type mergeHeap []*chunkReader

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	return h[i].current.parentPrefix < h[j].current.parentPrefix
}
func (h *mergeHeap) Push(x any) { *h = append(*h, x.(*chunkReader)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
