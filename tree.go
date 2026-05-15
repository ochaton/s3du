package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	treeMagic   = "S3DT"
	treeVersion = uint32(1)
)

// SCSize holds per-storage-class subtree stats.
type SCSize struct {
	SC    string
	Count int64
	Size  int64
}

// DirSection holds subtree stats for one directory prefix.
// Everything is loaded into memory; no file handle needed after OpenTreeIndex.
type DirSection struct {
	Count   int64
	Size    int64
	SCSizes []SCSize
}

func (d DirSection) monthlyCost(region string) float64 {
	var total float64
	for _, s := range d.SCSizes {
		total += monthlyStorageCost(s.Size, s.SC, region)
	}
	return total
}

func treePath(dir string) string { return filepath.Join(dir, "tree.bin") }

// BuildTreeBin builds tree.bin from an existing objects.bin.
// Zero in-RAM file accumulation during scan — all stats are derived here in post-processing.
func BuildTreeBin(objPath, treeFilePath string) error {
	objF, err := os.Open(objPath)
	if err != nil {
		return fmt.Errorf("open objects.bin: %w", err)
	}
	defer objF.Close()

	rawIndex, dataStart, err := readObjectsIndex(objF)
	if err != nil {
		return fmt.Errorf("read objects index: %w", err)
	}
	for k, sec := range rawIndex {
		sec.Offset += dataStart
		rawIndex[k] = sec
	}

	// Collect all prefixes from index + every ancestor path.
	prefixSet := make(map[string]struct{}, len(rawIndex)*2+1)
	prefixSet[""] = struct{}{}
	for p := range rawIndex {
		cur := p
		for {
			if _, ok := prefixSet[cur]; ok {
				break
			}
			prefixSet[cur] = struct{}{}
			par := parentPrefix(cur)
			if par == cur {
				break
			}
			cur = par
		}
	}

	sortedPrefixes := make([]string, 0, len(prefixSet))
	for p := range prefixSet {
		sortedPrefixes = append(sortedPrefixes, p)
	}
	sort.Strings(sortedPrefixes)

	// Per-node subtree stats (direct stats copied in first, then aggregated up).
	type scStats struct {
		count int64
		size  int64
	}
	type nodeStats struct {
		subCount int64
		subSize  int64
		bySC     map[string]*scStats
	}

	nodes := make(map[string]*nodeStats, len(sortedPrefixes))
	for _, p := range sortedPrefixes {
		nodes[p] = &nodeStats{bySC: make(map[string]*scStats)}
	}

	// Populate direct stats from objects.bin file entries.
	for p, sec := range rawIndex {
		n := nodes[p]
		files, err := ReadFilesForPrefix(objF, rawIndex, p)
		if err != nil {
			return fmt.Errorf("read files for %q: %w", p, err)
		}
		_ = sec
		for _, fe := range files {
			n.subCount++
			n.subSize += fe.SizeBytes
			s := n.bySC[fe.StorageClass]
			if s == nil {
				n.bySC[fe.StorageClass] = &scStats{}
				s = n.bySC[fe.StorageClass]
			}
			s.count++
			s.size += fe.SizeBytes
		}
	}

	// Bottom-up aggregation: process reverse sorted (deepest first).
	for i := len(sortedPrefixes) - 1; i >= 0; i-- {
		p := sortedPrefixes[i]
		par := parentPrefix(p)
		if par == p {
			continue // root
		}
		n := nodes[p]
		pn := nodes[par]
		if pn == nil {
			continue
		}
		pn.subCount += n.subCount
		pn.subSize += n.subSize
		for sc, s := range n.bySC {
			ps := pn.bySC[sc]
			if ps == nil {
				pn.bySC[sc] = &scStats{}
				ps = pn.bySC[sc]
			}
			ps.count += s.count
			ps.size += s.size
		}
	}

	// Write tree.bin.
	out, err := os.Create(treeFilePath)
	if err != nil {
		return err
	}
	defer out.Close()

	le := binary.LittleEndian
	bw := bufio.NewWriterSize(out, 1<<20)
	b2 := make([]byte, 2)
	b4 := make([]byte, 4)
	b8 := make([]byte, 8)

	bw.WriteString(treeMagic)
	le.PutUint32(b4, treeVersion)
	bw.Write(b4)
	le.PutUint32(b4, uint32(len(sortedPrefixes)))
	bw.Write(b4)

	for _, p := range sortedPrefixes {
		n := nodes[p]

		// Sort storage classes for deterministic output.
		scKeys := make([]string, 0, len(n.bySC))
		for sc := range n.bySC {
			scKeys = append(scKeys, sc)
		}
		sort.Strings(scKeys)

		le.PutUint16(b2, uint16(len(p)))
		bw.Write(b2)
		bw.WriteString(p)
		le.PutUint64(b8, uint64(n.subCount))
		bw.Write(b8)
		le.PutUint64(b8, uint64(n.subSize))
		bw.Write(b8)
		bw.WriteByte(byte(len(scKeys)))
		for _, sc := range scKeys {
			s := n.bySC[sc]
			bw.WriteByte(byte(len(sc)))
			bw.WriteString(sc)
			le.PutUint64(b8, uint64(s.count))
			bw.Write(b8)
			le.PutUint64(b8, uint64(s.size))
			bw.Write(b8)
		}
	}

	return bw.Flush()
}

// OpenTreeIndex reads tree.bin entirely into memory and returns the index.
func OpenTreeIndex(bucket, region string) (map[string]DirSection, error) {
	dir, err := cacheDir(bucket, region)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(treePath(dir))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readTreeIndex(f)
}

func readTreeIndex(f *os.File) (map[string]DirSection, error) {
	le := binary.LittleEndian
	b2 := make([]byte, 2)
	b4 := make([]byte, 4)
	b8 := make([]byte, 8)

	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, err
	}
	if string(magic) != treeMagic {
		return nil, fmt.Errorf("invalid tree.bin magic")
	}
	if _, err := io.ReadFull(f, b4); err != nil {
		return nil, err
	}
	if le.Uint32(b4) != treeVersion {
		return nil, fmt.Errorf("unsupported tree.bin version")
	}
	if _, err := io.ReadFull(f, b4); err != nil {
		return nil, err
	}
	numEntries := int(le.Uint32(b4))

	index := make(map[string]DirSection, numEntries)
	for i := 0; i < numEntries; i++ {
		if _, err := io.ReadFull(f, b2); err != nil {
			return nil, err
		}
		pBuf := make([]byte, le.Uint16(b2))
		if _, err := io.ReadFull(f, pBuf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(f, b8); err != nil {
			return nil, err
		}
		count := int64(le.Uint64(b8))
		if _, err := io.ReadFull(f, b8); err != nil {
			return nil, err
		}
		size := int64(le.Uint64(b8))

		numClasses := make([]byte, 1)
		if _, err := io.ReadFull(f, numClasses); err != nil {
			return nil, err
		}
		scs := make([]SCSize, numClasses[0])
		for j := range scs {
			scLen := make([]byte, 1)
			if _, err := io.ReadFull(f, scLen); err != nil {
				return nil, err
			}
			scBuf := make([]byte, scLen[0])
			if _, err := io.ReadFull(f, scBuf); err != nil {
				return nil, err
			}
			if _, err := io.ReadFull(f, b8); err != nil {
				return nil, err
			}
			scCount := int64(le.Uint64(b8))
			if _, err := io.ReadFull(f, b8); err != nil {
				return nil, err
			}
			scSize := int64(le.Uint64(b8))
			scs[j] = SCSize{SC: string(scBuf), Count: scCount, Size: scSize}
		}
		index[string(pBuf)] = DirSection{Count: count, Size: size, SCSizes: scs}
	}
	return index, nil
}
