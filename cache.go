package main

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	cacheVersion   = 3
	objectsMagic   = "S3DU"
	objectsVersion = uint32(1)
)

// CacheFile stores scan metadata only; actual stats live in tree.bin and objects.bin.
type CacheFile struct {
	Version      int       `json:"version"`
	Bucket       string    `json:"bucket"`
	Region       string    `json:"region"`
	ScannedAt    time.Time `json:"scanned_at"`
	ListRequests int64     `json:"list_requests"`
}

// ObjectSection locates a prefix's file entries in the objects.bin data section.
type ObjectSection struct {
	Offset int64
	Count  int32
}

// objects.bin layout:
//
//	[4]   magic "S3DU"
//	[4]   version uint32
//	[4]   num_sections uint32
//	INDEX (num_sections entries):
//	  [2]   prefix_len uint16
//	  [n]   prefix bytes
//	  [4]   file_count uint32
//	  [8]   data_offset int64   (relative to DATA section start)
//	DATA:
//	  per section, file_count times:
//	    [2]   name_len uint16
//	    [n]   name bytes
//	    [8]   size_bytes int64
//	    [1]   sc_len uint8
//	    [n]   storage_class bytes

func cacheDir(bucket, region string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".cache", "s3du", fmt.Sprintf("%s@%s", bucket, region))
	return dir, os.MkdirAll(dir, 0755)
}

func statsPath(dir string) string   { return filepath.Join(dir, "stats.json.gz") }
func objectsPath(dir string) string { return filepath.Join(dir, "objects.bin") }

// SaveCache writes scan metadata to stats.json.gz.
func SaveCache(bucket, region string, listRequests int64) (string, error) {
	dir, err := cacheDir(bucket, region)
	if err != nil {
		return "", err
	}
	cf := CacheFile{
		Version:      cacheVersion,
		Bucket:       bucket,
		Region:       region,
		ScannedAt:    time.Now(),
		ListRequests: listRequests,
	}
	if err := writeStatsGz(statsPath(dir), cf); err != nil {
		return "", fmt.Errorf("write stats: %w", err)
	}
	return dir, nil
}

// LoadStats reads stats.json.gz metadata.
func LoadStats(bucket, region string) (*CacheFile, error) {
	dir, err := cacheDir(bucket, region)
	if err != nil {
		return nil, err
	}
	return readStatsGz(statsPath(dir))
}

// OpenObjectsIndex opens objects.bin and loads the index into memory.
func OpenObjectsIndex(bucket, region string) (map[string]ObjectSection, *os.File, error) {
	dir, err := cacheDir(bucket, region)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(objectsPath(dir))
	if err != nil {
		return nil, nil, err
	}
	index, dataStart, err := readObjectsIndex(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	for k, sec := range index {
		sec.Offset += dataStart
		index[k] = sec
	}
	return index, f, nil
}

// ReadFilesForPrefix reads file entries for a given prefix from the open objects file.
func ReadFilesForPrefix(f *os.File, index map[string]ObjectSection, prefix string) ([]FileEntry, error) {
	sec, ok := index[prefix]
	if !ok || sec.Count == 0 {
		return nil, nil
	}
	if _, err := f.Seek(sec.Offset, 0); err != nil {
		return nil, err
	}
	entries := make([]FileEntry, 0, sec.Count)
	for i := int32(0); i < sec.Count; i++ {
		fe, err := readFileEntry(f)
		if err != nil {
			return entries, err
		}
		entries = append(entries, fe)
	}
	return entries, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func writeStatsGz(path string, cf CacheFile) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	if err := json.NewEncoder(gz).Encode(cf); err != nil {
		return err
	}
	return gz.Close()
}

func readStatsGz(path string) (*CacheFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	var cf CacheFile
	if err := json.NewDecoder(gz).Decode(&cf); err != nil {
		return nil, err
	}
	if cf.Version != cacheVersion {
		return nil, fmt.Errorf("cache version mismatch: got %d want %d", cf.Version, cacheVersion)
	}
	return &cf, nil
}

// readObjectsIndex reads the index section of objects.bin.
// Returns index with offsets relative to data section start, and the absolute data section start.
func readObjectsIndex(f *os.File) (map[string]ObjectSection, int64, error) {
	le := binary.LittleEndian
	buf4 := make([]byte, 4)
	buf2 := make([]byte, 2)
	buf8 := make([]byte, 8)

	magic := make([]byte, 4)
	if _, err := f.Read(magic); err != nil {
		return nil, 0, err
	}
	if string(magic) != objectsMagic {
		return nil, 0, fmt.Errorf("invalid objects.bin magic")
	}

	if _, err := f.Read(buf4); err != nil {
		return nil, 0, err
	}
	if le.Uint32(buf4) != objectsVersion {
		return nil, 0, fmt.Errorf("unsupported objects.bin version")
	}

	if _, err := f.Read(buf4); err != nil {
		return nil, 0, err
	}
	numSections := int(le.Uint32(buf4))

	index := make(map[string]ObjectSection, numSections)
	indexSize := int64(0)

	for i := 0; i < numSections; i++ {
		if _, err := f.Read(buf2); err != nil {
			return nil, 0, err
		}
		pLen := int(le.Uint16(buf2))
		pBuf := make([]byte, pLen)
		if _, err := f.Read(pBuf); err != nil {
			return nil, 0, err
		}
		if _, err := f.Read(buf4); err != nil {
			return nil, 0, err
		}
		count := int32(le.Uint32(buf4))
		if _, err := f.Read(buf8); err != nil {
			return nil, 0, err
		}
		offset := int64(le.Uint64(buf8))

		index[string(pBuf)] = ObjectSection{Offset: offset, Count: count}
		indexSize += int64(2 + pLen + 4 + 8)
	}

	dataStart := int64(12) + indexSize
	return index, dataStart, nil
}

func readFileEntry(f *os.File) (FileEntry, error) {
	le := binary.LittleEndian
	buf2 := make([]byte, 2)
	buf8 := make([]byte, 8)

	if _, err := f.Read(buf2); err != nil {
		return FileEntry{}, err
	}
	nameBuf := make([]byte, le.Uint16(buf2))
	if _, err := f.Read(nameBuf); err != nil {
		return FileEntry{}, err
	}
	if _, err := f.Read(buf8); err != nil {
		return FileEntry{}, err
	}
	size := int64(le.Uint64(buf8))

	scLen := make([]byte, 1)
	if _, err := f.Read(scLen); err != nil {
		return FileEntry{}, err
	}
	scBuf := make([]byte, scLen[0])
	if _, err := f.Read(scBuf); err != nil {
		return FileEntry{}, err
	}

	return FileEntry{Name: string(nameBuf), SizeBytes: size, StorageClass: string(scBuf)}, nil
}

