package radix

import (
	"bytes"
	"os"
	"runtime"
	"testing"
)

// realDataset is loaded once and reused across benchmarks.
var realDataset []Object

func loadRealDataset(b *testing.B) []Object {
	b.Helper()
	if realDataset != nil {
		return realDataset
	}
	const path = "../test.objects.jsonl"
	if _, err := os.Stat(path); err != nil {
		b.Skipf("skipping: %s not present", path)
	}
	realDataset = loadJSONL(b, path)
	return realDataset
}

// buildTree ingests the dataset in batches of the given size, chained via
// StartFrom to model real ListObjectsV2 pagination.
func buildTree(objs []Object, batchSize int) *Tree {
	tr := New()
	prev := ""
	for start := 0; start < len(objs); start += batchSize {
		end := min(start+batchSize, len(objs))
		_ = tr.AddBatch(Batch{StartFrom: prev, Objects: objs[start:end]})
		prev = objs[end-1].Key
	}
	return tr
}

func BenchmarkAddBatch1000(b *testing.B) {
	objs := loadRealDataset(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buildTree(objs, 1000)
	}
	b.ReportMetric(float64(len(objs))*float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkAddBatch100(b *testing.B) {
	objs := loadRealDataset(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buildTree(objs, 100)
	}
	b.ReportMetric(float64(len(objs))*float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkAddBatch10(b *testing.B) {
	objs := loadRealDataset(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buildTree(objs, 10)
	}
	b.ReportMetric(float64(len(objs))*float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

// BenchmarkSave measures snapshot write throughput on the loaded dataset.
func BenchmarkSave(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	var buf bytes.Buffer
	if err := tr.Save(&buf); err != nil {
		b.Fatalf("Save: %v", err)
	}
	b.SetBytes(int64(buf.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		_ = tr.Save(&buf)
	}
}

// BenchmarkLoad measures snapshot read throughput. The snapshot is built
// once outside the loop; each iteration reconstructs a fresh tree from the
// same bytes.
func BenchmarkLoad(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	var buf bytes.Buffer
	if err := tr.Save(&buf); err != nil {
		b.Fatalf("Save: %v", err)
	}
	snap := buf.Bytes()
	b.SetBytes(int64(len(snap)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := Load(bytes.NewReader(snap)); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

// BenchmarkAddBatchIdempotent stresses the cursor merge path: the tree is
// already built, the same dataset is re-batched, every key matches → no
// inserts/updates/deletes are issued, only cursor scanning + comparison.
func BenchmarkAddBatchIdempotent(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	const batchSize = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		prev := ""
		for start := 0; start < len(objs); start += batchSize {
			end := min(start+batchSize, len(objs))
			_ = tr.AddBatch(Batch{StartFrom: prev, Objects: objs[start:end]})
			prev = objs[end-1].Key
		}
	}
	b.ReportMetric(float64(len(objs))*float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkBuildHeap(b *testing.B) {
	objs := loadRealDataset(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		tr := buildTree(objs, 1000)
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "heap_bytes")
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(len(objs)), "bytes/object")
		_ = tr
	}
}

func BenchmarkListDirectoryRoot(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.ListDirectory("")
	}
}

func BenchmarkListDirectoryShallow(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	prefix := pickShallowPrefix(objs)
	b.Logf("prefix=%q", prefix)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.ListDirectory(prefix)
	}
}

func BenchmarkListDirectoryDeep(b *testing.B) {
	objs := loadRealDataset(b)
	tr := buildTree(objs, 1000)
	prefix := pickDeepPrefix(objs)
	b.Logf("prefix=%q", prefix)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.ListDirectory(prefix)
	}
}

// pickShallowPrefix returns the first one-segment prefix (e.g., "afk/").
func pickShallowPrefix(objs []Object) string {
	for _, o := range objs {
		for i := 0; i < len(o.Key); i++ {
			if o.Key[i] == '/' {
				return o.Key[:i+1]
			}
		}
	}
	return ""
}

// pickDeepPrefix returns the parent directory of the last (deepest by lex)
// object key — guaranteed to land on an existing dir boundary.
func pickDeepPrefix(objs []Object) string {
	last := objs[len(objs)-1].Key
	idx := -1
	for i := len(last) - 1; i >= 0; i-- {
		if last[i] == '/' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ""
	}
	return last[:idx+1]
}
