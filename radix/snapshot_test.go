package radix

import (
	"bytes"
	"os"
	"testing"
)

// snapshotRoundtrip saves tr into an in-memory buffer, loads it back, and
// returns the new tree. Used by tests that want to compare before/after.
func snapshotRoundtrip(t testing.TB, tr *Tree) *Tree {
	t.Helper()
	var buf bytes.Buffer
	if err := tr.Save(&buf); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return loaded
}

func TestSnapshotEmptyTree(t *testing.T) {
	tr := New()
	loaded := snapshotRoundtrip(t, tr)
	entries, err := loaded.ListDirectory("")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty listing, got %#v", entries)
	}
}

func TestSnapshotSingleObject(t *testing.T) {
	tr := New()
	if err := tr.AddBatch(objsToBatch([]Object{obj("a.txt", 7, "STANDARD")})); err != nil {
		t.Fatal(err)
	}
	loaded := snapshotRoundtrip(t, tr)
	entries, _ := loaded.ListDirectory("")
	want := []Entry{{Name: "a.txt", Class: ClassStandard, Size: 7}}
	if !entriesEqual(entries, want) {
		t.Fatalf("got %#v want %#v", entries, want)
	}
}

func TestSnapshotPreservesAggregates(t *testing.T) {
	tr := New()
	in := []Object{
		obj("afk/images/2024/05/09/x.jpg", 100, "GLACIER_IR"),
		obj("afk/images/2024/05/09/y.jpg", 200, "GLACIER_IR"),
		obj("afk/images/2024/05/10/z.jpg", 300, "STANDARD"),
	}
	if err := tr.AddBatch(objsToBatch(in)); err != nil {
		t.Fatal(err)
	}
	loaded := snapshotRoundtrip(t, tr)
	// All prefixes derivable from the dataset should match between trees.
	for _, prefix := range derivePrefixes(in) {
		got, err := loaded.ListDirectory(prefix)
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		want, _ := tr.ListDirectory(prefix)
		if !entriesEqual(got, want) {
			t.Fatalf("prefix %q mismatch\n got:  %s\n want: %s", prefix, dumpEntries(got), dumpEntries(want))
		}
	}
}

func TestSnapshotPreservesFreeListSlots(t *testing.T) {
	// Insert + delete + insert again; verify the snapshot retains reused IDs
	// correctly and listing matches.
	tr := New()
	first := []Object{
		obj("a/b.txt", 1, "STANDARD"),
		obj("a/c.txt", 2, "STANDARD"),
		obj("a/d.txt", 3, "STANDARD"),
	}
	if err := tr.AddBatch(objsToBatch(first)); err != nil {
		t.Fatal(err)
	}
	// Delete the middle one (drives freelist).
	second := Batch{
		StartFrom: "a/a",
		Objects: []Object{
			obj("a/b.txt", 1, "STANDARD"),
			obj("a/d.txt", 3, "STANDARD"),
		},
	}
	if err := tr.AddBatch(second); err != nil {
		t.Fatal(err)
	}
	loaded := snapshotRoundtrip(t, tr)
	got, _ := loaded.ListDirectory("a/")
	want, _ := tr.ListDirectory("a/")
	if !entriesEqual(got, want) {
		t.Fatalf("got %s want %s", dumpEntries(got), dumpEntries(want))
	}
}

func TestSnapshotMagicMismatch(t *testing.T) {
	// 20 bytes of nonsense so Load gets a complete header to inspect.
	junk := bytes.NewReader([]byte("not-a-real-radix-snap"))
	if _, err := Load(junk); err != ErrSnapshotMagic {
		t.Fatalf("want ErrSnapshotMagic, got %v", err)
	}
}

// TestSnapshotRealDataset round-trips a subsample of the real dataset and
// verifies that every derived prefix returns identical listings before and
// after.
func TestSnapshotRealDataset(t *testing.T) {
	const path = "../test.objects.jsonl"
	if _, err := os.Stat(path); err != nil {
		t.Skipf("skipping: %s not present", path)
	}
	full := loadJSONL(t, path)
	sample := subsampleObjects(full, 50_000, 0xC0FFEE)

	tr := New()
	const batchSize = 1000
	prev := ""
	for start := 0; start < len(sample); start += batchSize {
		end := min(start+batchSize, len(sample))
		if err := tr.AddBatch(Batch{StartFrom: prev, Objects: sample[start:end]}); err != nil {
			t.Fatalf("AddBatch: %v", err)
		}
		prev = sample[end-1].Key
	}

	loaded := snapshotRoundtrip(t, tr)

	for _, prefix := range derivePrefixes(sample) {
		got, _ := loaded.ListDirectory(prefix)
		want, _ := tr.ListDirectory(prefix)
		if !entriesEqual(got, want) {
			t.Fatalf("prefix %q mismatch", prefix)
		}
	}
}

func TestExportRoundtrip(t *testing.T) {
	const path = "../test.objects.jsonl"
	if _, err := os.Stat(path); err != nil {
		t.Skipf("skipping: %s not present", path)
	}
	full := loadJSONL(t, path)
	sample := subsampleObjects(full, 50_000, 0xC0FFEE)

	tr := New()
	const batchSize = 1000
	prev := ""
	for start := 0; start < len(sample); start += batchSize {
		end := min(start+batchSize, len(sample))
		_ = tr.AddBatch(Batch{StartFrom: prev, Objects: sample[start:end]})
		prev = sample[end-1].Key
	}

	exported := make([]Object, 0, len(sample))
	tr.Export(func(o Object) bool {
		exported = append(exported, o)
		return true
	})
	if len(exported) != len(sample) {
		t.Fatalf("exported %d, sampled %d", len(exported), len(sample))
	}
	for i := range exported {
		if exported[i] != sample[i] {
			t.Fatalf("export[%d]=%v sample[%d]=%v", i, exported[i], i, sample[i])
		}
	}
}
