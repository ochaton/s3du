package radix

import (
	"bufio"
	"encoding/json"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"testing"
)

// objsToBatch packages a sorted slice of objects into a Batch starting from "".
func objsToBatch(objs []Object) Batch {
	return Batch{StartFrom: "", Objects: objs}
}

func obj(key string, size int64, class string) Object {
	return Object{Key: key, Size: size, Class: class}
}

func sortObjects(objs []Object) {
	sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })
}

func TestListDirectoryEmptyTree(t *testing.T) {
	tr := New()
	entries, err := tr.ListDirectory("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty listing, got %#v", entries)
	}
}

func TestInvalidPrefix(t *testing.T) {
	tr := New()
	if _, err := tr.ListDirectory("foo"); err != ErrInvalidPrefix {
		t.Fatalf("want ErrInvalidPrefix, got %v", err)
	}
}

func TestSingleObject(t *testing.T) {
	tr := New()
	if err := tr.AddBatch(objsToBatch([]Object{obj("a.txt", 7, "STANDARD")})); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	entries, err := tr.ListDirectory("")
	if err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}
	want := []Entry{{Name: "a.txt", IsDir: false, Class: "STANDARD", Size: 7}}
	if !entriesEqual(entries, want) {
		t.Fatalf("got %#v want %#v", entries, want)
	}
}

func TestSplitOnInsert(t *testing.T) {
	tr := New()
	in := []Object{
		obj("afk/abc.jpg", 10, "STANDARD"),
		obj("afk/abcdef/x.jpg", 20, "GLACIER_IR"),
	}
	if err := tr.AddBatch(objsToBatch(in)); err != nil {
		t.Fatal(err)
	}
	entries, err := tr.ListDirectory("afk/")
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry{
		{Name: "abc.jpg", IsDir: false, Class: "STANDARD", Size: 10},
		{Name: "abcdef/", IsDir: true, Aggregate: Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 20}}},
	}
	if !entriesEqual(entries, want) {
		t.Fatalf("got %#v want %#v", entries, want)
	}
}

func TestDeepCompressedChain(t *testing.T) {
	tr := New()
	in := []Object{
		obj("afk/images/2024/05/09/x.jpg", 100, "GLACIER_IR"),
	}
	if err := tr.AddBatch(objsToBatch(in)); err != nil {
		t.Fatal(err)
	}
	// Each '/' boundary inside the compressed edge must remain navigable.
	cases := []struct {
		prefix string
		want   []Entry
	}{
		{"", []Entry{{Name: "afk/", IsDir: true, Aggregate: Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 100}}}}},
		{"afk/", []Entry{{Name: "images/", IsDir: true, Aggregate: Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 100}}}}},
		{"afk/images/", []Entry{{Name: "2024/", IsDir: true, Aggregate: Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 100}}}}},
		{"afk/images/2024/05/", []Entry{{Name: "09/", IsDir: true, Aggregate: Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 100}}}}},
		{"afk/images/2024/05/09/", []Entry{{Name: "x.jpg", IsDir: false, Class: "GLACIER_IR", Size: 100}}},
	}
	for _, c := range cases {
		got, err := tr.ListDirectory(c.prefix)
		if err != nil {
			t.Fatalf("%q: %v", c.prefix, err)
		}
		if !entriesEqual(got, c.want) {
			t.Fatalf("prefix %q: got %#v want %#v", c.prefix, got, c.want)
		}
	}
}

func TestDirectoryMarker(t *testing.T) {
	tr := New()
	in := []Object{
		obj("afk/", 0, "STANDARD"),
		obj("afk/file.jpg", 5, "GLACIER_IR"),
	}
	if err := tr.AddBatch(objsToBatch(in)); err != nil {
		t.Fatal(err)
	}
	rootEntries, _ := tr.ListDirectory("")
	wantRoot := []Entry{{Name: "afk/", IsDir: true, Aggregate: Aggregate{Objects: 2, Bytes: ClassBytes{GlacierIR: 5}}}}
	if !entriesEqual(rootEntries, wantRoot) {
		t.Fatalf("root: got %#v want %#v", rootEntries, wantRoot)
	}
	afkEntries, _ := tr.ListDirectory("afk/")
	wantAfk := []Entry{
		{Name: "", IsDir: false, Class: "STANDARD", Size: 0},
		{Name: "file.jpg", IsDir: false, Class: "GLACIER_IR", Size: 5},
	}
	if !entriesEqual(afkEntries, wantAfk) {
		t.Fatalf("afk: got %#v want %#v", afkEntries, wantAfk)
	}
}

func TestIdempotentBatch(t *testing.T) {
	tr := New()
	in := []Object{
		obj("a/b.txt", 1, "STANDARD"),
		obj("a/c.txt", 2, "STANDARD"),
		obj("a/d.txt", 3, "STANDARD"),
	}
	b := objsToBatch(in)
	if err := tr.AddBatch(b); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddBatch(b); err != nil {
		t.Fatal(err)
	}
	got, _ := tr.ListDirectory("a/")
	want := []Entry{
		{Name: "b.txt", IsDir: false, Class: "STANDARD", Size: 1},
		{Name: "c.txt", IsDir: false, Class: "STANDARD", Size: 2},
		{Name: "d.txt", IsDir: false, Class: "STANDARD", Size: 3},
	}
	if !entriesEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	root, _ := tr.ListDirectory("")
	if root[0].Aggregate.Objects != 3 {
		t.Fatalf("aggregate object count wrong: %d", root[0].Aggregate.Objects)
	}
}

func TestRangeDeletes(t *testing.T) {
	tr := New()
	// Initial state.
	first := []Object{
		obj("a/b.txt", 1, "STANDARD"),
		obj("a/c.txt", 2, "STANDARD"),
		obj("a/d.txt", 3, "STANDARD"),
	}
	if err := tr.AddBatch(objsToBatch(first)); err != nil {
		t.Fatal(err)
	}
	// Re-list with c removed: batch covers (a/a, a/d] and contains only b and d.
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
	got, _ := tr.ListDirectory("a/")
	want := []Entry{
		{Name: "b.txt", IsDir: false, Class: "STANDARD", Size: 1},
		{Name: "d.txt", IsDir: false, Class: "STANDARD", Size: 3},
	}
	if !entriesEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestRangeUpdates(t *testing.T) {
	tr := New()
	if err := tr.AddBatch(objsToBatch([]Object{obj("a/x", 10, "STANDARD")})); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddBatch(Batch{StartFrom: "a", Objects: []Object{obj("a/x", 25, "GLACIER_IR")}}); err != nil {
		t.Fatal(err)
	}
	got, _ := tr.ListDirectory("a/")
	want := []Entry{{Name: "x", IsDir: false, Class: "GLACIER_IR", Size: 25}}
	if !entriesEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	root, _ := tr.ListDirectory("")
	wantAgg := Aggregate{Objects: 1, Bytes: ClassBytes{GlacierIR: 25}}
	if !aggEqual(root[0].Aggregate, wantAgg) {
		t.Fatalf("agg: got %#v want %#v", root[0].Aggregate, wantAgg)
	}
}

func TestBatchValidation(t *testing.T) {
	tr := New()
	cases := []struct {
		name string
		b    Batch
		want error
	}{
		{"unsorted", Batch{Objects: []Object{obj("b", 0, "S"), obj("a", 0, "S")}}, ErrUnsortedBatch},
		{"duplicate", Batch{Objects: []Object{obj("a", 0, "S"), obj("a", 0, "S")}}, ErrUnsortedBatch},
		{"below-startfrom", Batch{StartFrom: "z", Objects: []Object{obj("a", 0, "S")}}, ErrBatchRange},
		{"equal-startfrom", Batch{StartFrom: "a", Objects: []Object{obj("a", 0, "S")}}, ErrBatchRange},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tr.AddBatch(c.b); got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

// loadJSONL loads an S3 listing snapshot in JSONL form into a sorted slice.
func loadJSONL(t testing.TB, path string) []Object {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var raw struct {
		Key   string `json:"key"`
		Size  int64  `json:"size"`
		Class string `json:"class"`
	}
	var out []Object
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("parse: %v", err)
		}
		out = append(out, Object{Key: raw.Key, Size: raw.Size, Class: raw.Class})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	sortObjects(out) // dataset is already sorted but be defensive
	return out
}

// oracleListDirectory computes the expected ListDirectory result via a linear
// pass over the source dataset. It is intentionally simple and slow to serve
// as a trusted reference.
func oracleListDirectory(all []Object, prefix string) []Entry {
	type aggBuilder struct {
		objects int64
		bytes   ClassBytes
	}
	dirAgg := make(map[string]*aggBuilder)
	files := make(map[string]Object)
	for _, o := range all {
		if !strings.HasPrefix(o.Key, prefix) {
			continue
		}
		rest := o.Key[len(prefix):]
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			dir := rest[:i+1]
			b, ok := dirAgg[dir]
			if !ok {
				b = &aggBuilder{}
				dirAgg[dir] = b
			}
			b.objects++
			b.bytes.Add(o.Class, o.Size)
		} else {
			// rest may be "" if a directory marker key equals prefix exactly.
			files[rest] = o
		}
	}
	out := make([]Entry, 0, len(dirAgg)+len(files))
	for name, o := range files {
		out = append(out, Entry{Name: name, IsDir: false, Class: o.Class, Size: o.Size})
	}
	for name, b := range dirAgg {
		out = append(out, Entry{Name: name, IsDir: true, Aggregate: Aggregate{Objects: b.objects, Bytes: b.bytes}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// TestOracleAgainstRealData ingests up to oracleMaxSample objects randomly
// sampled from the real-bucket dataset, then compares ListDirectory against
// a linear-scan oracle over the same sample for every derived '/' prefix.
// Capping the sample keeps the oracle scan tractable (O(prefixes × sample))
// regardless of how large the source snapshot grows.
func TestOracleAgainstRealData(t *testing.T) {
	const (
		path             = "../test.objects.jsonl"
		oracleMaxSample  = 50_000
		oracleSampleSeed = 0xC0FFEE
	)
	if _, err := os.Stat(path); err != nil {
		t.Skipf("skipping: %s not present", path)
	}
	full := loadJSONL(t, path)
	sample := subsampleObjects(full, oracleMaxSample, oracleSampleSeed)
	t.Logf("oracle sample: %d / %d objects", len(sample), len(full))

	tr := New()
	const batchSize = 1000
	prev := ""
	for start := 0; start < len(sample); start += batchSize {
		end := min(start+batchSize, len(sample))
		batch := Batch{StartFrom: prev, Objects: sample[start:end]}
		if err := tr.AddBatch(batch); err != nil {
			t.Fatalf("AddBatch[%d:%d]: %v", start, end, err)
		}
		prev = sample[end-1].Key
	}
	probes := derivePrefixes(sample)
	for _, p := range probes {
		got, err := tr.ListDirectory(p)
		if err != nil {
			t.Fatalf("list %q: %v", p, err)
		}
		want := oracleListDirectory(sample, p)
		if !entriesEqual(got, want) {
			t.Fatalf("prefix %q mismatch\n got:  %s\n want: %s", p, dumpEntries(got), dumpEntries(want))
		}
	}
}

// derivePrefixes returns every distinct '/' prefix observed in the dataset,
// plus the root prefix. Intended to drive the oracle test against a bounded
// (subsampled) dataset; do not call on the full unbounded dataset.
func derivePrefixes(all []Object) []string {
	set := map[string]struct{}{"": {}}
	for _, o := range all {
		k := o.Key
		for i := 0; i < len(k); i++ {
			if k[i] == '/' {
				set[k[:i+1]] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// subsampleObjects returns a deterministic random subsample of size n drawn
// from src and sorted ascending by Key. If len(src) <= n the input is
// returned unchanged.
func subsampleObjects(src []Object, n int, seed uint64) []Object {
	if len(src) <= n {
		return src
	}
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	buf := make([]Object, len(src))
	copy(buf, src)
	r.Shuffle(len(buf), func(i, j int) { buf[i], buf[j] = buf[j], buf[i] })
	out := buf[:n:n]
	sortObjects(out)
	return out
}

func entriesEqual(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].IsDir != b[i].IsDir {
			return false
		}
		if !a[i].IsDir {
			if a[i].Class != b[i].Class || a[i].Size != b[i].Size {
				return false
			}
			continue
		}
		if !aggEqual(a[i].Aggregate, b[i].Aggregate) {
			return false
		}
	}
	return true
}

func aggEqual(a, b Aggregate) bool {
	return a == b
}

func dumpEntries(e []Entry) string {
	var b strings.Builder
	for i := range e {
		if i > 0 {
			b.WriteByte(' ')
		}
		if e[i].IsDir {
			b.WriteString("[")
			b.WriteString(e[i].Name)
			b.WriteString(":")
			b.WriteString(itoa(e[i].Aggregate.Objects))
			b.WriteString(":")
			nz := e[i].Aggregate.Bytes.NonZero()
			cs := make([]string, 0, len(nz))
			for _, kv := range nz {
				cs = append(cs, kv.Class+"="+itoa(kv.Bytes))
			}
			sort.Strings(cs)
			b.WriteString(strings.Join(cs, ","))
			b.WriteString("]")
		} else {
			b.WriteString("(")
			b.WriteString(e[i].Name)
			b.WriteString(":")
			b.WriteString(e[i].Class)
			b.WriteString(":")
			b.WriteString(itoa(e[i].Size))
			b.WriteString(")")
		}
	}
	return b.String()
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

