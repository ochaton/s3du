package sim

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ochaton/s3du/radix"
)

// buildTree constructs a radix.Tree from the keys in any order. Keys are
// deduplicated and sorted before insertion so callers don't have to think
// about lex order. All objects share ClassStandard for brevity.
func buildTree(t *testing.T, keys ...string) *radix.Tree {
	t.Helper()
	slices.Sort(keys)
	keys = slices.Compact(keys)
	objs := make([]radix.Object, 0, len(keys))
	for _, k := range keys {
		objs = append(objs, radix.Object{
			Key:   k,
			Size:  int64(len(k)),
			Class: radix.ClassStandard,
		})
	}
	tr := radix.New()
	if err := tr.AddBatch(radix.Batch{Objects: objs}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	return tr
}

func keysOf(objs []radix.Object) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Key)
	}
	return out
}

func TestListNoDelimiterReturnsEverythingUnderPrefix(t *testing.T) {
	tr := buildTree(t,
		"a/1", "a/2", "a/3",
		"b/1", "b/2",
		"c/1",
	)
	b := New(tr, 0)

	resp, err := b.List(context.Background(), ListReq{Prefix: "a/", MaxKeys: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	got := keysOf(resp.Contents)
	want := []string{"a/1", "a/2", "a/3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Contents = %v, want %v", got, want)
	}
	if resp.IsTruncated {
		t.Fatalf("unexpected truncation")
	}
	if len(resp.CommonPrefixes) != 0 {
		t.Fatalf("CommonPrefixes = %v, want empty (no delimiter)", resp.CommonPrefixes)
	}
}

func TestListWithDelimiterGroupsCommonPrefixes(t *testing.T) {
	tr := buildTree(t,
		"a/x/1", "a/x/2",
		"a/y/1",
		"b/1",
		"top.txt",
	)
	b := New(tr, 0)

	resp, err := b.List(context.Background(), ListReq{Delimiter: "/", MaxKeys: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got, want := keysOf(resp.Contents), []string{"top.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Contents = %v, want %v", got, want)
	}
	if got, want := resp.CommonPrefixes, []string{"a/", "b/"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CommonPrefixes = %v, want %v", got, want)
	}
}

func TestListWithDelimiterUnderPrefix(t *testing.T) {
	tr := buildTree(t,
		"docs/2024/01/a", "docs/2024/01/b",
		"docs/2024/02/a",
		"docs/2025/01/a",
		"docs/index.html",
	)
	b := New(tr, 0)

	resp, err := b.List(context.Background(), ListReq{Prefix: "docs/", Delimiter: "/", MaxKeys: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got, want := keysOf(resp.Contents), []string{"docs/index.html"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Contents = %v, want %v", got, want)
	}
	if got, want := resp.CommonPrefixes, []string{"docs/2024/", "docs/2025/"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CommonPrefixes = %v, want %v", got, want)
	}
}

func TestPaginationMatchesUnpagedResult(t *testing.T) {
	tr := buildTree(t,
		"a/1", "a/2", "a/3", "a/4", "a/5", "a/6", "a/7", "a/8", "a/9", "a/10",
	)
	b := New(tr, 0)

	// Unpaged.
	full, _ := b.List(context.Background(), ListReq{Prefix: "a/", MaxKeys: 100})
	wantKeys := keysOf(full.Contents)

	// Three-at-a-time pages.
	got := make([]string, 0)
	startAfter := ""
	for {
		resp, _ := b.List(context.Background(), ListReq{Prefix: "a/", MaxKeys: 3, StartAfter: startAfter})
		got = append(got, keysOf(resp.Contents)...)
		if !resp.IsTruncated {
			break
		}
		startAfter = resp.NextContinuationToken
	}

	// The keys "1, 10, 2, 3, ..." sort lex; both paths should produce the
	// same ordered slice.
	if !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("paged = %v, want %v", got, wantKeys)
	}
}

func TestDelimiterPaginationMaxKeys(t *testing.T) {
	tr := buildTree(t,
		"a/1", "a/2",
		"b/1",
		"c/1",
		"d/1",
		"e/1",
		"f/1",
	)
	b := New(tr, 0)

	// Page 1: at most 3 entries.
	p1, _ := b.List(context.Background(), ListReq{Delimiter: "/", MaxKeys: 3})
	if len(p1.CommonPrefixes) != 3 {
		t.Fatalf("page 1 CommonPrefixes len = %d, want 3", len(p1.CommonPrefixes))
	}
	if !p1.IsTruncated {
		t.Fatalf("page 1 expected truncated")
	}

	// Page 2.
	p2, _ := b.List(context.Background(), ListReq{Delimiter: "/", MaxKeys: 100, StartAfter: p1.NextContinuationToken})
	want := []string{"d/", "e/", "f/"}
	if !reflect.DeepEqual(p2.CommonPrefixes, want) {
		t.Fatalf("page 2 CommonPrefixes = %v, want %v", p2.CommonPrefixes, want)
	}
}

func TestRequestsCounter(t *testing.T) {
	tr := buildTree(t, "a", "b")
	b := New(tr, 0)
	for range 5 {
		_, _ = b.List(context.Background(), ListReq{MaxKeys: 10})
	}
	if got, want := b.Requests(), int64(5); got != want {
		t.Fatalf("Requests = %d, want %d", got, want)
	}
}

// TestExhaustiveListMatchesExport sanity-checks that a paged delimiter-less
// listing returns every key in the tree, in the same order as Export, with
// no duplicates.
func TestExhaustiveListMatchesExport(t *testing.T) {
	var keys []string
	for i := range 100 {
		keys = append(keys, "obj-"+strings.Repeat("x", i%10)+"-"+itoa(i))
	}
	tr := buildTree(t, keys...)

	var expected []string
	tr.Export(func(o radix.Object) bool {
		expected = append(expected, o.Key)
		return true
	})

	b := New(tr, 0)
	got := make([]string, 0, len(expected))
	startAfter := ""
	for {
		resp, _ := b.List(context.Background(), ListReq{MaxKeys: 7, StartAfter: startAfter})
		got = append(got, keysOf(resp.Contents)...)
		if !resp.IsTruncated {
			break
		}
		startAfter = resp.NextContinuationToken
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("paged listing != Export\ngot  = %v\nwant = %v", got, expected)
	}
}

// itoa is a tiny stdlib-free helper to keep buildTree calls compact in tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
