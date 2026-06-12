// radix-bench loads an S3 listing snapshot (JSONL with key/size/class), builds
// the radix tree via chained AddBatch calls, and prints structural stats and
// heap usage.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/ochaton/s3du/radix"
)

func main() {
	var (
		path      = flag.String("input", "test.objects.jsonl", "JSONL file with {key,size,class}")
		batchSize = flag.Int("batch", 1000, "objects per AddBatch call")
		list      = flag.String("list", "\x00", "if set (use \"\" for root), print ListDirectory(prefix) after build")
	)
	flag.Parse()

	objs, err := loadJSONL(*path)
	if err != nil {
		log.Fatalf("load: %v", err)
	}
	log.Printf("loaded %d objects from %s", len(objs), *path)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()
	tr := buildTree(objs, *batchSize)
	elapsed := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heap := after.HeapAlloc - before.HeapAlloc
	log.Printf("built in %s (%.0f obj/sec)", elapsed, float64(len(objs))/elapsed.Seconds())
	log.Printf("heap: %d bytes (%.1f bytes/object)", heap, float64(heap)/float64(len(objs)))

	if *list != "\x00" {
		entries, err := tr.ListDirectory(*list)
		if err != nil {
			log.Fatalf("list: %v", err)
		}
		printListing(*list, entries)
	}

	stats := computeStats(tr)
	log.Printf("nodes: %d (files=%d, internal=%d)", stats.nodes, stats.files, stats.internal)
	log.Printf("edge bytes: %d total, %.1f avg, %d max", stats.edgeBytes, float64(stats.edgeBytes)/float64(stats.nodes), stats.maxEdge)
	log.Printf("depth (nodes): max=%d, p50=%d, p99=%d", stats.maxDepth, stats.p50Depth, stats.p99Depth)
	log.Printf("fanout: max=%d, avg=%.2f", stats.maxFanout, stats.avgFanout)
}

func buildTree(objs []radix.Object, batchSize int) *radix.Tree {
	tr := radix.New()
	prev := ""
	for s := 0; s < len(objs); s += batchSize {
		e := min(s+batchSize, len(objs))
		if err := tr.AddBatch(radix.Batch{StartFrom: prev, Objects: objs[s:e]}); err != nil {
			log.Fatalf("AddBatch[%d:%d]: %v", s, e, err)
		}
		prev = objs[e-1].Key
	}
	return tr
}

func loadJSONL(path string) ([]radix.Object, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var raw struct {
		Key   string `json:"key"`
		Size  int64  `json:"size"`
		Class string `json:"class"`
	}
	out := make([]radix.Object, 0, 1<<16)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, err
		}
		out = append(out, radix.Object{Key: raw.Key, Size: raw.Size, Class: radix.ParseClass(raw.Class)})
	}
	return out, sc.Err()
}

func printListing(prefix string, entries []radix.Entry) {
	fmt.Printf("ListDirectory(%q) → %d entries\n", prefix, len(entries))
	for _, e := range entries {
		if e.IsDir {
			fmt.Printf("  dir  %-40s objects=%d  %s\n", e.Name, e.Aggregate.Objects, classBreakdown(e.Aggregate.Bytes))
		} else {
			fmt.Printf("  file %-40s class=%-10s size=%d\n", e.Name, e.Class.String(), e.Size)
		}
	}
}

func classBreakdown(c radix.ClassBytes) string {
	nz := c.NonZero()
	if len(nz) == 0 {
		return ""
	}
	parts := make([]string, len(nz))
	for i, kv := range nz {
		parts[i] = fmt.Sprintf("%s=%d", kv.Class, kv.Size)
	}
	return strings.Join(parts, " ")
}

// statsExposer is a thin wrapper to introspect the tree. The radix package is
// black-box for this binary, so stats are computed via repeated ListDirectory
// walks — sufficient for visibility, though more precise instrumentation could
// be added later via package-internal accessors.
type stats struct {
	nodes, files, internal int
	edgeBytes              int
	maxEdge                int
	maxDepth               int
	p50Depth, p99Depth     int
	maxFanout              int
	avgFanout              float64
}

func computeStats(tr *radix.Tree) stats {
	// Walk via DFS using ListDirectory; this is approximate (counts logical
	// directories and files, not physical radix nodes). Better instrumentation
	// can replace this when the radix package exposes structural accessors.
	type frame struct {
		prefix string
		depth  int
	}
	stack := []frame{{"", 0}}
	depths := make([]int, 0, 1024)
	var s stats
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := tr.ListDirectory(f.prefix)
		if err != nil {
			continue
		}
		s.nodes++
		s.internal++
		if len(entries) > s.maxFanout {
			s.maxFanout = len(entries)
		}
		s.avgFanout += float64(len(entries))
		if f.depth > s.maxDepth {
			s.maxDepth = f.depth
		}
		depths = append(depths, f.depth)
		for _, e := range entries {
			if e.IsDir {
				stack = append(stack, frame{f.prefix + e.Name, f.depth + 1})
			} else {
				s.files++
				s.nodes++
			}
		}
	}
	if len(depths) > 0 {
		sort.Ints(depths)
		s.p50Depth = depths[len(depths)/2]
		s.p99Depth = depths[(len(depths)*99)/100]
		s.avgFanout /= float64(s.internal)
	}
	return s
}
