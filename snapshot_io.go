package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ochaton/s3du/radix"
)

// countingWriter wraps an io.Writer and atomically counts the bytes that
// have passed through. Cheap enough to leave in place even when no UI
// observer is reading the counter — the atomic Add is unavoidable on each
// underlying write call anyway.
type countingWriter struct {
	w     io.Writer
	bytes atomic.Int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.bytes.Add(int64(n))
	return n, err
}

// countingReader is the read-side mirror of countingWriter.
type countingReader struct {
	r     io.Reader
	bytes atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.bytes.Add(int64(n))
	return n, err
}

// reportIO ticks every interval and prints a one-line in-place progress
// indicator ("\r"-anchored) with the cumulative bytes through the counter
// and the rate observed between consecutive ticks. On done close it prints
// the final summary and returns.
func reportIO(w io.Writer, label string, counter *atomic.Int64, done <-chan struct{}, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	start := time.Now()
	var prev int64
	for {
		select {
		case <-done:
			cur := counter.Load()
			elapsed := time.Since(start)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(cur) / elapsed.Seconds()
			}
			fmt.Fprintf(w, "\r%s: %s in %s (avg %s)\033[K\n",
				label, humanBytes(cur), elapsed.Round(time.Millisecond), humanRate(rate))
			return
		case <-tick.C:
			cur := counter.Load()
			delta := cur - prev
			prev = cur
			rate := float64(delta) / interval.Seconds()
			fmt.Fprintf(w, "\r%s: %s  %s\033[K",
				label, humanBytes(cur), humanRate(rate))
		}
	}
}

// saveSnapshotWithProgress writes tree to path atomically (via "*.tmp" +
// rename, parent dirs created if missing) while streaming a bytes-written
// progress line to stderr. The radix package's own SaveFile does the same
// atomic dance but without the UI; this variant exists because save in
// s3du can take many seconds on multi-gigabyte snapshots and the user
// needs feedback.
func saveSnapshotWithProgress(tree *radix.Tree, path string, interval time.Duration) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = os.Remove(tmp)
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	cw := &countingWriter{w: f}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { reportIO(os.Stderr, "saving snapshot", &cw.bytes, done, interval) })

	saveErr := tree.Save(cw)
	close(done)
	wg.Wait()
	if saveErr != nil {
		_ = f.Close()
		return saveErr
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadSnapshotWithProgress reads a snapshot from path and reports the
// bytes-read rate to stderr while doing so. The reported rate is the read
// throughput from the buffered reader, not the underlying disk — when the
// file is in the page cache the visible numbers shoot up accordingly.
func loadSnapshotWithProgress(path string, interval time.Duration) (*radix.Tree, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Wrap directly around the file so the counter sees the actual disk
	// reads; radix.Load's internal bufio sits on top and amplifies bytes
	// across many small Read calls into one large file read per refill.
	cr := &countingReader{r: f}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { reportIO(os.Stderr, "loading snapshot", &cr.bytes, done, interval) })
	tree, err := radix.Load(bufio.NewReaderSize(cr, 1<<20))
	close(done)
	wg.Wait()
	return tree, err
}

// printTreeStats dumps the arena occupancy and per-component heap accounting
// for the given tree. Used by the -stats flag to drive memory-layout
// decisions; numbers cover sizes the Go allocator actually charged (small-
// object size classes), not raw byte counts.
func printTreeStats(w io.Writer, tree *radix.Tree) {
	s := tree.Stats()
	pct := func(part, whole int64) string {
		if whole == 0 {
			return " 0.0%"
		}
		return fmt.Sprintf("%5.1f%%", float64(part)*100/float64(whole))
	}
	avg := func(num, den int64) string {
		if den == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.2f", float64(num)/float64(den))
	}
	fmt.Fprintf(w, "\n=== radix.Tree stats ===\n")
	fmt.Fprintf(w, "alive nodes        : %d\n", s.AliveNodes)
	fmt.Fprintf(w, "  leaves           : %d (%s)   [file != nil, no children]\n", s.Leaves, pct(s.Leaves, s.AliveNodes))
	fmt.Fprintf(w, "  internals        : %d (%s)   [no file, children > 0]\n", s.Internals, pct(s.Internals, s.AliveNodes))
	fmt.Fprintf(w, "  internal+file    : %d (%s)   [S3 dir-marker objects]\n", s.InternalsWithFile, pct(s.InternalsWithFile, s.AliveNodes))
	fmt.Fprintf(w, "  empty            : %d (%s)\n", s.Empty, pct(s.Empty, s.AliveNodes))
	fmt.Fprintf(w, "nodes with agg     : %d (%s)\n", s.NodesWithAgg, pct(s.NodesWithAgg, s.AliveNodes))
	fmt.Fprintf(w, "*ClassByte allocs  : %d (%s of alive)\n", s.ClassBytePtrs, pct(s.ClassBytePtrs, s.AliveNodes))
	fmt.Fprintf(w, "ClassBytes entries : len=%d  cap=%d  avg-per-agg=%s\n", s.ClassBytesLen, s.ClassBytesCap, avg(s.ClassBytesLen, s.NodesWithAgg))
	fmt.Fprintf(w, "edge bytes (sum)   : %s  max=%d  avg=%s\n", humanBytes(s.EdgeBytes), s.MaxEdgeLen, avg(s.EdgeBytes, s.AliveNodes))
	fmt.Fprintf(w, "children slots(cap): %d  max=%d  avg=%s\n", s.ChildrenSlots, s.MaxChildrenCount, avg(s.ChildrenSlots, s.AliveNodes))

	fmt.Fprintf(w, "\nedge-length histogram (bytes):\n")
	edgeLabels := []string{"0", "1", "2", "3-4", "5-8", "9-16", "17-32", "33-64", "65+"}
	for i, label := range edgeLabels {
		fmt.Fprintf(w, "  %-6s : %d (%s)\n", label, s.EdgeLenHist[i], pct(s.EdgeLenHist[i], s.AliveNodes))
	}

	fmt.Fprintf(w, "\nchildren-count histogram:\n")
	childLabels := []string{"0", "1", "2", "3", "4", "5-7", "8-15", "16-31", "32+"}
	for i, label := range childLabels {
		fmt.Fprintf(w, "  %-6s : %d (%s)\n", label, s.ChildrenHist[i], pct(s.ChildrenHist[i], s.AliveNodes))
	}

	fmt.Fprintf(w, "\nestimated heap (allocator-rounded):\n")
	fmt.Fprintf(w, "  nodes (arena)    : %s\n", humanBytes(s.EstHeapNodes))
	fmt.Fprintf(w, "  edges            : %s\n", humanBytes(s.EstHeapEdges))
	fmt.Fprintf(w, "  children slices  : %s\n", humanBytes(s.EstHeapChildren))
	fmt.Fprintf(w, "  *ClassByte       : %s\n", humanBytes(s.EstHeapClassBytes))
	fmt.Fprintf(w, "  *Aggregate hdr   : %s\n", humanBytes(s.EstHeapAggHeaders))
	fmt.Fprintf(w, "  Aggregate.Bytes  : %s\n", humanBytes(s.EstHeapAggBytes))
	fmt.Fprintf(w, "  TOTAL            : %s\n", humanBytes(s.EstHeapTotal))
	if s.AliveNodes > 0 {
		fmt.Fprintf(w, "  B / alive-node   : %.1f\n", float64(s.EstHeapTotal)/float64(s.AliveNodes))
	}
	if s.Leaves+s.InternalsWithFile > 0 {
		fmt.Fprintf(w, "  B / object       : %.1f\n", float64(s.EstHeapTotal)/float64(s.Leaves+s.InternalsWithFile))
	}

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(tree)
	fmt.Fprintf(w, "\nruntime.HeapAlloc  : %s   (estimate covers %s of it)\n",
		humanBytes(int64(m.HeapAlloc)), pct(s.EstHeapTotal, int64(m.HeapAlloc)))
}

// reportHeap prints a single stderr line with the current resident heap
// after a forced GC. Called immediately after load/scan so the reported
// number reflects the steady-state tree (transient allocations the
// scanner/loader pushed onto the heap have been collected).
func reportHeap(label string, nObjects int64) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if nObjects > 0 {
		fmt.Fprintf(os.Stderr, "%s heap=%s  (%.1f B/object across %d objects)\n",
			label, humanBytes(int64(m.HeapAlloc)), float64(m.HeapAlloc)/float64(nObjects), nObjects)
		return
	}
	fmt.Fprintf(os.Stderr, "%s heap=%s\n", label, humanBytes(int64(m.HeapAlloc)))
}
