package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/ochaton/s3du/radix"
)

// numStorageClasses is the size of the per-class counter array. It must be
// at least one greater than the highest StorageClass enum value defined in
// the radix package.
const numStorageClasses = 16

// Progress holds atomic counters that workers and the discoverer update as
// they observe S3 ListObjectsV2 responses. Read-side consumers (the TTY
// reporter, the TUI status bar) snapshot with Snapshot.
type Progress struct {
	region string

	listRequests atomic.Int64
	objectsSeen  atomic.Int64

	// One bucket per StorageClass enum value plus headroom. Indexed by the
	// uint8 value of radix.StorageClass.
	bytesByClass   [numStorageClasses]atomic.Int64
	objectsByClass [numStorageClasses]atomic.Int64
}

// NewProgress returns a zero-state Progress tracking storage in the given
// AWS region (used by the cost calculator).
func NewProgress(region string) *Progress {
	return &Progress{region: region}
}

// observeList records one ListObjectsV2 round-trip.
func (p *Progress) observeList() { p.listRequests.Add(1) }

// observeBatch tallies a batch of objects into the running counters.
func (p *Progress) observeBatch(objs []radix.Object) {
	p.objectsSeen.Add(int64(len(objs)))
	for i := range objs {
		// Class is uint8, so 0 <= class always; clamp only the high end
		// (defensive: a future radix release may grow the enum past 16).
		class := int(objs[i].Class)
		if class >= numStorageClasses {
			class = int(radix.ClassUnknown)
		}
		p.bytesByClass[class].Add(objs[i].Size)
		p.objectsByClass[class].Add(1)
	}
}

// ProgressSnapshot is a point-in-time copy of the counters, safe to format
// without further locking.
type ProgressSnapshot struct {
	Region         string
	ListRequests   int64
	ObjectsSeen    int64
	BytesByClass   [numStorageClasses]int64
	ObjectsByClass [numStorageClasses]int64
}

// Snapshot copies the current counters into a value the caller can read
// without further synchronization.
func (p *Progress) Snapshot() ProgressSnapshot {
	s := ProgressSnapshot{
		Region:       p.region,
		ListRequests: p.listRequests.Load(),
		ObjectsSeen:  p.objectsSeen.Load(),
	}
	for i := range s.BytesByClass {
		s.BytesByClass[i] = p.bytesByClass[i].Load()
		s.ObjectsByClass[i] = p.objectsByClass[i].Load()
	}
	return s
}

// ListCost returns the cumulative LIST cost in dollars at the snapshot
// time, using regional per-1000-request pricing.
func (s ProgressSnapshot) ListCost() float64 {
	return computeCost(s.ListRequests, s.Region)
}

// MonthlyStorageCost returns the per-month storage cost summed over all
// observed classes.
func (s ProgressSnapshot) MonthlyStorageCost() float64 {
	var total float64
	for class := range numStorageClasses {
		bytes := s.BytesByClass[class]
		if bytes == 0 {
			continue
		}
		total += monthlyStorageCost(bytes, radix.StorageClass(class).String(), s.Region)
	}
	return total
}

// TotalBytes returns the sum of all per-class bytes.
func (s ProgressSnapshot) TotalBytes() int64 {
	var t int64
	for i := range s.BytesByClass {
		t += s.BytesByClass[i]
	}
	return t
}

// RunReporter prints a one-line progress summary to w every interval until
// done is signalled (closed channel). It always prints a final summary so
// the caller sees the steady-state numbers even on a very fast scan.
func RunReporter(w io.Writer, p *Progress, done <-chan struct{}, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-done:
			printProgress(w, p.Snapshot(), true)
			return
		case <-tick.C:
			printProgress(w, p.Snapshot(), false)
		}
	}
}

func printProgress(w io.Writer, s ProgressSnapshot, final bool) {
	end := "\r"
	if final {
		end = "\n"
	}
	fmt.Fprintf(w,
		"lists=%-7d objects=%-9d bytes=%-9s list$=%-7s storage$/mo=%-7s%s",
		s.ListRequests,
		s.ObjectsSeen,
		humanBytes(s.TotalBytes()),
		humanDollars(s.ListCost()),
		humanDollars(s.MonthlyStorageCost()),
		end,
	)
}

// humanBytes formats a byte count in human-readable units (KiB/MiB/GiB/TiB).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanDollars(d float64) string {
	if d < 0.01 {
		return fmt.Sprintf("$%.4f", d)
	}
	return fmt.Sprintf("$%.2f", d)
}
