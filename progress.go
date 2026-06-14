package main

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ochaton/s3du/radix"
)

// numStorageClasses is the size of the per-class counter array. It must be
// at least one greater than the highest StorageClass enum value defined in
// the radix package.
const numStorageClasses = 16

// ewmaTau is the time constant of the in-flight EWMA. With reporter samples
// at ~250 ms it gives roughly ~tau/dt ≈ 120 samples per time constant —
// smooth but still responsive on the 10-minute-scan order.
const ewmaTau = 30 * time.Second

// Progress holds atomic counters that workers and the discoverer update as
// they observe S3 ListObjectsV2 responses. Read-side consumers (the TTY
// reporter, the TUI status bar) snapshot with Snapshot.
type Progress struct {
	region     string
	maxWorkers int
	startedAt  time.Time

	listRequests atomic.Int64
	objectsSeen  atomic.Int64

	// inflight is incremented by beginRequest and decremented by
	// endRequest; reads return the instantaneous count of in-flight
	// ListObjectsV2 round-trips, including discover's own.
	inflight atomic.Int64

	// totalRequestNanos accumulates the wall time spent inside every
	// ListObjectsV2 round-trip, summed across all workers. Divided by the
	// elapsed wall clock since startedAt, this is the lifetime-average
	// effective parallelism (Little's Law).
	totalRequestNanos atomic.Int64

	// inflightEWMAbits stores the IEEE-754 bit pattern of the
	// exponentially weighted moving average of inflight, sampled by the
	// reporter goroutine. Only the reporter writes it; everyone else
	// reads via math.Float64frombits — no torn reads thanks to the 8-byte
	// atomic.
	inflightEWMAbits atomic.Uint64

	// objectsPerSecBits / requestsPerSecBits hold EWMAs of the per-second
	// rates of objectsSeen and listRequests, sampled in lockstep with
	// inflight. Same float64-in-uint64 trick.
	objectsPerSecBits  atomic.Uint64
	requestsPerSecBits atomic.Uint64

	lastSampleAt    atomic.Int64 // unix-nano of the previous EWMA sample
	lastObjectsSeen atomic.Int64
	lastListRequests atomic.Int64

	// One bucket per StorageClass enum value plus headroom. Indexed by the
	// uint8 value of radix.StorageClass.
	bytesByClass   [numStorageClasses]atomic.Int64
	objectsByClass [numStorageClasses]atomic.Int64

	// queueDepthFn returns the current length of the scanner's work queue.
	// Set by the scanner after construction; left nil when no scanner is
	// running (e.g. -load mode), in which case Snapshot reports 0.
	// Wrapped in atomic.Pointer so reader (TUI sample goroutine) and writer
	// (scanner) coordinate without a mutex.
	queueDepthFn atomic.Pointer[func() int]
}

// SetQueueDepthFn registers a callback returning the current length of the
// scanner's work queue. The callback is invoked at every Snapshot.
func (p *Progress) SetQueueDepthFn(f func() int) {
	p.queueDepthFn.Store(&f)
}

// NewProgress returns a zero-state Progress tracking storage in the given
// AWS region (used by the cost calculator). maxWorkers is the configured
// upper bound on concurrent ListObjectsV2 workers; it is used for display
// purposes ("inflight = X/maxWorkers") and never as a runtime gate.
func NewProgress(region string, maxWorkers int) *Progress {
	p := &Progress{
		region:     region,
		maxWorkers: maxWorkers,
		startedAt:  time.Now(),
	}
	p.lastSampleAt.Store(p.startedAt.UnixNano())
	return p
}

// beginRequest marks the start of one ListObjectsV2 round-trip. The caller
// must pass the returned start time back to endRequest.
func (p *Progress) beginRequest() time.Time {
	p.inflight.Add(1)
	return time.Now()
}

// endRequest finalises one ListObjectsV2 round-trip: drops the inflight
// counter, increments listRequests, and adds the request duration to the
// total-request-time accumulator.
func (p *Progress) endRequest(start time.Time) {
	p.inflight.Add(-1)
	p.listRequests.Add(1)
	p.totalRequestNanos.Add(int64(time.Since(start)))
}

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

// sampleInflight folds three measurements into their EWMAs in one pass:
// the instantaneous inflight count, and the per-second rates of new
// objects and list requests observed since the last sample. Must be called
// from a single goroutine (the reporter); the writer side is lock-free.
//
//	alpha = 1 - exp(-dt / tau)
//	ewma += alpha * (sample - ewma)
//
// which reduces to the standard exponential smoother and is correct under
// arbitrary sample spacing.
func (p *Progress) sampleInflight() {
	now := time.Now()
	last := time.Unix(0, p.lastSampleAt.Load())
	dt := now.Sub(last)
	if dt <= 0 {
		return
	}
	p.lastSampleAt.Store(now.UnixNano())
	alpha := 1 - math.Exp(-float64(dt)/float64(ewmaTau))

	// Inflight gauge: sample is the current count.
	inflight := float64(p.inflight.Load())
	prev := math.Float64frombits(p.inflightEWMAbits.Load())
	p.inflightEWMAbits.Store(math.Float64bits(prev + alpha*(inflight-prev)))

	// Object rate: delta over the elapsed sample window converted to /s.
	objs := p.objectsSeen.Load()
	dObjs := objs - p.lastObjectsSeen.Swap(objs)
	objRate := float64(dObjs) / dt.Seconds()
	prevR := math.Float64frombits(p.objectsPerSecBits.Load())
	p.objectsPerSecBits.Store(math.Float64bits(prevR + alpha*(objRate-prevR)))

	// Request rate.
	reqs := p.listRequests.Load()
	dReqs := reqs - p.lastListRequests.Swap(reqs)
	reqRate := float64(dReqs) / dt.Seconds()
	prevQ := math.Float64frombits(p.requestsPerSecBits.Load())
	p.requestsPerSecBits.Store(math.Float64bits(prevQ + alpha*(reqRate-prevQ)))
}

// ProgressSnapshot is a point-in-time copy of the counters, safe to format
// without further locking.
type ProgressSnapshot struct {
	Region            string
	MaxWorkers        int
	Elapsed           time.Duration
	ListRequests      int64
	ObjectsSeen       int64
	Inflight          int64
	InflightEWMA      float64
	ObjectsPerSec     float64
	RequestsPerSec    float64
	ObjectsPerList    float64 // EWMA-based "keys returned per ListObjectsV2 call"
	TotalRequestNanos int64
	QueueDepth        int // current length of the scanner's work queue
	BytesByClass      [numStorageClasses]int64
	ObjectsByClass    [numStorageClasses]int64
}

// Snapshot copies the current counters into a value the caller can read
// without further synchronization.
func (p *Progress) Snapshot() ProgressSnapshot {
	s := ProgressSnapshot{
		Region:            p.region,
		MaxWorkers:        p.maxWorkers,
		Elapsed:           time.Since(p.startedAt),
		ListRequests:      p.listRequests.Load(),
		ObjectsSeen:       p.objectsSeen.Load(),
		Inflight:          p.inflight.Load(),
		InflightEWMA:      math.Float64frombits(p.inflightEWMAbits.Load()),
		ObjectsPerSec:     math.Float64frombits(p.objectsPerSecBits.Load()),
		RequestsPerSec:    math.Float64frombits(p.requestsPerSecBits.Load()),
		TotalRequestNanos: p.totalRequestNanos.Load(),
	}
	// EWMA-based ratio: stays responsive to "now" without storing a
	// separate ratio EWMA. Guard against the cold-start case where no
	// requests have completed yet.
	if s.RequestsPerSec > 0 {
		s.ObjectsPerList = s.ObjectsPerSec / s.RequestsPerSec
	}
	if fnp := p.queueDepthFn.Load(); fnp != nil {
		s.QueueDepth = (*fnp)()
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

// CumulativeParallelism is the lifetime-average effective parallelism
// derived from Little's Law: total request time divided by elapsed wall
// time. Used for the final scan summary, where the historical average is
// the honest number.
func (s ProgressSnapshot) CumulativeParallelism() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.TotalRequestNanos) / float64(s.Elapsed)
}

// percent returns the integer-rounded percentage of x against the cap.
func percent(x float64, cap int) int {
	if cap <= 0 {
		return 0
	}
	return int(math.Round(x * 100 / float64(cap)))
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

// humanRate formats a per-second rate with K/M/G suffixes. Falls back to a
// plain integer with no suffix below 1000.
func humanRate(r float64) string {
	switch {
	case r >= 1e9:
		return fmt.Sprintf("%.1fG/s", r/1e9)
	case r >= 1e6:
		return fmt.Sprintf("%.1fM/s", r/1e6)
	case r >= 1e3:
		return fmt.Sprintf("%.1fK/s", r/1e3)
	default:
		return fmt.Sprintf("%.0f/s", r)
	}
}
