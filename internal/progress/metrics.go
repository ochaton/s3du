// Package progress aggregates the live scan/load metrics (atomic
// counters, EWMAs) and the bubbletea dashboards that visualise them.
// Importers see a single public surface — progress.Progress,
// progress.Snapshot, progress.RunScanUI, progress.RunIO — instead of
// three top-level files cluttering the main package.
package progress

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ochaton/s3du/internal/pricing"
	"github.com/ochaton/s3du/radix"
)

// NumStorageClasses is the size of the per-class counter array. It must be
// at least one greater than the highest StorageClass enum value defined in
// the radix package.
const NumStorageClasses = 16

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

	inflight atomic.Int64

	totalRequestNanos atomic.Int64

	inflightEWMAbits atomic.Uint64

	objectsPerSecBits  atomic.Uint64
	requestsPerSecBits atomic.Uint64

	lastSampleAt     atomic.Int64
	lastObjectsSeen  atomic.Int64
	lastListRequests atomic.Int64

	bytesByClass   [NumStorageClasses]atomic.Int64
	objectsByClass [NumStorageClasses]atomic.Int64

	queueDepthFn atomic.Pointer[func() int]
}

// SetQueueDepthFn registers a callback returning the current length of the
// scanner's work queue. Invoked once per Snapshot.
func (p *Progress) SetQueueDepthFn(f func() int) {
	p.queueDepthFn.Store(&f)
}

// New returns a zero-state Progress tracking storage in the given AWS
// region (used by the cost calculator). maxWorkers is the configured upper
// bound on concurrent ListObjectsV2 workers; used for display only.
func New(region string, maxWorkers int) *Progress {
	p := &Progress{
		region:     region,
		maxWorkers: maxWorkers,
		startedAt:  time.Now(),
	}
	p.lastSampleAt.Store(p.startedAt.UnixNano())
	return p
}

// BeginRequest marks the start of one ListObjectsV2 round-trip. The caller
// passes the returned start time back to EndRequest.
func (p *Progress) BeginRequest() time.Time {
	p.inflight.Add(1)
	return time.Now()
}

// EndRequest finalises one round-trip: drops the inflight counter,
// increments listRequests, and adds duration to the total accumulator.
func (p *Progress) EndRequest(start time.Time) {
	p.inflight.Add(-1)
	p.listRequests.Add(1)
	p.totalRequestNanos.Add(int64(time.Since(start)))
}

// ObserveBatch tallies a batch of objects into the running counters.
func (p *Progress) ObserveBatch(objs []radix.Object) {
	p.objectsSeen.Add(int64(len(objs)))
	for i := range objs {
		class := int(objs[i].Class)
		if class >= NumStorageClasses {
			class = int(radix.ClassUnknown)
		}
		p.bytesByClass[class].Add(objs[i].Size)
		p.objectsByClass[class].Add(1)
	}
}

// SampleInflight folds three measurements into their EWMAs in one pass:
// the instantaneous inflight count, and the per-second rates of new
// objects and list requests observed since the last sample. Must be called
// from a single goroutine (the reporter); writers are lock-free.
//
//	alpha = 1 - exp(-dt / tau)
//	ewma += alpha * (sample - ewma)
func (p *Progress) SampleInflight() {
	now := time.Now()
	last := time.Unix(0, p.lastSampleAt.Load())
	dt := now.Sub(last)
	if dt <= 0 {
		return
	}
	p.lastSampleAt.Store(now.UnixNano())
	alpha := 1 - math.Exp(-float64(dt)/float64(ewmaTau))

	inflight := float64(p.inflight.Load())
	prev := math.Float64frombits(p.inflightEWMAbits.Load())
	p.inflightEWMAbits.Store(math.Float64bits(prev + alpha*(inflight-prev)))

	objs := p.objectsSeen.Load()
	dObjs := objs - p.lastObjectsSeen.Swap(objs)
	objRate := float64(dObjs) / dt.Seconds()
	prevR := math.Float64frombits(p.objectsPerSecBits.Load())
	p.objectsPerSecBits.Store(math.Float64bits(prevR + alpha*(objRate-prevR)))

	reqs := p.listRequests.Load()
	dReqs := reqs - p.lastListRequests.Swap(reqs)
	reqRate := float64(dReqs) / dt.Seconds()
	prevQ := math.Float64frombits(p.requestsPerSecBits.Load())
	p.requestsPerSecBits.Store(math.Float64bits(prevQ + alpha*(reqRate-prevQ)))
}

// Snapshot is a point-in-time copy of the counters, safe to format
// without further locking.
type Snapshot struct {
	Region            string
	MaxWorkers        int
	Elapsed           time.Duration
	ListRequests      int64
	ObjectsSeen       int64
	Inflight          int64
	InflightEWMA      float64
	ObjectsPerSec     float64
	RequestsPerSec    float64
	ObjectsPerList    float64
	TotalRequestNanos int64
	QueueDepth        int
	BytesByClass      [NumStorageClasses]int64
	ObjectsByClass    [NumStorageClasses]int64
}

// Snapshot copies the current counters into a value the caller can read
// without further synchronization.
func (p *Progress) Snapshot() Snapshot {
	s := Snapshot{
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

// ListCost returns the cumulative LIST cost in dollars.
func (s Snapshot) ListCost() float64 {
	return pricing.ListCost(s.ListRequests, s.Region)
}

// MonthlyStorageCost returns the per-month storage cost summed over all
// observed classes.
func (s Snapshot) MonthlyStorageCost() float64 {
	var total float64
	for class, bytes := range s.BytesByClass {
		if bytes == 0 {
			continue
		}
		total += pricing.MonthlyStorage(bytes, radix.StorageClass(class).String(), s.Region)
	}
	return total
}

// TotalBytes returns the sum of all per-class bytes.
func (s Snapshot) TotalBytes() int64 {
	var t int64
	for i := range s.BytesByClass {
		t += s.BytesByClass[i]
	}
	return t
}

// CumulativeParallelism is the lifetime-average effective parallelism
// derived from Little's Law: total request time divided by elapsed wall
// time. Used for the final scan summary.
func (s Snapshot) CumulativeParallelism() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.TotalRequestNanos) / float64(s.Elapsed)
}

// Percent returns the integer-rounded percentage of x against the cap.
func Percent(x float64, cap int) int {
	if cap <= 0 {
		return 0
	}
	return int(math.Round(x * 100 / float64(cap)))
}

// HumanBytes formats a byte count in human-readable units (KiB/MiB/GiB/TiB).
func HumanBytes(n int64) string {
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

// HumanDollars formats a dollar amount, switching from 4-decimal to
// 2-decimal at the cent boundary.
func HumanDollars(d float64) string {
	if d < 0.01 {
		return fmt.Sprintf("$%.4f", d)
	}
	return fmt.Sprintf("$%.2f", d)
}

// HumanRate formats a per-second rate with K/M/G suffixes. Falls back to a
// plain integer with no suffix below 1000.
func HumanRate(r float64) string {
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
