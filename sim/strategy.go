package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Strategy is one named discovery algorithm under test. Each strategy
// receives a fresh Bucket (so its request counter starts at 0) plus a
// RunOpts knob bag, and drives whatever workers/queues/recursion it
// chooses to drain the bucket.
type Strategy interface {
	Name() string
	Run(ctx context.Context, b *Bucket, opts RunOpts) (Result, error)
}

// RunOpts holds the strategy-tunable knobs. Strategies are free to ignore
// fields that don't apply to them (e.g. S1 ignores Workers and MaxDepth).
type RunOpts struct {
	Workers  int
	MaxDepth int // S3-style probe-to-depth cap; ignored by strategies that don't probe
}

// Result is the post-run summary. Counts and timings are exact (no
// sampling); PerWorker captures the per-worker request distribution so we
// can see whether a strategy actually parallelises or just appears to.
type Result struct {
	StrategyName   string
	Workers        int
	Requests       int64
	ContentsCount  int64
	CommonPrefixes int64
	Elapsed        time.Duration
	PerWorker      []int64 // requests per worker
}

// AvgPerReq is the headline cost metric: average entries (Contents +
// CommonPrefixes) per List call. Higher is cheaper.
func (r Result) AvgPerReq() float64 {
	if r.Requests == 0 {
		return 0
	}
	return float64(r.ContentsCount+r.CommonPrefixes) / float64(r.Requests)
}

// ReqsPerSec is the radix-tree's effective listing rate when the bucket
// has zero simulated latency. With non-zero latency it reflects the
// strategy's parallelism efficiency.
func (r Result) ReqsPerSec() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Requests) / r.Elapsed.Seconds()
}

// WorkerLoadStats summarises how evenly the strategy spread work across
// its workers. minMaxRatio of 1.0 = perfectly balanced; 0 = one worker
// did everything.
func (r Result) WorkerLoadStats() (minReqs, maxReqs int64, stddev float64) {
	if len(r.PerWorker) == 0 {
		return 0, 0, 0
	}
	minReqs, maxReqs = r.PerWorker[0], r.PerWorker[0]
	var sum int64
	for _, n := range r.PerWorker {
		if n < minReqs {
			minReqs = n
		}
		if n > maxReqs {
			maxReqs = n
		}
		sum += n
	}
	mean := float64(sum) / float64(len(r.PerWorker))
	var sq float64
	for _, n := range r.PerWorker {
		d := float64(n) - mean
		sq += d * d
	}
	stddev = sq / float64(len(r.PerWorker))
	// stddev is variance for now; sqrt at format time only if a caller wants it.
	return minReqs, maxReqs, stddev
}

// String renders a compact summary line for terminal output.
func (r Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-30s w=%2d reqs=%-7d contents=%-9d cp=%-7d elapsed=%-9s obj/req=%-6.1f reqs/s=%-7.0f",
		r.StrategyName, r.Workers, r.Requests, r.ContentsCount, r.CommonPrefixes,
		r.Elapsed.Round(time.Millisecond), r.AvgPerReq(), r.ReqsPerSec())
	if len(r.PerWorker) > 1 {
		minN, maxN, _ := r.WorkerLoadStats()
		balance := 0.0
		if maxN > 0 {
			balance = float64(minN) / float64(maxN)
		}
		fmt.Fprintf(&b, " load=%.2f", balance)
	}
	return b.String()
}

// strategies registry — main can pick by name.
var strategies = map[string]Strategy{}

// Register adds a Strategy to the lookup table. Called from each
// strategy's package-level init.
func Register(s Strategy) {
	strategies[s.Name()] = s
}

// Lookup returns the named strategy or nil if absent.
func Lookup(name string) Strategy {
	return strategies[name]
}

// Names returns every registered strategy name, sorted, for help text.
func Names() []string {
	names := make([]string, 0, len(strategies))
	for n := range strategies {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
