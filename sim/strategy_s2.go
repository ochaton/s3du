package sim

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func init() { Register(ProbeRootRecurseEach{}) }

// ProbeRootRecurseEach (S2) probes the root once with delimiter='/' (one
// or a few paginated calls), enqueues every returned CommonPrefix into a
// worker pool, and each worker runs a delimiter-less recursive scan over
// its assigned prefix.
//
// This is the natural first step up from S1: parallelism equal to the
// branching factor at the root. Vulnerable to long-tail prefixes (one
// worker may inherit a 30 M-object subtree while peers finish small
// ones).
type ProbeRootRecurseEach struct{}

func (ProbeRootRecurseEach) Name() string { return "s2-probe-root" }

func (s ProbeRootRecurseEach) Run(ctx context.Context, b *Bucket, opts RunOpts) (Result, error) {
	workers := max(opts.Workers, 1)
	start := time.Now()

	// Probe root, gathering Contents (root-level files) and CommonPrefixes.
	var reqs atomic.Int64
	var contents atomic.Int64
	var commonPrefixes atomic.Int64
	perWorker := make([]int64, workers)

	prefixes := make([]string, 0, 256)
	token := ""
	for {
		resp, err := b.List(ctx, ListReq{Delimiter: "/", StartAfter: token, MaxKeys: MaxKeysCap})
		if err != nil {
			return Result{}, err
		}
		reqs.Add(1)
		contents.Add(int64(resp.ContentsCount))
		commonPrefixes.Add(int64(len(resp.CommonPrefixes)))
		prefixes = append(prefixes, resp.CommonPrefixes...)
		if !resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}

	if len(prefixes) == 0 {
		return Result{
			StrategyName: s.Name(),
			Workers:      workers,
			Requests:     reqs.Load(),
			ContentsCount: contents.Load(),
			Elapsed:      time.Since(start),
			PerWorker:    perWorker,
		}, nil
	}

	// Worker pool: each worker drains assigned prefixes via paginated
	// delimiter-less ListObjectsV2.
	workQ := make(chan string, len(prefixes))
	for _, p := range prefixes {
		workQ <- p
	}
	close(workQ)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for prefix := range workQ {
				token := ""
				for {
					resp, err := b.List(ctx, ListReq{
						Prefix:     prefix,
						StartAfter: token,
						MaxKeys:    MaxKeysCap,
					})
					if err != nil {
						return
					}
					reqs.Add(1)
					perWorker[workerID]++
					contents.Add(int64(resp.ContentsCount))
					if !resp.IsTruncated {
						break
					}
					token = resp.NextContinuationToken
				}
			}
		}(w)
	}
	wg.Wait()

	return Result{
		StrategyName:   s.Name(),
		Workers:        workers,
		Requests:       reqs.Load(),
		ContentsCount: contents.Load(),
		CommonPrefixes: commonPrefixes.Load(),
		Elapsed:        time.Since(start),
		PerWorker:      perWorker,
	}, nil
}
