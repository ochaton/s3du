package sim

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func init() { Register(CurrentProduction{}) }

// CurrentProduction (S3) is the faithful port of scanner.go's algorithm
// as of this commit: probe with delimiter='/' to depth=opts.MaxDepth,
// then switch to delimiter-less recursive listing for everything beyond.
// Workers pull from a global non-blocking channel; producers fall back
// to inline recursion when the channel is full.
type CurrentProduction struct{}

func (CurrentProduction) Name() string { return "s3-current" }

type s3WorkItem struct {
	prefix string
	depth  int
}

func (s CurrentProduction) Run(ctx context.Context, b *Bucket, opts RunOpts) (Result, error) {
	workers := max(opts.Workers, 1)
	maxDepth := opts.MaxDepth
	if maxDepth < 1 {
		maxDepth = 8
	}

	start := time.Now()

	var reqs, contents, commonPrefixes atomic.Int64
	perWorker := make([]int64, workers)
	var pending atomic.Int64

	workQ := make(chan s3WorkItem, workers)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tryEnqueue := func(item s3WorkItem) bool {
		if ctx.Err() != nil {
			return false
		}
		pending.Add(1)
		select {
		case workQ <- item:
			return true
		default:
			pending.Add(-1)
			return false
		}
	}

	var dispatch func(s3WorkItem) error
	var process func(workerID int, item s3WorkItem) error

	dispatch = func(item s3WorkItem) error {
		if tryEnqueue(item) {
			return nil
		}
		// inline-fallback — currentWorkerID isn't propagated; charge a
		// "fallback" worker slot (last index). For comparison purposes
		// what matters is total per-worker request counts, and the
		// fallback path is part of the algorithm under test.
		return process(workers-1, item)
	}

	process = func(workerID int, item s3WorkItem) error {
		if item.depth >= maxDepth {
			return s.listRecursive(ctx, b, workerID, item.prefix, &reqs, &contents, perWorker)
		}
		return s.probeFanOut(ctx, b, workerID, item, dispatch, &reqs, &contents, &commonPrefixes, perWorker)
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				case item := <-workQ:
					_ = process(id, item)
					if pending.Add(-1) == 0 {
						close(done)
						return
					}
				}
			}
		}(w)
	}

	// Seed.
	pending.Add(1)
	workQ <- s3WorkItem{prefix: "", depth: 0}

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

func (CurrentProduction) probeFanOut(
	ctx context.Context, b *Bucket, workerID int, item s3WorkItem,
	dispatch func(s3WorkItem) error,
	reqs, contents, commonPrefixes *atomic.Int64, perWorker []int64,
) error {
	token := ""
	for {
		resp, err := b.List(ctx, ListReq{
			Prefix:     item.prefix,
			Delimiter:  "/",
			StartAfter: token,
			MaxKeys:    MaxKeysCap,
		})
		if err != nil {
			return err
		}
		reqs.Add(1)
		perWorker[workerID]++
		contents.Add(int64(resp.ContentsCount))
		commonPrefixes.Add(int64(len(resp.CommonPrefixes)))
		for _, cp := range resp.CommonPrefixes {
			if err := dispatch(s3WorkItem{prefix: cp, depth: item.depth + 1}); err != nil {
				return err
			}
		}
		if !resp.IsTruncated {
			return nil
		}
		token = resp.NextContinuationToken
	}
}

func (CurrentProduction) listRecursive(
	ctx context.Context, b *Bucket, workerID int, prefix string,
	reqs, contents *atomic.Int64, perWorker []int64,
) error {
	token := ""
	for {
		resp, err := b.List(ctx, ListReq{
			Prefix:     prefix,
			StartAfter: token,
			MaxKeys:    MaxKeysCap,
		})
		if err != nil {
			return err
		}
		reqs.Add(1)
		perWorker[workerID]++
		contents.Add(int64(resp.ContentsCount))
		if !resp.IsTruncated {
			return nil
		}
		token = resp.NextContinuationToken
	}
}
