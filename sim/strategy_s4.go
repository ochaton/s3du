package sim

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func init() { Register(WorkStealingProbe{}) }

// WorkStealingProbe (S4) is the proposed-but-not-yet-in-production
// algorithm: per-worker LIFO deques + random-victim work stealing,
// always-probe (no maxDepth fallback to recursive scan), pending counter
// for termination. The thesis is that:
//
//   - Always-probe maximises keys-per-list at the leaf level without
//     trapping a single worker in a multi-minute paginated recursive scan.
//   - Per-worker deques expose every sub-prefix to stealing, so the tail
//     of the scan has no idle workers.
//
// Implementation knobs: random victim selection (uniform IntN), gosched
// then 100µs backoff when local empty and no victim has work.
type WorkStealingProbe struct{}

func (WorkStealingProbe) Name() string { return "s4-worksteal" }

type s4WorkItem struct {
	prefix string
	depth  int
}

type s4Queue struct {
	mu    sync.Mutex
	items []s4WorkItem
	head  int // index of oldest live item; advanced by stealTop, compacted periodically
}

func (q *s4Queue) pushBottom(item s4WorkItem) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
}

func (q *s4Queue) popBottom() (s4WorkItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.items)
	if n <= q.head {
		return s4WorkItem{}, false
	}
	it := q.items[n-1]
	q.items[n-1] = s4WorkItem{}
	q.items = q.items[:n-1]
	return it, true
}

func (q *s4Queue) stealTop() (s4WorkItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.items) {
		return s4WorkItem{}, false
	}
	it := q.items[q.head]
	q.items[q.head] = s4WorkItem{}
	q.head++
	// Periodic compaction: when the head has advanced past half the slice
	// AND we've held onto enough free slots to be worth a copy, shift the
	// live items down. Cheap relative to the work being scheduled.
	if q.head > 1024 && q.head*2 > len(q.items) {
		n := copy(q.items, q.items[q.head:])
		q.items = q.items[:n]
		q.head = 0
	}
	return it, true
}

func (s WorkStealingProbe) Run(ctx context.Context, b *Bucket, opts RunOpts) (Result, error) {
	workers := max(opts.Workers, 1)
	start := time.Now()

	queues := make([]*s4Queue, workers)
	for i := range queues {
		queues[i] = &s4Queue{}
	}

	var reqs, contents, commonPrefixes atomic.Int64
	perWorker := make([]int64, workers)
	var pending atomic.Int64

	// Seed: push to worker 0's queue.
	pending.Add(1)
	queues[0].pushBottom(s4WorkItem{prefix: "", depth: 0})

	process := func(workerID int, item s4WorkItem) {
		token := ""
		for {
			resp, err := b.List(ctx, ListReq{
				Prefix:     item.prefix,
				Delimiter:  "/",
				StartAfter: token,
				MaxKeys:    MaxKeysCap,
			})
			if err != nil {
				return
			}
			reqs.Add(1)
			perWorker[workerID]++
			contents.Add(int64(resp.ContentsCount))
			commonPrefixes.Add(int64(len(resp.CommonPrefixes)))
			for _, cp := range resp.CommonPrefixes {
				pending.Add(1)
				queues[workerID].pushBottom(s4WorkItem{prefix: cp, depth: item.depth + 1})
			}
			if !resp.IsTruncated {
				return
			}
			token = resp.NextContinuationToken
		}
	}

	dequeue := func(myID int) (s4WorkItem, bool) {
		if it, ok := queues[myID].popBottom(); ok {
			return it, true
		}
		if workers == 1 {
			return s4WorkItem{}, false
		}
		// Steal from one random victim per attempt; failed steals fall
		// through to the worker loop's backoff which will retry.
		victim := rand.IntN(workers)
		if victim == myID {
			victim = (victim + 1) % workers
		}
		return queues[victim].stealTop()
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			spins := 0
			for {
				if ctx.Err() != nil {
					return
				}
				item, ok := dequeue(id)
				if ok {
					spins = 0
					process(id, item)
					pending.Add(-1)
					continue
				}
				if pending.Load() == 0 {
					return
				}
				spins++
				switch {
				case spins < 8:
					runtime.Gosched()
				case spins < 64:
					time.Sleep(100 * time.Microsecond)
				default:
					time.Sleep(1 * time.Millisecond)
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
