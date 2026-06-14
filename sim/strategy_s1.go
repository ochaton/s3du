package sim

import (
	"context"
	"time"
)

func init() { Register(SingleRecursive{}) }

// SingleRecursive (S1) is the simplest possible scan: one worker, one
// paginated ListObjectsV2 from prefix="" with no delimiter, pages until
// exhaustion. Establishes the floor of "1× S3 request per 1 000 keys"
// regardless of bucket shape.
type SingleRecursive struct{}

func (SingleRecursive) Name() string { return "s1-recursive" }

func (s SingleRecursive) Run(ctx context.Context, b *Bucket, _ RunOpts) (Result, error) {
	start := time.Now()
	var reqs, contents int64
	token := ""
	for {
		resp, err := b.List(ctx, ListReq{StartAfter: token, MaxKeys: MaxKeysCap})
		if err != nil {
			return Result{}, err
		}
		reqs++
		contents += int64(resp.ContentsCount)
		if !resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	return Result{
		StrategyName: s.Name(),
		Workers:      1,
		Requests:     reqs,
		ContentsCount: contents,
		Elapsed:      time.Since(start),
		PerWorker:    []int64{reqs},
	}, nil
}
