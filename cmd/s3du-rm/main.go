// s3du-rm: parallel batched DeleteObjects under a single non-empty prefix.
//
// Pipeline:
//
//	seed (BFS fanout) -> prefixCh -> [L list workers] -> keyCh ->
//	[batcher] -> batchCh -> [D delete workers] -> stats
//
// Safety:
//   - -prefix is required and must be non-empty (no whole-bucket rm).
//   - Refuses to run if bucket versioning is Enabled (DeleteObject would
//     only insert delete markers, not free storage). Suspended is fine.
//   - Defaults to dry-run; real deletion requires -yes plus a stdin
//     confirmation of "<bucket>/<prefix>".
//
// Throttling:
//   - SDK is configured with adaptive retry and a high MaxAttempts so that
//     S3 SlowDown (503) responses are absorbed transparently. Batches that
//     still fail after the retryer exhausts are counted as `throttled`.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	// deleteBatchSize is the DeleteObjects API hard cap.
	deleteBatchSize = 1000
	// seedPreviewLimit caps how many seed prefixes we print at startup.
	seedPreviewLimit = 10
	// keyChanBuffer sizes the key channel. Large enough to absorb a few
	// pages from each list worker so listing doesn't stall on bursty deletes.
	keyChanBuffer = 10_000
	// retryMaxAttempts bumps the SDK retryer well above its default of 3
	// because S3 throttles (SlowDown 503) need many attempts under bulk load.
	retryMaxAttempts = 20
	// retryMaxBackoff caps exponential backoff so a single throttled call
	// cannot wedge a delete worker for minutes.
	retryMaxBackoff = 30 * time.Second
)

// syncWriter serializes Write calls so progress redraws and log lines do
// not interleave byte streams on stderr. log.Logger and fmt.Fprintf both
// issue one Write per message, so a per-Write mutex is sufficient.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

var stderr = &syncWriter{w: os.Stderr}

type opts struct {
	bucket        string
	prefix        string
	listWorkers   int
	deleteWorkers int
	fanoutDepth   int
	fanoutMin     int
	yes           bool
	force         bool
	region        string
	endpoint      string
	quiet         bool
}

type stats struct {
	listed       atomic.Int64
	listedBytes  atomic.Int64
	deleted      atomic.Int64
	deletedBytes atomic.Int64
	// listErrors counts failed ListObjectsV2 calls (one per failed prefix
	// attempt). Distinct from deleteErrors so the per-object delete count
	// stays meaningful.
	listErrors   atomic.Int64
	deleteErrors atomic.Int64
	// throttled counts batches that still failed with SlowDown after the
	// SDK retryer exhausted all attempts. Signals oversubscribed worker
	// count for the bucket.
	throttled atomic.Int64
	requests  atomic.Int64
}

// objRef carries the key plus its size through the pipeline so the delete
// stage can report bytes freed.
type objRef struct {
	key  *string
	size int64
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("s3du-rm: ")
	log.SetOutput(stderr)

	var o opts
	flag.StringVar(&o.bucket, "bucket", "", "S3 bucket (required)")
	flag.StringVar(&o.prefix, "prefix", "", "non-empty key prefix to delete under (required)")
	flag.IntVar(&o.listWorkers, "list-workers", 8, "parallel ListObjectsV2 workers (L)")
	flag.IntVar(&o.deleteWorkers, "delete-workers", 4, "parallel DeleteObjects workers (D)")
	flag.IntVar(&o.fanoutDepth, "fanout-depth", 1, "max BFS depth used to fan out subprefixes")
	flag.IntVar(&o.fanoutMin, "fanout-min", 4, "stop fanning out once we have at least this many subprefixes")
	flag.BoolVar(&o.yes, "yes", false, "actually delete (omit for dry-run)")
	flag.BoolVar(&o.force, "force", false, "skip stdin confirmation (still requires -yes)")
	flag.StringVar(&o.region, "region", "", "AWS region override")
	flag.StringVar(&o.endpoint, "endpoint", "", "S3-compatible endpoint URL")
	flag.BoolVar(&o.quiet, "quiet", false, "suppress progress output")
	flag.Parse()

	if err := run(o); err != nil {
		log.Fatal(err)
	}
}

func run(o opts) error {
	if err := validate(o); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := newS3Client(ctx, o.region, o.endpoint)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	if err := guardVersioning(ctx, client, o.bucket); err != nil {
		return err
	}

	seeds, err := fanout(ctx, client, o.bucket, o.prefix, o.fanoutDepth, o.fanoutMin)
	if err != nil {
		return fmt.Errorf("fanout: %w", err)
	}
	if len(seeds) == 0 {
		return errors.New("no prefixes resolved from seed; nothing to do")
	}

	fmt.Fprintf(stderr,
		"bucket=%s prefix=%q seed_prefixes=%d list_workers=%d delete_workers=%d batch=%d dry_run=%v\n",
		o.bucket, o.prefix, len(seeds), o.listWorkers, o.deleteWorkers, deleteBatchSize, !o.yes)
	if len(seeds) <= seedPreviewLimit {
		for _, s := range seeds {
			fmt.Fprintf(stderr, "  seed: %s\n", s)
		}
	}

	if !o.yes {
		fmt.Fprintln(stderr, "DRY-RUN: nothing will be deleted (pass -yes to delete)")
	} else if !o.force {
		if err := confirmStdin(o.bucket, o.prefix); err != nil {
			return err
		}
	}

	return runPipeline(ctx, client, o, seeds)
}

func validate(o opts) error {
	switch {
	case o.bucket == "":
		return errors.New("-bucket required")
	case strings.TrimSpace(o.prefix) == "":
		return errors.New("-prefix required and must be non-empty (refusing whole-bucket delete)")
	case o.listWorkers < 1:
		return errors.New("-list-workers must be >= 1")
	case o.deleteWorkers < 1:
		return errors.New("-delete-workers must be >= 1")
	case o.fanoutDepth < 1:
		return errors.New("-fanout-depth must be >= 1")
	case o.fanoutMin < 1:
		return errors.New("-fanout-min must be >= 1")
	}
	return nil
}

func newS3Client(ctx context.Context, region, endpoint string) (*s3.Client, error) {
	loaders := []func(*config.LoadOptions) error{
		config.WithRetryer(func() aws.Retryer {
			return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
				o.StandardOptions = append(o.StandardOptions,
					func(so *retry.StandardOptions) {
						so.MaxAttempts = retryMaxAttempts
						so.MaxBackoff = retryMaxBackoff
					})
			})
		}),
	}
	if region != "" {
		loaders = append(loaders, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loaders...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(opt *s3.Options) {
		if endpoint != "" {
			opt.BaseEndpoint = aws.String(endpoint)
			opt.UsePathStyle = true
		}
	}), nil
}

// guardVersioning aborts only when versioning is Enabled. In that state
// DeleteObject inserts a delete marker instead of actually removing data,
// so storage would not be freed. Suspended buckets behave like unversioned
// ones for new deletes (replace the "null" version), which is acceptable.
func guardVersioning(ctx context.Context, c *s3.Client, bucket string) error {
	out, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("GetBucketVersioning: %w", err)
	}
	if out.Status == types.BucketVersioningStatusEnabled {
		return errors.New("bucket versioning is Enabled; refusing to delete (DeleteObject would only insert delete markers, not free storage)")
	}
	return nil
}

// fanout performs BFS expansion via Delimiter="/" until we have at least
// minSubs leaf prefixes or until maxDepth is reached. Each returned prefix
// is then listed recursively by a list worker.
func fanout(ctx context.Context, c *s3.Client, bucket, root string, maxDepth, minSubs int) ([]string, error) {
	current := []string{root}
	for range maxDepth {
		next := make([]string, 0, len(current)*4)
		grew := false
		for _, p := range current {
			subs, err := listCommonPrefixes(ctx, c, bucket, p)
			if err != nil {
				return nil, err
			}
			if len(subs) == 0 {
				next = append(next, p)
				continue
			}
			next = append(next, subs...)
			grew = true
		}
		current = next
		if !grew || len(current) >= minSubs {
			break
		}
	}
	return current, nil
}

func listCommonPrefixes(ctx context.Context, c *s3.Client, bucket, prefix string) ([]string, error) {
	var subs []string
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			if s := aws.ToString(cp.Prefix); s != "" {
				subs = append(subs, s)
			}
		}
	}
	return subs, nil
}

func confirmStdin(bucket, prefix string) error {
	want := bucket + "/" + prefix
	fmt.Fprintf(stderr, "type exactly %q to confirm deletion: ", want)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read confirmation: %w", err)
	}
	if strings.TrimRight(line, "\r\n") != want {
		return errors.New("confirmation mismatch; aborting")
	}
	return nil
}

func runPipeline(ctx context.Context, c *s3.Client, o opts, seeds []string) error {
	prefixCh := make(chan string, o.listWorkers*4)
	keyCh := make(chan objRef, keyChanBuffer)
	batchCh := make(chan []objRef, o.deleteWorkers*2)

	var st stats
	start := time.Now()

	var wgSeed sync.WaitGroup
	wgSeed.Go(func() {
		defer close(prefixCh)
		for _, p := range seeds {
			select {
			case <-ctx.Done():
				return
			case prefixCh <- p:
			}
		}
	})

	var wgList sync.WaitGroup
	for id := range o.listWorkers {
		wgList.Go(func() {
			for p := range prefixCh {
				if err := listRecursive(ctx, c, o.bucket, p, keyCh, &st); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("list worker %d prefix=%q: %v", id, p, err)
					st.listErrors.Add(1)
				}
			}
		})
	}

	var wgBatch sync.WaitGroup
	wgBatch.Go(func() {
		defer close(batchCh)
		buf := make([]objRef, 0, deleteBatchSize)
		flush := func() {
			if len(buf) == 0 {
				return
			}
			b := make([]objRef, len(buf))
			copy(b, buf)
			select {
			case <-ctx.Done():
			case batchCh <- b:
			}
			buf = buf[:0]
		}
		for r := range keyCh {
			buf = append(buf, r)
			if len(buf) >= deleteBatchSize {
				flush()
			}
		}
		flush()
	})

	var wgDel sync.WaitGroup
	for id := range o.deleteWorkers {
		wgDel.Go(func() { deleteWorker(ctx, c, o, id, batchCh, &st) })
	}

	var wgProgress sync.WaitGroup
	stopProgress := make(chan struct{})
	if !o.quiet {
		wgProgress.Go(func() { progress(ctx, &st, start, stopProgress) })
	}

	wgSeed.Wait()
	wgList.Wait()
	close(keyCh)
	wgBatch.Wait()
	wgDel.Wait()
	close(stopProgress)
	wgProgress.Wait()

	if !o.quiet {
		fmt.Fprintln(stderr)
	}
	fmt.Fprintf(stderr,
		"final: listed=%d (%s) deleted=%d (%s) list_errors=%d delete_errors=%d throttled=%d requests=%d dry_run=%v elapsed=%s\n",
		st.listed.Load(), humanBytes(st.listedBytes.Load()),
		st.deleted.Load(), humanBytes(st.deletedBytes.Load()),
		st.listErrors.Load(), st.deleteErrors.Load(), st.throttled.Load(),
		st.requests.Load(), !o.yes, time.Since(start).Round(time.Millisecond),
	)
	return nil
}

func deleteWorker(ctx context.Context, c *s3.Client, o opts, id int,
	batchCh <-chan []objRef, st *stats) {
	for batch := range batchCh {
		st.requests.Add(1)
		ids := make([]types.ObjectIdentifier, len(batch))
		batchBytes := int64(0)
		for i, r := range batch {
			ids[i] = types.ObjectIdentifier{Key: r.key}
			batchBytes += r.size
		}
		if !o.yes {
			st.deleted.Add(int64(len(batch)))
			st.deletedBytes.Add(batchBytes)
			continue
		}
		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(o.bucket),
			Delete: &types.Delete{
				Objects: ids,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isThrottle(err) {
				st.throttled.Add(1)
			}
			log.Printf("delete worker %d: %v", id, err)
			st.deleteErrors.Add(int64(len(batch)))
			continue
		}
		ok := int64(len(batch))
		okBytes := batchBytes
		if len(out.Errors) > 0 {
			sizeByKey := make(map[string]int64, len(batch))
			for _, r := range batch {
				sizeByKey[aws.ToString(r.key)] = r.size
			}
			for _, e := range out.Errors {
				log.Printf("delete err key=%q code=%q msg=%q",
					aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
				st.deleteErrors.Add(1)
				ok--
				okBytes -= sizeByKey[aws.ToString(e.Key)]
			}
		}
		st.deleted.Add(ok)
		st.deletedBytes.Add(okBytes)
	}
}

// isThrottle reports whether err is an S3 throttle/SlowDown response that
// the SDK could not absorb via its retryer. Used only for accounting.
func isThrottle(err error) bool {
	apiErr, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		return false
	}
	switch apiErr.ErrorCode() {
	case "SlowDown", "Throttling", "ThrottlingException", "RequestLimitExceeded":
		return true
	}
	return false
}

func listRecursive(ctx context.Context, c *s3.Client, bucket, prefix string,
	out chan<- objRef, st *stats) error {
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		st.requests.Add(1)
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			size := aws.ToInt64(obj.Size)
			st.listed.Add(1)
			st.listedBytes.Add(size)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- objRef{key: obj.Key, size: size}:
			}
		}
	}
	return nil
}

func progress(ctx context.Context, st *stats, start time.Time, stop <-chan struct{}) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	var prevDel, prevBytes int64
	prevTime := start
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case now := <-t.C:
			d := st.deleted.Load()
			dBytes := st.deletedBytes.Load()
			rate, byteRate := 0.0, 0.0
			if dt := now.Sub(prevTime).Seconds(); dt > 0 {
				rate = float64(d-prevDel) / dt
				byteRate = float64(dBytes-prevBytes) / dt
			}
			prevDel, prevBytes, prevTime = d, dBytes, now
			fmt.Fprintf(stderr,
				"\rlisted=%d (%s) deleted=%d (%s) err=%d/%d thr=%d req=%d rate=%.0f/s %s/s elapsed=%s   ",
				st.listed.Load(), humanBytes(st.listedBytes.Load()),
				d, humanBytes(dBytes),
				st.listErrors.Load(), st.deleteErrors.Load(), st.throttled.Load(),
				st.requests.Load(),
				rate, humanBytes(int64(byteRate)),
				time.Since(start).Round(time.Second),
			)
		}
	}
}

// humanBytes formats a byte count using IEC binary units (KiB, MiB, ...).
func humanBytes(n int64) string {
	const unit = 1024
	const suffixes = "KMGTPE"
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit && exp < len(suffixes)-1; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), suffixes[exp])
}
