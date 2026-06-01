// s3du-rm: parallel batched DeleteObjects under a single non-empty prefix.
//
// Pipeline:
//
//	seed (BFS fanout) -> prefixCh -> [L list workers] -> keyCh ->
//	[batcher] -> batchCh -> [D delete workers] -> stats
//
// Safety:
//   - -prefix is required and must be non-empty (no whole-bucket rm).
//   - Refuses to run if bucket versioning is Enabled or Suspended.
//   - Defaults to dry-run; real deletion requires -yes plus a stdin
//     confirmation of "<bucket>/<prefix>".
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// deleteBatchSize is the DeleteObjects API hard cap.
const deleteBatchSize = 1000

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
	listed   atomic.Int64
	deleted  atomic.Int64
	errored  atomic.Int64
	requests atomic.Int64
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("s3du-rm: ")

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

	fmt.Fprintf(os.Stderr,
		"bucket=%s prefix=%q seed_prefixes=%d list_workers=%d delete_workers=%d batch=%d real_delete=%v\n",
		o.bucket, o.prefix, len(seeds), o.listWorkers, o.deleteWorkers, deleteBatchSize, o.yes)
	if len(seeds) <= 10 {
		for _, s := range seeds {
			fmt.Fprintf(os.Stderr, "  seed: %s\n", s)
		}
	}

	if !o.yes {
		fmt.Fprintln(os.Stderr, "DRY-RUN: nothing will be deleted (pass -yes to delete)")
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
	case o.listWorkers < 1, o.deleteWorkers < 1:
		return errors.New("worker counts must be >= 1")
	case o.fanoutDepth < 1:
		return errors.New("-fanout-depth must be >= 1")
	case o.fanoutMin < 1:
		return errors.New("-fanout-min must be >= 1")
	}
	return nil
}

func newS3Client(ctx context.Context, region, endpoint string) (*s3.Client, error) {
	var loaders []func(*config.LoadOptions) error
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

// guardVersioning aborts unless the bucket has never had versioning enabled.
// Suspended buckets still retain old versions and produce delete-markers on
// delete, which this tool is not designed to handle.
func guardVersioning(ctx context.Context, c *s3.Client, bucket string) error {
	out, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("GetBucketVersioning: %w", err)
	}
	switch out.Status {
	case types.BucketVersioningStatusEnabled, types.BucketVersioningStatusSuspended:
		return fmt.Errorf("bucket versioning is %q; refusing to delete (this tool only deletes current versions)", out.Status)
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
	fmt.Fprintf(os.Stderr, "type exactly %q to confirm deletion: ", want)
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
	keyCh := make(chan types.ObjectIdentifier, 10_000)
	batchCh := make(chan []types.ObjectIdentifier, o.deleteWorkers*2)

	var st stats
	start := time.Now()

	// seed feeder
	go func() {
		defer close(prefixCh)
		for _, p := range seeds {
			select {
			case <-ctx.Done():
				return
			case prefixCh <- p:
			}
		}
	}()

	// list workers
	var wgList sync.WaitGroup
	for id := range o.listWorkers {
		wgList.Go(func() {
			for p := range prefixCh {
				if err := listRecursive(ctx, c, o.bucket, p, keyCh, &st); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("list worker %d prefix=%q: %v", id, p, err)
					st.errored.Add(1)
				}
			}
		})
	}

	// batcher: pack keys into ≤deleteBatchSize chunks
	var wgBatch sync.WaitGroup
	wgBatch.Go(func() {
		defer close(batchCh)
		buf := make([]types.ObjectIdentifier, 0, deleteBatchSize)
		flush := func() {
			if len(buf) == 0 {
				return
			}
			b := make([]types.ObjectIdentifier, len(buf))
			copy(b, buf)
			select {
			case <-ctx.Done():
			case batchCh <- b:
			}
			buf = buf[:0]
		}
		for k := range keyCh {
			buf = append(buf, k)
			if len(buf) >= deleteBatchSize {
				flush()
			}
		}
		flush()
	})

	// delete workers
	var wgDel sync.WaitGroup
	for id := range o.deleteWorkers {
		wgDel.Go(func() {
			for batch := range batchCh {
				st.requests.Add(1)
				if !o.yes {
					st.deleted.Add(int64(len(batch)))
					continue
				}
				out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(o.bucket),
					Delete: &types.Delete{
						Objects: batch,
						Quiet:   aws.Bool(true),
					},
				})
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("delete worker %d: %v", id, err)
					st.errored.Add(int64(len(batch)))
					continue
				}
				ok := int64(len(batch))
				for _, e := range out.Errors {
					log.Printf("delete err key=%q code=%q msg=%q",
						aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
					st.errored.Add(1)
					ok--
				}
				st.deleted.Add(ok)
			}
		})
	}

	// progress
	var wgProgress sync.WaitGroup
	stopProgress := make(chan struct{})
	if !o.quiet {
		wgProgress.Go(func() { progress(ctx, &st, start, stopProgress) })
	}

	wgList.Wait()
	close(keyCh)
	wgBatch.Wait()
	wgDel.Wait()
	close(stopProgress)
	wgProgress.Wait()

	if !o.quiet {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprintf(os.Stderr,
		"final: listed=%d deleted=%d errored=%d requests=%d dry_run=%v elapsed=%s\n",
		st.listed.Load(), st.deleted.Load(), st.errored.Load(), st.requests.Load(),
		!o.yes, time.Since(start).Round(time.Millisecond),
	)
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func listRecursive(ctx context.Context, c *s3.Client, bucket, prefix string,
	out chan<- types.ObjectIdentifier, st *stats) error {
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
			st.listed.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- types.ObjectIdentifier{Key: obj.Key}:
			}
		}
	}
	return nil
}

func progress(ctx context.Context, st *stats, start time.Time, stop <-chan struct{}) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	var prevDel int64
	prevTime := start
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case now := <-t.C:
			d := st.deleted.Load()
			rate := 0.0
			if dt := now.Sub(prevTime).Seconds(); dt > 0 {
				rate = float64(d-prevDel) / dt
			}
			prevDel, prevTime = d, now
			fmt.Fprintf(os.Stderr,
				"\rlisted=%d deleted=%d errored=%d req=%d rate=%.0f/s elapsed=%s   ",
				st.listed.Load(), d, st.errored.Load(), st.requests.Load(),
				rate, time.Since(start).Round(time.Second),
			)
		}
	}
}
