// s3du scans an S3 bucket and builds a compressed radix-tree index of every
// object's key, size, and storage class. The index is snapshotted to disk
// and can be browsed interactively via the bubbletea TUI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ochaton/s3du/radix"
)

const (
	retryMaxAttempts = 8
	retryMaxBackoff  = 30 * time.Second
)

type opts struct {
	bucket        string
	region        string
	endpoint      string
	workers       int
	parallelDepth int
	snapshotPath  string
	loadOnly      bool
	interactive   bool
	progressEvery time.Duration
}

func main() {
	var o opts
	progressMs := flag.Int("progress-ms", 250, "progress reporter interval in milliseconds")
	flag.StringVar(&o.bucket, "bucket", "", "S3 bucket to scan (required unless -load is set)")
	flag.StringVar(&o.region, "region", "", "AWS region (auto-detected when empty)")
	flag.StringVar(&o.endpoint, "endpoint", "", "non-AWS S3 endpoint URL (uses path-style addressing)")
	flag.IntVar(&o.workers, "workers", 32, "number of concurrent ListObjectsV2 workers")
	flag.IntVar(&o.parallelDepth, "parallel-depth", 3, "delimiter-walk depth at which prefixes become parallel work units")
	flag.StringVar(&o.snapshotPath, "snapshot", "", "path to write/read the binary tree snapshot (defaults to ~/.cache/s3du/<bucket>@<region>/tree.snap)")
	flag.BoolVar(&o.loadOnly, "load", false, "skip scanning, load the snapshot from -snapshot and continue (e.g., launch TUI)")
	flag.BoolVar(&o.interactive, "i", false, "launch the bubbletea TUI after the scan finishes")
	flag.Parse()
	o.progressEvery = time.Duration(*progressMs) * time.Millisecond

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "s3du:", err)
		os.Exit(1)
	}
}

func run(o opts) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Resolve snapshot path early; both paths (scan + load) need it.
	if o.snapshotPath == "" {
		if o.bucket == "" {
			return errors.New("either -bucket or -snapshot must be provided")
		}
		p, err := defaultSnapshotPath(o.bucket, o.region)
		if err != nil {
			return err
		}
		o.snapshotPath = p
	}

	tree, err := acquireTree(ctx, &o)
	if err != nil {
		return err
	}

	if o.interactive {
		return runTUI(tree, o.region)
	}
	return printSummary(tree, o.region)
}

// acquireTree returns the radix.Tree the rest of the program operates on.
// In -load mode it deserialises from disk; otherwise it scans the bucket and
// snapshots the result. o.region is updated in place when the SDK auto-
// resolved it from the environment so cost calculations match the scan.
func acquireTree(ctx context.Context, o *opts) (*radix.Tree, error) {
	if o.loadOnly {
		tree, err := loadSnapshot(o.snapshotPath)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", o.snapshotPath, err)
		}
		fmt.Fprintf(os.Stderr, "loaded snapshot %s\n", o.snapshotPath)
		return tree, nil
	}

	if o.bucket == "" {
		return nil, errors.New("-bucket is required when not using -load")
	}
	client, err := newS3Client(ctx, o.region, o.endpoint)
	if err != nil {
		return nil, fmt.Errorf("init S3 client: %w", err)
	}
	if o.region == "" {
		o.region = client.Options().Region
	}

	tree := radix.New()
	prog := NewProgress(o.region, o.workers)

	// Reporter is joined via WaitGroup so its final progress line is fully
	// flushed before the next stderr write — replaces the previous
	// time.Sleep-and-hope.
	done := make(chan struct{})
	var reporterWG sync.WaitGroup
	reporterWG.Go(func() {
		RunReporter(os.Stderr, prog, done, o.progressEvery)
	})

	scanner := NewScanner(client, o.bucket, o.workers, o.parallelDepth, tree, prog)
	start := time.Now()
	scanErr := scanner.Run(ctx)
	close(done)
	reporterWG.Wait()

	if scanErr != nil {
		return nil, scanErr
	}
	final := prog.Snapshot()
	fmt.Fprintf(os.Stderr,
		"scan completed in %s · eff parallelism: avg %.1f/%d (%d%%), final %.1f/%d (%d%%)\n",
		time.Since(start),
		final.CumulativeParallelism(), final.MaxWorkers,
		int(final.CumulativeParallelism()*100/float64(max(final.MaxWorkers, 1))),
		final.InflightEWMA, final.MaxWorkers,
		int(final.InflightEWMA*100/float64(max(final.MaxWorkers, 1))),
	)

	if err := saveSnapshot(o.snapshotPath, tree); err != nil {
		return nil, fmt.Errorf("save snapshot %s: %w", o.snapshotPath, err)
	}
	fmt.Fprintf(os.Stderr, "snapshot saved to %s\n", o.snapshotPath)
	return tree, nil
}

// printSummary prints the top-level directory listing along with totals when
// the TUI is not requested.
func printSummary(tree *radix.Tree, region string) error {
	entries, err := tree.ListDirectory("")
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Println("(empty tree)")
		return nil
	}
	fmt.Printf("%-40s %12s %15s %12s\n", "name", "objects", "bytes", "$/month")
	for _, e := range entries {
		if e.IsDir {
			cost := 0.0
			for _, kv := range e.Aggregate.Bytes {
				cost += monthlyStorageCost(kv.Size, kv.Class.String(), region)
			}
			fmt.Printf("%-40s %12d %15s %12s\n", e.Name, e.Aggregate.Objects, humanBytes(byteSum(e.Aggregate.Bytes)), humanDollars(cost))
			continue
		}
		fmt.Printf("%-40s %12s %15s %12s\n", e.Name, "(file)", humanBytes(e.Size), humanDollars(monthlyStorageCost(e.Size, e.Class.String(), region)))
	}
	return nil
}

func byteSum(b radix.ClassBytes) int64 {
	var t int64
	for _, kv := range b {
		t += kv.Size
	}
	return t
}

// defaultSnapshotPath returns ~/.cache/s3du/<bucket>@<region>/tree.snap.
func defaultSnapshotPath(bucket, region string) (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	if region == "" {
		region = "unknown"
	}
	return filepath.Join(dir, "s3du", bucket+"@"+region, "tree.snap"), nil
}

// saveSnapshot writes tree to path, creating parent directories.
func saveSnapshot(path string, tree *radix.Tree) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := tree.Save(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// loadSnapshot reads a previously saved tree from path.
func loadSnapshot(path string) (*radix.Tree, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return radix.Load(f)
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

