// s3du scans an S3 bucket and builds a compressed radix-tree index of every
// object's key, size, and storage class. The index is snapshotted to disk
// and can be browsed interactively via the bubbletea TUI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithylogging "github.com/aws/smithy-go/logging"

	"github.com/ochaton/s3du/internal/pricing"
	"github.com/ochaton/s3du/internal/progress"
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
	maxDepth      int
	snapshotPath  string
	logPath       string
	loadOnly      bool
	interactive   bool
	debug         bool
	stats         bool
	debugTree     bool
	progressEvery time.Duration
}

// defaultWorkers picks a worker count of NumCPU × 2. ListObjectsV2 is
// almost entirely network-bound (~150 ms round-trip), so wanting more
// inflight requests than CPUs is correct; the 2× heuristic gives us
// headroom on small instances without paying for a hand-tuned constant.
func defaultWorkers() int {
	n := runtime.NumCPU() * 2
	if n < 4 {
		return 4
	}
	return n
}

func main() {
	var o opts
	progressMs := flag.Int("progress-ms", 250, "progress reporter interval in milliseconds")
	flag.StringVar(&o.bucket, "bucket", "", "S3 bucket to scan (required unless -load is set)")
	flag.StringVar(&o.region, "region", "", "AWS region (auto-detected when empty)")
	flag.StringVar(&o.endpoint, "endpoint", "", "non-AWS S3 endpoint URL (uses path-style addressing)")
	flag.IntVar(&o.workers, "workers", defaultWorkers(), "number of concurrent ListObjectsV2 workers (defaults to NumCPU × 2)")
	flag.IntVar(&o.maxDepth, "max-depth", 3, "cap on delimiter-probe depth; beyond this the worker switches to a paginated recursive scan")
	flag.StringVar(&o.snapshotPath, "snapshot", "", "path to write/read the binary tree snapshot (defaults to ~/.cache/s3du/<bucket>@<region>/tree.snap)")
	flag.BoolVar(&o.loadOnly, "load", false, "skip scanning, load the snapshot from -snapshot and continue (e.g., launch TUI)")
	flag.BoolVar(&o.interactive, "i", false, "launch the bubbletea TUI after the scan finishes")
	flag.BoolVar(&o.debug, "debug", false, "mirror the structured log to stderr (disables the live progress dashboard); file logging happens regardless")
	flag.StringVar(&o.logPath, "log", "", "structured-log file path; file logging is disabled when empty")
	flag.BoolVar(&o.stats, "stats", false, "after acquiring the tree, print arena/memory statistics and exit (skips TUI and listing)")
	flag.BoolVar(&o.debugTree, "debug-tree", false, "launch the radix-internals TUI (raw nodes, edges, CIDs) instead of the directory browser; implies -i")
	flag.Parse()
	o.progressEvery = time.Duration(*progressMs) * time.Millisecond
	logFile, err := configureLogging(o.debug, o.logPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s3du: log setup:", err)
		os.Exit(1)
	}
	if logFile != nil {
		defer logFile.Close()
	}
	installGoroutineDumper(o.bucket, o.region)

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

	if o.stats {
		printTreeStats(os.Stderr, tree)
		return nil
	}
	if o.debugTree {
		return runRadixTUI(tree, o.region)
	}
	if o.interactive {
		return runTUI(tree, o.region)
	}
	return printRootListing(tree, o.region)
}

// acquireTree returns the radix.Tree the rest of the program operates on.
// In -load mode it deserialises from disk; otherwise it scans the bucket and
// snapshots the result. o.region is updated in place when the SDK auto-
// resolved it from the environment so cost calculations match the scan.
func acquireTree(ctx context.Context, o *opts) (*radix.Tree, error) {
	if o.loadOnly {
		tree, err := loadSnapshotWithProgress(o.snapshotPath, 250*time.Millisecond)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", o.snapshotPath, err)
		}
		reportHeap("loaded snapshot", tree.RootAggregate().Objects)
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
	// Auto-discover the bucket's actual region via HeadBucket. Saves a
	// PermanentRedirect cascade when the SDK's default region differs from
	// where the bucket lives, and ensures cost calculations use the right
	// regional price table. Skipped for custom endpoints — those are not
	// AWS S3 and HeadBucket's region-discovery semantics don't apply.
	if o.endpoint == "" {
		if discovered, err := discoverBucketRegion(ctx, client, o.bucket); err != nil {
			slog.Warn("HeadBucket region discovery failed; using configured region",
				"err", err, "region", o.region)
		} else if discovered != "" && discovered != o.region {
			slog.Info("HeadBucket discovered bucket region",
				"bucket", o.bucket, "from", o.region, "to", discovered)
			o.region = discovered
			client, err = newS3Client(ctx, o.region, o.endpoint)
			if err != nil {
				return nil, fmt.Errorf("re-init S3 client in %s: %w", o.region, err)
			}
		}
	}

	tree := radix.New()
	prog := progress.New(o.region, o.workers)

	// Progress dashboard is joined via WaitGroup so its final frame is
	// fully torn down before the next stderr write. The EWMA sampler runs
	// on a fixed cadence inside runProgressUI regardless of UI refresh.
	// With -debug, structured logs already share stderr, so we skip the
	// bubbletea dashboard and let slog have stderr to itself.
	done := make(chan struct{})
	var reporterWG sync.WaitGroup
	reporterWG.Go(func() {
		if o.debug {
			// progress.RunScanUI normally owns the EWMA sampler; bypass it
			// here so structured slog logs share stderr without a TUI.
			stop := progress.StartSampler(prog, 250*time.Millisecond)
			defer stop()
			progress.RunPlainReporter(os.Stderr, prog, done, o.progressEvery)
			return
		}
		progress.RunScanUI(prog, done, 250*time.Millisecond, o.progressEvery)
	})

	scanner := NewScanner(client, o.bucket, o.workers, o.maxDepth, tree, prog)
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

	if err := saveSnapshotWithProgress(tree, o.snapshotPath, 250*time.Millisecond); err != nil {
		return nil, fmt.Errorf("save snapshot %s: %w", o.snapshotPath, err)
	}
	fmt.Fprintf(os.Stderr, "snapshot saved to %s\n", o.snapshotPath)
	reportHeap("post-scan", int64(final.ObjectsSeen))
	return tree, nil
}


// printRootListing prints the top-level directory listing along with per-
// directory totals when the TUI is not requested. Distinct from the scan-
// completion line that acquireTree emits: that one reports timing and
// effective parallelism, this one reports what was actually scanned.
func printRootListing(tree *radix.Tree, region string) error {
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
			fmt.Printf("%-40s %12d %15s %12s\n",
				e.Name,
				e.Aggregate.Objects,
				progress.HumanBytes(e.Aggregate.Bytes.Total()),
				progress.HumanDollars(dirCost(e.Aggregate.Bytes, region)),
			)
			continue
		}
		fmt.Printf("%-40s %12s %15s %12s\n",
			e.Name,
			"(file)",
			progress.HumanBytes(e.Size),
			progress.HumanDollars(pricing.MonthlyStorage(e.Size, e.Class.String(), region)),
		)
	}
	return nil
}

// configureLogging installs slog's default logger and returns the (possibly
// nil) opened log file so main can close it on exit.
//
// File logging is opt-in via -log <path>. Without the flag and without
// -debug, slog writes to io.Discard so the dashboard's stderr stays
// uncluttered and no disk is consumed. With -debug, slog writes to stderr
// at Debug level (and TUI is disabled in that mode). When both -log and
// -debug are set, slog writes to both file and stderr.
func configureLogging(debug bool, logPath string) (*os.File, error) {
	level := slog.LevelDebug

	var writers []io.Writer
	var file *os.File

	if path := resolveLogPath(logPath); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create log dir %q: %w", filepath.Dir(path), err)
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open log %q: %w", path, err)
		}
		file = f
		writers = append(writers, f)
	}
	if debug {
		writers = append(writers, os.Stderr)
	}

	var out io.Writer
	switch len(writers) {
	case 0:
		out = io.Discard
	case 1:
		out = writers[0]
	default:
		out = io.MultiWriter(writers...)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: level})))
	if file != nil {
		slog.Info("logging to file", "path", file.Name())
	}
	return file, nil
}

// resolveLogPath returns the file the structured log should be written to,
// or "" to skip file logging. Only honours the explicit -log flag — file
// logging is off by default to avoid spamming disk on every run.
func resolveLogPath(explicit string) string {
	return explicit
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
		// Surface SDK warnings (retries, throttling decisions) through slog
		// so the operator can see what the retryer is doing under load.
		config.WithClientLogMode(aws.LogRetries),
		config.WithLogger(slogAWSLogger{}),
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

// discoverBucketRegion calls HeadBucket and returns the value the server
// reports for x-amz-bucket-region. SDK v2 propagates that header into
// BucketRegion on the response even when the bucket lives in a different
// region than the calling client (the SDK transparently follows the 301
// redirect on retry, but the header is exposed regardless).
//
// Empty string + nil error means the response did not include a region —
// typically only the case for very old S3 implementations or non-AWS S3
// services that don't honour the header. The caller falls back to the
// configured region in that case.
func discoverBucketRegion(ctx context.Context, client *s3.Client, bucket string) (string, error) {
	out, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return "", err
	}
	if out.BucketRegion == nil {
		return "", nil
	}
	return *out.BucketRegion, nil
}

// slogAWSLogger adapts the smithy-go logger interface onto slog. Warn-class
// messages (retries, throttling) hit slog.Warn so they show up even at the
// default Info level; debug-class messages go to slog.Debug.
type slogAWSLogger struct{}

func (slogAWSLogger) Logf(classification smithylogging.Classification, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	switch classification {
	case smithylogging.Warn:
		slog.Warn(msg, "src", "aws")
	case smithylogging.Debug:
		slog.Debug(msg, "src", "aws")
	default:
		slog.Info(msg, "src", "aws")
	}
}

// installGoroutineDumper wires SIGUSR1 to a handler that writes the current
// goroutine dump to a file in the user's cache dir. Useful for diagnosing a
// scan that has gone quiet without killing the process — workers stuck in
// retry backoff vs deadlocked vs all exited each look distinct.
func installGoroutineDumper(bucket, region string) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			path := goroutineDumpPath(bucket, region)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				slog.Error("goroutine dump: mkdir", "err", err, "path", path)
				continue
			}
			f, err := os.Create(path)
			if err != nil {
				slog.Error("goroutine dump: create", "err", err, "path", path)
				continue
			}
			if err := pprof.Lookup("goroutine").WriteTo(f, 2); err != nil {
				slog.Error("goroutine dump: write", "err", err, "path", path)
			}
			_ = f.Close()
			slog.Warn("goroutine dump written",
				"path", path,
				"numGoroutine", runtime.NumGoroutine())
		}
	}()
}

func goroutineDumpPath(bucket, region string) string {
	dir, _ := os.UserCacheDir()
	if region == "" {
		region = "unknown"
	}
	ts := time.Now().Format("20060102-150405")
	return filepath.Join(dir, "s3du", bucket+"@"+region, "goroutine-"+ts+".txt")
}

