// s3du scans an S3 bucket and builds a compressed radix-tree index of every
// object's key, size, and storage class. The index is snapshotted to disk
// and can be browsed interactively via the bubbletea TUI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

func main() {
	var (
		bucket        = flag.String("bucket", "", "S3 bucket to scan (required unless -load is set)")
		region        = flag.String("region", "", "AWS region (auto-detected when empty)")
		endpoint      = flag.String("endpoint", "", "non-AWS S3 endpoint URL (uses path-style addressing)")
		workers       = flag.Int("workers", 32, "number of concurrent ListObjectsV2 workers")
		parallelDepth = flag.Int("parallel-depth", 3, "delimiter-walk depth at which prefixes become parallel work units")
		snapshotPath  = flag.String("snapshot", "", "path to write/read the binary tree snapshot (defaults to ~/.cache/s3du/<bucket>@<region>/tree.snap)")
		loadOnly      = flag.Bool("load", false, "skip scanning, load the snapshot from -snapshot and continue (e.g., launch TUI)")
		interactive   = flag.Bool("i", false, "launch the bubbletea TUI after the scan finishes")
		progressMs    = flag.Int("progress-ms", 250, "progress reporter interval in milliseconds")
	)
	flag.Parse()

	if err := run(*bucket, *region, *endpoint, *workers, *parallelDepth, *snapshotPath, *loadOnly, *interactive, *progressMs); err != nil {
		fmt.Fprintln(os.Stderr, "s3du:", err)
		os.Exit(1)
	}
}

func run(bucket, region, endpoint string, workers, parallelDepth int, snapshotPath string, loadOnly, interactive bool, progressMs int) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Resolve snapshot path early; both paths (scan + load) need it.
	if snapshotPath == "" {
		if bucket == "" {
			return errors.New("either -bucket or -snapshot must be provided")
		}
		p, err := defaultSnapshotPath(bucket, region)
		if err != nil {
			return err
		}
		snapshotPath = p
	}

	var tree *radix.Tree

	switch {
	case loadOnly:
		t, err := loadSnapshot(snapshotPath)
		if err != nil {
			return fmt.Errorf("load %s: %w", snapshotPath, err)
		}
		tree = t
		log.Printf("loaded snapshot %s", snapshotPath)

	default:
		if bucket == "" {
			return errors.New("-bucket is required when not using -load")
		}
		client, err := newS3Client(ctx, region, endpoint)
		if err != nil {
			return fmt.Errorf("init S3 client: %w", err)
		}
		// Resolve region from client config if it was auto-detected so cost
		// calculations use the actual scan region.
		if region == "" {
			region = clientRegion(client)
		}

		tree = radix.New()
		prog := NewProgress(region)

		done := make(chan struct{})
		go RunReporter(os.Stderr, prog, done, time.Duration(progressMs)*time.Millisecond)

		scanner := NewScanner(client, bucket, workers, parallelDepth, tree, prog)
		start := time.Now()
		err = scanner.Run(ctx)
		close(done)
		// Give the reporter a moment to flush its final line cleanly.
		time.Sleep(50 * time.Millisecond)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		fmt.Fprintf(os.Stderr, "scan completed in %s\n", time.Since(start))

		if err := saveSnapshot(snapshotPath, tree); err != nil {
			return fmt.Errorf("save snapshot %s: %w", snapshotPath, err)
		}
		fmt.Fprintf(os.Stderr, "snapshot saved to %s\n", snapshotPath)
	}

	if interactive {
		return runTUI(tree, region)
	}
	return printSummary(tree, region)
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

// clientRegion returns the region the s3.Client is configured with, or "".
func clientRegion(c *s3.Client) string {
	// The SDK exposes the region only on Options; capture it via a no-op
	// Options mutator just before returning the client. Easier path: read it
	// from the SDK helper.
	return c.Options().Region
}
