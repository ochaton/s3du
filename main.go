package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var errWriter = os.Stderr

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name (required)")
	workers := flag.Int("workers", 32, "parallel worker count")
	region := flag.String("region", "", "AWS region (default: from env/profile)")
	interactive := flag.Bool("i", false, "launch interactive TUI after scan")
	refresh := flag.Bool("refresh", false, "ignore cache and re-scan (use with -i)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: s3du -bucket <name> [-workers N] [-region r] [-i] [--refresh]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *bucket == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	cfgOpts := []func(*config.LoadOptions) error{}
	if *region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(*region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "AWS config error: %v\n", err)
		os.Exit(1)
	}

	client := s3.NewFromConfig(cfg)
	bucketRegion, err := resolveBucketRegion(ctx, client, *bucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot determine bucket region: %v\n", err)
		os.Exit(1)
	}
	if bucketRegion != cfg.Region {
		cfg, err = config.LoadDefaultConfig(ctx, append(cfgOpts, config.WithRegion(bucketRegion))...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "AWS config error: %v\n", err)
			os.Exit(1)
		}
		client = s3.NewFromConfig(cfg)
	}

	// Try loading from cache when in interactive mode and --refresh not set.
	if *interactive && !*refresh {
		if cf, err := LoadStats(*bucket, bucketRegion); err == nil {
			treeIndex, treeErr := OpenTreeIndex(*bucket, bucketRegion)
			objIndex, objFile, objErr := OpenObjectsIndex(*bucket, bucketRegion)
			if treeErr != nil || objErr != nil {
				fmt.Fprintf(os.Stderr, "warning: cache incomplete (%v / %v), re-scanning\n", treeErr, objErr)
			} else {
				defer objFile.Close()
				fmt.Fprintf(os.Stderr, "Loaded from cache (scanned %s, %d LIST requests)\n",
					cf.ScannedAt.Format("2006-01-02 15:04:05"), cf.ListRequests)
				if err := runTUI(*bucket, bucketRegion, cf.ListRequests, treeIndex, objIndex, objFile); err != nil {
					fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
					os.Exit(1)
				}
				return
			}
		}
	}

	// Run full scan.
	prog := &Progress{}
	filesChan := make(chan taggedFile, 2048)

	doneProg := make(chan struct{})
	go runProgress(prog, bucketRegion, doneProg)

	cacheD, err := cacheDir(*bucket, bucketRegion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cache dir error: %v\n", err)
		os.Exit(1)
	}
	walPath := cacheD + "/objects.wal"
	objPath := objectsPath(cacheD)
	treeP := treePath(cacheD)

	workChan := discover(ctx, client, *bucket, filesChan, prog)

	walDone := make(chan error, 1)
	go func() { walDone <- WriteWAL(walPath, filesChan) }()

	runWorkers(ctx, client, *bucket, workChan, filesChan, *workers, prog)
	// filesChan closed by runWorkers

	close(doneProg)

	if err := <-walDone; err != nil {
		fmt.Fprintf(os.Stderr, "warning: WAL write error: %v\n", err)
	}

	if err := BuildObjectsBin(walPath, objPath); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not build objects index: %v\n", err)
	}
	os.Remove(walPath)

	if err := BuildTreeBin(objPath, treeP); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not build tree index: %v\n", err)
	}

	listReqs := prog.listRequests.Load()

	if _, saveErr := SaveCache(*bucket, bucketRegion, listReqs); saveErr != nil {
		fmt.Fprintf(os.Stderr, "warning: could not save cache: %v\n", saveErr)
	}

	if *interactive {
		treeIndex, treeErr := OpenTreeIndex(*bucket, bucketRegion)
		objIndex, objFile, objErr := OpenObjectsIndex(*bucket, bucketRegion)
		if treeErr != nil || objErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not open indexes: %v / %v\n", treeErr, objErr)
		} else {
			defer objFile.Close()
		}
		if err := runTUI(*bucket, bucketRegion, listReqs, treeIndex, objIndex, objFile); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	treeIndex, err := OpenTreeIndex(*bucket, bucketRegion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: could not open tree index: %v\n", err)
		os.Exit(1)
	}
	printResults(treeIndex, listReqs, bucketRegion)
}

func printResults(treeIndex map[string]DirSection, listRequests int64, region string) {
	root, ok := treeIndex[""]
	if !ok {
		fmt.Println("(no data)")
		return
	}

	// Per-storage-class summary from root.
	scs := make([]SCSize, len(root.SCSizes))
	copy(scs, root.SCSizes)
	sort.Slice(scs, func(i, j int) bool { return scs[i].SC < scs[j].SC })
	for _, s := range scs {
		cost := monthlyStorageCost(s.Size, s.SC, region)
		fmt.Printf("[%s]   %d files   %s   $%.4f/month\n", s.SC, s.Count, humanSize(s.Size), cost)
	}
	fmt.Printf("GRAND TOTAL  %d files   %s\n\n", root.Count, humanSize(root.Size))

	// Direct children of root.
	type child struct {
		prefix string
		sec    DirSection
	}
	var children []child
	for k, sec := range treeIndex {
		if k == "" || strings.Count(k, "/") != 1 {
			continue
		}
		children = append(children, child{k, sec})
	}
	sort.Slice(children, func(i, j int) bool { return children[i].sec.Size > children[j].sec.Size })

	if len(children) > 0 {
		fmt.Println("Top-level directories:")
		for _, c := range children {
			cost := c.sec.monthlyCost(region)
			fmt.Printf("  %-40s  %d files   %s   $%.4f/month\n",
				c.prefix, c.sec.Count, humanSize(c.sec.Size), cost)
		}
		fmt.Println()
	}

	pricePerK := listCostForRegion(region)
	listCost := computeCost(listRequests, region)
	fmt.Printf("LIST run: %d requests @ $%.4f/1k = $%.6f\n", listRequests, pricePerK, listCost)
}
