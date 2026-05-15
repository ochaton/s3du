package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var errWriter = os.Stderr

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name (required)")
	workers := flag.Int("workers", 32, "parallel worker count")
	region := flag.String("region", "", "AWS region (default: from env/profile)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: s3du -bucket <name> [-workers N] [-region r]\n\n")
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

	// Probe bucket region first — bucket may live in a different region than credentials.
	// GetBucketLocation returns empty string for us-east-1 (AWS quirk).
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

	prog := &Progress{}
	statsChan := make(chan StatBatch, 512)

	doneProg := make(chan struct{})
	go runProgress(prog, bucketRegion, doneProg)

	workChan := discover(ctx, client, *bucket, statsChan, prog)

	aggDone := make(chan *AggregatedStats, 1)
	go func() {
		aggDone <- aggregator(statsChan)
	}()

	runWorkers(ctx, client, *bucket, workChan, statsChan, *workers, prog)

	agg := <-aggDone
	close(doneProg)

	listReqs := prog.listRequests.Load()
	printResults(agg, listReqs, bucketRegion)
}

func printResults(agg *AggregatedStats, listRequests int64, region string) {
	// collect unique storage classes and prefixes
	classSet := map[string]struct{}{}
	prefixSet := map[string]struct{}{}
	for k := range agg.data {
		classSet[k.StorageClass] = struct{}{}
		prefixSet[k.Prefix] = struct{}{}
	}

	classes := sortedKeys(classSet)
	prefixes := sortedKeys(prefixSet)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	var grandCount, grandSize int64

	for _, sc := range classes {
		fmt.Fprintf(w, "\n[%s]\n", sc)
		fmt.Fprintf(w, "  Prefix\tCount\tSize\n")
		fmt.Fprintf(w, "  %s\t%s\t%s\n", strings.Repeat("-", 40), "-----", "--------")

		var classCount, classSize int64
		for _, p := range prefixes {
			key := PrefixClassKey{Prefix: p, StorageClass: sc}
			s, ok := agg.data[key]
			if !ok {
				continue
			}
			label := p
			if label == "" {
				label = "(root)"
			}
			fmt.Fprintf(w, "  %s\t%d\t%s\n", label, s.Count, humanSize(s.Size))
			classCount += s.Count
			classSize += s.Size
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\n", strings.Repeat("-", 40), "-----", "--------")
		fmt.Fprintf(w, "  TOTAL\t%d\t%s\n", classCount, humanSize(classSize))
		grandCount += classCount
		grandSize += classSize
	}

	cost := computeCost(listRequests, region)
	pricePerK := listCostForRegion(region)
	fmt.Fprintf(w, "\n[GRAND TOTAL]\n")
	fmt.Fprintf(w, "  Objects: %d\tSize: %s\n", grandCount, humanSize(grandSize))
	fmt.Fprintf(w, "\n[COST — %s]\n", region)
	fmt.Fprintf(w, "  LIST requests: %d\t@ $%.4f/1k\t= $%.6f\n", listRequests, pricePerK, cost)
	w.Flush()
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func aggregator(statsChan <-chan StatBatch) *AggregatedStats {
	agg := newAggregatedStats()
	for batch := range statsChan {
		agg.add(batch)
	}
	return agg
}
