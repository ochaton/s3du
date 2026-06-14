// s3du-sim is a small driver that wires sim.Bucket against a loaded
// snapshot so we can poke at ListObjectsV2 semantics without leaving the
// laptop. First milestone: just exercise the API and report request
// counts. Strategies arrive in later commits.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ochaton/s3du/radix"
	"github.com/ochaton/s3du/sim"
)

func main() {
	snapshot := flag.String("snapshot", "", "path to a radix snapshot to use as the simulated bucket")
	prefix := flag.String("prefix", "", "Prefix for the test List call")
	delim := flag.String("delim", "/", "Delimiter for the test List call (\"\" for none)")
	maxKeys := flag.Int("max-keys", 1000, "MaxKeys per List call (capped at 1000)")
	pages := flag.Int("pages", 1, "number of pages to fetch (negative = until exhausted)")
	flag.Parse()

	if *snapshot == "" {
		fmt.Fprintln(os.Stderr, "s3du-sim: -snapshot is required")
		os.Exit(2)
	}

	tree, err := radix.LoadFile(*snapshot)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s3du-sim: load snapshot:", err)
		os.Exit(1)
	}
	root := tree.RootAggregate()
	fmt.Fprintf(os.Stderr, "loaded snapshot: %d objects across %d classes\n",
		root.Objects, len(root.Bytes))

	bucket := sim.New(tree, 0)
	ctx := context.Background()
	token := ""
	totalContents, totalCommonPrefixes := 0, 0
	start := time.Now()
	for i := 0; *pages < 0 || i < *pages; i++ {
		resp, err := bucket.List(ctx, sim.ListReq{
			Prefix:     *prefix,
			Delimiter:  *delim,
			StartAfter: token,
			MaxKeys:    *maxKeys,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "list:", err)
			os.Exit(1)
		}
		totalContents += len(resp.Contents)
		totalCommonPrefixes += len(resp.CommonPrefixes)
		if !resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	elapsed := time.Since(start)
	fmt.Printf("requests=%d  contents=%d  commonPrefixes=%d  elapsed=%s  obj/req=%.1f\n",
		bucket.Requests(),
		totalContents,
		totalCommonPrefixes,
		elapsed.Round(time.Millisecond),
		float64(totalContents+totalCommonPrefixes)/float64(max(bucket.Requests(), 1)),
	)
}
