// gen-demo generates a synthetic radix snapshot for screenshots and demos.
// Output: /tmp/s3du-demo.snap by default, override with -out.
//
// Usage:
//
//	go run ./internal/cmd/gen-demo
//	s3du -load -snapshot /tmp/s3du-demo.snap -i
//
// The keys are fully synthetic — vaguely plausible "data-lake / archive /
// logs" patterns across a few years, several storage classes, and a long
// tail of file sizes. Total objects: ~7 000. Total bytes: ~4 GiB.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"

	"github.com/ochaton/s3du/radix"
)

type objSpec struct {
	pathPattern []string
	sizeMin     int64
	sizeMax     int64
	count       int
	class       radix.StorageClass
}

// specs roughly model a multi-tenant data platform: a hot dataset on
// STANDARD, IA-tier intermediate files, GLACIER long-term backups,
// DEEP_ARCHIVE compliance retention, and an ad-hoc logs prefix.
var specs = []objSpec{
	{
		// Hot dataset: parquet partitions by year/month/day.
		pathPattern: []string{"datasets/clickstream/year=2025/month=%02d/day=%02d/part-%04d.parquet"},
		sizeMin:     8 << 20,
		sizeMax:     180 << 20,
		count:       1200,
		class:       radix.ClassStandard,
	},
	{
		pathPattern: []string{"datasets/clickstream/year=2024/month=%02d/day=%02d/part-%04d.parquet"},
		sizeMin:     4 << 20,
		sizeMax:     120 << 20,
		count:       2400,
		class:       radix.ClassStandardIA,
	},
	{
		pathPattern: []string{"datasets/users/year=2025/month=%02d/users-%04d.parquet"},
		sizeMin:     2 << 20,
		sizeMax:     20 << 20,
		count:       240,
		class:       radix.ClassStandard,
	},
	{
		pathPattern: []string{"datasets/products/snapshot=2025-Q3/products-%04d.parquet"},
		sizeMin:     500 << 10,
		sizeMax:     8 << 20,
		count:       300,
		class:       radix.ClassIntelligentTier,
	},
	{
		// Logs — many small files.
		pathPattern: []string{"logs/web-frontend/2025/%02d/%02d/access-%04d.json.gz"},
		sizeMin:     20 << 10,
		sizeMax:     2 << 20,
		count:       1800,
		class:       radix.ClassStandard,
	},
	{
		pathPattern: []string{"logs/api/2025/%02d/%02d/req-%04d.json.gz"},
		sizeMin:     50 << 10,
		sizeMax:     5 << 20,
		count:       900,
		class:       radix.ClassStandardIA,
	},
	{
		// Cold tier — big chunks, few of them.
		pathPattern: []string{"archive/db-dumps/2024/%02d/%02d/dump-%04d.tar.zst"},
		sizeMin:     200 << 20,
		sizeMax:     2 << 30,
		count:       60,
		class:       radix.ClassGlacier,
	},
	{
		pathPattern: []string{"archive/db-dumps/2023/%02d/%02d/dump-%04d.tar.zst"},
		sizeMin:     150 << 20,
		sizeMax:     1 << 30,
		count:       80,
		class:       radix.ClassDeepArchive,
	},
	{
		pathPattern: []string{"reports/quarterly/2025-Q%d/%s-report.pdf"},
		sizeMin:     500 << 10,
		sizeMax:     8 << 20,
		count:       40,
		class:       radix.ClassStandard,
	},
	{
		// Misc artifacts at the root.
		pathPattern: []string{"misc/exports/export-%04d.csv"},
		sizeMin:     10 << 10,
		sizeMax:     2 << 20,
		count:       50,
		class:       radix.ClassOneZoneIA,
	},
}

func main() {
	out := flag.String("out", "/tmp/s3du-demo.snap", "path for the generated snapshot")
	seed := flag.Uint64("seed", 1, "RNG seed for deterministic generation")
	flag.Parse()

	rng := rand.New(rand.NewPCG(*seed, *seed^0xdeadbeef))
	objs := generate(rng)

	tree := radix.New()
	const batchSize = 1000
	for start := 0; start < len(objs); start += batchSize {
		end := min(start+batchSize, len(objs))
		batch := radix.Batch{}
		if start > 0 {
			batch.StartFrom = objs[start-1].Key
		}
		batch.Objects = objs[start:end]
		if err := tree.AddBatch(batch); err != nil {
			fmt.Fprintln(os.Stderr, "AddBatch:", err)
			os.Exit(1)
		}
	}

	if err := tree.SaveFile(*out); err != nil {
		fmt.Fprintln(os.Stderr, "SaveFile:", err)
		os.Exit(1)
	}
	agg := tree.RootAggregate()
	fmt.Printf("wrote %s\n  objects: %d\n  total bytes: %s\n",
		*out, agg.Objects, humanBytes(agg.Bytes.Total()))
}

func generate(rng *rand.Rand) []radix.Object {
	var objs []radix.Object
	for _, sp := range specs {
		for i := range sp.count {
			key := materialiseKey(rng, sp.pathPattern[0], i)
			size := sp.sizeMin + rng.Int64N(max(sp.sizeMax-sp.sizeMin, 1))
			objs = append(objs, radix.Object{
				Key:   key,
				Size:  size,
				Class: sp.class,
			})
		}
	}
	slices.SortStableFunc(objs, func(a, b radix.Object) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		default:
			return 0
		}
	})
	objs = slices.CompactFunc(objs, func(a, b radix.Object) bool { return a.Key == b.Key })
	return objs
}

// materialiseKey fills in pattern placeholders with values shaped by i and
// rng so partitions span plausible date ranges and we don't collide on
// every iteration.
func materialiseKey(rng *rand.Rand, pattern string, i int) string {
	// Counts of "%" specifiers — we generate args accordingly.
	switch strings.Count(pattern, "%") {
	case 4:
		return fmt.Sprintf(pattern, 1+rng.IntN(12), 1+rng.IntN(28), i, i)
	case 3:
		return fmt.Sprintf(pattern, 1+rng.IntN(12), 1+rng.IntN(28), i)
	case 2:
		// %d + %s or %02d + %04d.
		if strings.Contains(pattern, "%s") {
			return fmt.Sprintf(pattern, 1+rng.IntN(4), reportName(rng))
		}
		return fmt.Sprintf(pattern, 1+rng.IntN(12), i)
	case 1:
		return fmt.Sprintf(pattern, i)
	default:
		return pattern
	}
}

func reportName(rng *rand.Rand) string {
	names := []string{"finance", "growth", "marketing", "ops", "engineering", "support"}
	return names[rng.IntN(len(names))]
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
