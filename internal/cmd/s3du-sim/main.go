// s3du-sim drives sim.Strategy implementations against a loaded radix
// snapshot and prints per-strategy request-count / parallelism summary.
// First-stage benchmarks: read-only — we never mutate the loaded tree.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/ochaton/s3du/radix"
	"github.com/ochaton/s3du/sim"
)

func main() {
	snapshot := flag.String("snapshot", "", "path to a radix snapshot to use as the simulated bucket")
	strategyName := flag.String("strategy", "", "strategy name (empty = run all)")
	workers := flag.Int("workers", 32, "worker count for parallel strategies")
	maxDepth := flag.Int("max-depth", 8, "max-depth knob for S3-style probe-and-recursive")
	latency := flag.Duration("latency", 0, "synthetic per-list latency to simulate S3 network round-trip")
	cpuProfile := flag.String("cpuprofile", "", "write a CPU profile to this path")
	memProfile := flag.String("memprofile", "", "write a heap profile to this path AFTER the run")
	showAllocs := flag.Bool("show-allocs", false, "report allocation deltas around each strategy run")
	flag.Parse()

	if *snapshot == "" {
		fmt.Fprintln(os.Stderr, "s3du-sim: -snapshot is required")
		fmt.Fprintln(os.Stderr, "available strategies:", strings.Join(sim.Names(), ", "))
		os.Exit(2)
	}

	loadStart := time.Now()
	tree, err := radix.LoadFile(*snapshot)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s3du-sim: load snapshot:", err)
		os.Exit(1)
	}
	root := tree.RootAggregate()
	fmt.Fprintf(os.Stderr, "loaded %d objects in %s\n",
		root.Objects, time.Since(loadStart).Round(time.Millisecond))

	var targets []string
	if *strategyName == "" {
		targets = sim.Names()
	} else {
		if sim.Lookup(*strategyName) == nil {
			fmt.Fprintf(os.Stderr, "s3du-sim: unknown strategy %q (have %v)\n", *strategyName, sim.Names())
			os.Exit(2)
		}
		targets = []string{*strategyName}
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cpuprofile:", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "cpuprofile start:", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	opts := sim.RunOpts{Workers: *workers, MaxDepth: *maxDepth}
	for _, name := range targets {
		strat := sim.Lookup(name)
		bucket := sim.New(tree, *latency)

		var m0 runtime.MemStats
		if *showAllocs {
			runtime.GC()
			runtime.ReadMemStats(&m0)
		}

		result, err := strat.Run(context.Background(), bucket, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			continue
		}
		fmt.Println(result)

		if *showAllocs {
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			fmt.Printf("   allocs: %d objects, %.2f GiB total, %.2f MiB live delta\n",
				m1.Mallocs-m0.Mallocs,
				float64(m1.TotalAlloc-m0.TotalAlloc)/(1<<30),
				float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/(1<<20),
			)
		}
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "memprofile:", err)
			return
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "memprofile write:", err)
		}
	}
}
