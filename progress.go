package main

import (
	"fmt"
	"time"
)

var spinFrames = []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

func runProgress(prog *Progress, region string, done <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	frame := 0
	for {
		select {
		case <-done:
			reqs := prog.listRequests.Load()
			objs := prog.objectsAccounted.Load()
			bytes := prog.bytesAccounted.Load()
			cost := computeCost(reqs, region)
			fmt.Fprintf(errWriter, "\r\033[K✓ Requests: %d | Objects: %d | Size: %s | Cost: $%.6f\n",
				reqs, objs, humanSize(bytes), cost)
			return
		case <-ticker.C:
			reqs := prog.listRequests.Load()
			objs := prog.objectsAccounted.Load()
			bytes := prog.bytesAccounted.Load()
			cost := computeCost(reqs, region)
			fmt.Fprintf(errWriter, "\r\033[K%c Requests: %d | Objects: %d | Size: %s | Cost: $%.6f",
				spinFrames[frame%len(spinFrames)], reqs, objs, humanSize(bytes), cost)
			frame++
		}
	}
}

func humanSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
