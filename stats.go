package main

import (
	"sync/atomic"
)

type WorkItem struct {
	Prefix string
	Depth  int
}

type StatBatch struct {
	Prefix       string
	StorageClass string
	Count        int64
	SizeBytes    int64
}

type PrefixClassKey struct {
	Prefix       string
	StorageClass string
}

type PrefixStats struct {
	Count int64
	Size  int64
}

type AggregatedStats struct {
	data map[PrefixClassKey]*PrefixStats
}

func newAggregatedStats() *AggregatedStats {
	return &AggregatedStats{data: make(map[PrefixClassKey]*PrefixStats)}
}

func (a *AggregatedStats) add(batch StatBatch) {
	key := PrefixClassKey{Prefix: batch.Prefix, StorageClass: batch.StorageClass}
	if s, ok := a.data[key]; ok {
		s.Count += batch.Count
		s.Size += batch.SizeBytes
	} else {
		a.data[key] = &PrefixStats{Count: batch.Count, Size: batch.SizeBytes}
	}
}

type Progress struct {
	listRequests     atomic.Int64
	objectsAccounted atomic.Int64
	bytesAccounted   atomic.Int64
}
