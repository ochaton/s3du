package main

import (
	"sync/atomic"
)

type WorkItem struct {
	Prefix string
	Depth  int
}

// FileEntry is a single object's data stored relative to its parent prefix.
type FileEntry struct {
	Name         string
	SizeBytes    int64
	StorageClass string
}

// taggedFile pairs a FileEntry with its immediate parent prefix.
type taggedFile struct {
	ParentPrefix string
	FileEntry
}

type Progress struct {
	listRequests     atomic.Int64
	objectsAccounted atomic.Int64
	bytesAccounted   atomic.Int64
}
