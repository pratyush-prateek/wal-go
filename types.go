package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
)

// WAL struct
type WAL struct {
	dataDirectory     string
	snapshotDirectory string

	flushLock    sync.Mutex
	snapshotLock sync.Mutex
	syncCond     *sync.Cond

	segments            []*segment
	currentSegment      *os.File
	bufWriter           *bufio.Writer
	currentSegmentBytes int64
	currentSegmentIndex int

	firstLogSequenceNumber      uint64
	lastLogSequenceNumber       uint64
	lastSyncedLogSequenceNumber uint64
	compactionPoint             uint64

	maxFileSize              int64
	maxSegmentsUntilSnapshot int
	mode                     int
	flushInterval            time.Duration
	flushTimer               *time.Ticker
	snapshotProvider         SnapshotProvider
	compactTrigger           chan struct{}

	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	bgWg   sync.WaitGroup
}

// segment is one on-disk log file for a contiguous LSN range.
type segment struct {
	number   int
	path     string
	startLSN uint64
	endLSN   uint64
	offsets  []int64
}

// Constants
const (
	SegmentExtension  = ".seg"
	SnapshotExtension = ".snap"
)

// Modes for WAL operations
const (
	MODE_AUTO_FLUSH   = 0
	MODE_MANUAL_FLUSH = 1
)

// Defaults
const (
	defaultMaxFileSize              = 16 << 20
	defaultMaxSegmentsUntilSnapshot = 8
	defaultFlushInterval            = 200 * time.Millisecond
	lengthPrefixBytes               = 4
)
