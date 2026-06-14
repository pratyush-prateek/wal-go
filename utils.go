package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WalConfig configures a WAL.
type WalConfig struct {
	DataDirectory            string
	SnapshotDirectory        string
	MaxFileSize              int64
	MaxSegmentsUntilSnapshot int
	Mode                     int
	FlushInterval            time.Duration
	SnapshotProvider         SnapshotProvider
}

// NewWal opens or creates a WAL, recovering any existing state.
func NewWal(config WalConfig) (*WAL, error) {
	if config.DataDirectory == "" {
		return nil, fmt.Errorf("wal: DataDirectory is required")
	}
	if config.SnapshotDirectory == "" {
		config.SnapshotDirectory = filepath.Join(config.DataDirectory, "snapshots")
	}
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = defaultMaxFileSize
	}
	if config.MaxSegmentsUntilSnapshot <= 0 {
		config.MaxSegmentsUntilSnapshot = defaultMaxSegmentsUntilSnapshot
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = defaultFlushInterval
	}
	if err := os.MkdirAll(config.DataDirectory, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.SnapshotDirectory, 0755); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &WAL{
		dataDirectory:            config.DataDirectory,
		snapshotDirectory:        config.SnapshotDirectory,
		maxFileSize:              config.MaxFileSize,
		maxSegmentsUntilSnapshot: config.MaxSegmentsUntilSnapshot,
		mode:                     config.Mode,
		flushInterval:            config.FlushInterval,
		snapshotProvider:         config.SnapshotProvider,
		ctx:                      ctx,
		cancel:                   cancel,
	}
	w.syncCond = sync.NewCond(&w.flushLock)

	_, uptoLSN, err := loadLatestSnapshot(w.snapshotDirectory)
	if err != nil {
		cancel()
		return nil, err
	}
	w.compactionPoint = uptoLSN

	if err := w.recover(uptoLSN); err != nil {
		cancel()
		return nil, err
	}

	if w.snapshotProvider != nil {
		w.compactTrigger = make(chan struct{}, 1)
		w.bgWg.Add(1)
		go w.compactionLoop()
	}
	if w.mode == MODE_AUTO_FLUSH {
		w.startFlushLoop()
	}
	return w, nil
}

// recover rebuilds in-memory state from the segment files on disk.
func (w *WAL) recover(snapshotUptoLSN uint64) error {
	nums, err := listSegmentNumbers(w.dataDirectory)
	if err != nil {
		return err
	}
	for _, n := range nums {
		seg, _, serr := scanSegment(w.dataDirectory, n)
		if serr != nil {
			return serr
		}
		if len(seg.offsets) == 0 {
			os.Remove(seg.path) // drop empty segments
			continue
		}
		w.segments = append(w.segments, seg)
		w.lastLogSequenceNumber = seg.endLSN
	}

	if len(w.segments) == 0 {
		// fresh or fully compacted
		w.firstLogSequenceNumber = snapshotUptoLSN + 1
		w.lastLogSequenceNumber = snapshotUptoLSN
		return w.createSegment(0)
	}

	w.firstLogSequenceNumber = w.segments[0].startLSN

	// reopen the last segment for appending
	last := w.segments[len(w.segments)-1]
	f, err := os.OpenFile(last.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	w.currentSegment = f
	w.currentSegmentIndex = last.number
	w.currentSegmentBytes = end
	w.bufWriter = bufio.NewWriter(f)
	w.lastSyncedLogSequenceNumber = w.lastLogSequenceNumber
	return nil
}

// createSegment opens a fresh active segment.
func (w *WAL) createSegment(number int) error {
	path := filepath.Join(w.dataDirectory, segmentFileName(number))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	seg := &segment{number: number, path: path}
	w.segments = append(w.segments, seg)
	w.currentSegment = f
	w.currentSegmentIndex = number
	w.currentSegmentBytes = 0
	w.bufWriter = bufio.NewWriter(f)
	return nil
}

// startFlushLoop runs the auto-flush ticker.
func (w *WAL) startFlushLoop() {
	w.flushTimer = time.NewTicker(w.flushInterval)
	w.bgWg.Add(1)
	go func() {
		defer w.bgWg.Done()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-w.flushTimer.C:
				w.flushLock.Lock()
				if !w.closed && w.bufWriter != nil {
					_ = w.syncLocked()
				}
				w.flushLock.Unlock()
			}
		}
	}()
}
