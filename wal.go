package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

/*
*
Contract for WAL operations
- Write: append an entry
- WriteAndSync: append an entry and sync
- Sync: flush and sync to disk
- WaitUntilSynced: block until durable (auto-flush mode)
- Read: read the entry at an index
- FirstIndex: lowest index still on disk
- LastIndex: highest index in the log
- TruncateSuffix: delete entries from an index onwards
- LatestSnapshot: latest snapshot and the index it covers
- CompactAndSnapshot: force a compaction now
- Close: flush, sync and release resources
*/
type WalContract interface {
	Write(entry []byte) (logSequenceNumber uint64, err error)
	WriteAndSync(entry []byte) (logSequenceNumber uint64, err error)
	Sync() error
	WaitUntilSynced() error
	Read(index uint64) (entry []byte, err error)
	FirstIndex() uint64
	LastIndex() uint64
	TruncateSuffix(fromIndex uint64) error
	LatestSnapshot() (snapshot []byte, uptoIndex uint64, err error)
	CompactAndSnapshot() error
	Close() error
}

/*
*
SnapshotProvider materializes a snapshot during compaction (supplied by the app).
*/
type SnapshotProvider func() (snapshot []byte, uptoIndex uint64, err error)

var _ WalContract = (*WAL)(nil)

var (
	errClosed             = errors.New("wal: closed")
	errNoSnapshotProvider = errors.New("wal: no snapshot provider configured")
)

// Write appends an entry.
func (w *WAL) Write(entry []byte) (uint64, error) {
	w.flushLock.Lock()
	if w.closed {
		w.flushLock.Unlock()
		return 0, errClosed
	}
	lsn, err := w.appendLocked(entry)
	if err != nil {
		w.flushLock.Unlock()
		return 0, err
	}
	compact := w.shouldCompactLocked()
	w.flushLock.Unlock()
	if compact {
		w.triggerCompaction()
	}
	return lsn, nil
}

// WriteAndSync appends an entry and fsyncs.
func (w *WAL) WriteAndSync(entry []byte) (uint64, error) {
	w.flushLock.Lock()
	if w.closed {
		w.flushLock.Unlock()
		return 0, errClosed
	}
	lsn, err := w.appendLocked(entry)
	if err != nil {
		w.flushLock.Unlock()
		return 0, err
	}
	if err := w.syncLocked(); err != nil {
		w.flushLock.Unlock()
		return 0, err
	}
	compact := w.shouldCompactLocked()
	w.flushLock.Unlock()
	if compact {
		w.triggerCompaction()
	}
	return lsn, nil
}

// Sync flushes and fsyncs.
func (w *WAL) Sync() error {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	if w.closed {
		return errClosed
	}
	return w.syncLocked()
}

// WaitUntilSynced blocks until writes so far are durable.
func (w *WAL) WaitUntilSynced() error {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	target := w.lastLogSequenceNumber
	for w.lastSyncedLogSequenceNumber < target && !w.closed {
		w.syncCond.Wait()
	}
	if w.closed {
		return errClosed
	}
	return nil
}

// Read returns the entry bytes at the given log sequence number.
func (w *WAL) Read(lsn uint64) ([]byte, error) {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	if w.closed {
		return nil, errClosed
	}
	if w.lastLogSequenceNumber == 0 || lsn < w.firstLogSequenceNumber || lsn > w.lastLogSequenceNumber {
		return nil, fmt.Errorf("wal: lsn %d out of range [%d, %d]", lsn, w.firstLogSequenceNumber, w.lastLogSequenceNumber)
	}
	if err := w.bufWriter.Flush(); err != nil {
		return nil, err
	}
	seg := w.findSegmentLocked(lsn)
	if seg == nil {
		return nil, fmt.Errorf("wal: no segment for lsn %d", lsn)
	}
	return w.readAtLocked(seg, lsn)
}

// FirstIndex is the lowest available LSN.
func (w *WAL) FirstIndex() uint64 {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	return w.firstLogSequenceNumber
}

// LastIndex is the highest LSN in the log (0 if empty).
func (w *WAL) LastIndex() uint64 {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	return w.lastLogSequenceNumber
}

// TruncateSuffix deletes entries with LSN >= fromLSN.
func (w *WAL) TruncateSuffix(fromLSN uint64) error {
	w.flushLock.Lock()
	defer w.flushLock.Unlock()
	if w.closed {
		return errClosed
	}
	if w.lastLogSequenceNumber == 0 || fromLSN > w.lastLogSequenceNumber {
		return nil
	}
	if fromLSN < w.firstLogSequenceNumber {
		fromLSN = w.firstLogSequenceNumber
	}
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if err := w.currentSegment.Close(); err != nil {
		return err
	}
	w.currentSegment = nil
	w.bufWriter = nil

	var kept []*segment
	var active *segment
	for _, seg := range w.segments {
		switch {
		case len(seg.offsets) == 0:
			os.Remove(seg.path)
		case seg.startLSN >= fromLSN:
			os.Remove(seg.path)
		case seg.endLSN >= fromLSN:
			idx := int(fromLSN - seg.startLSN)
			truncOffset := seg.offsets[idx]
			f, err := os.OpenFile(seg.path, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			if err := f.Truncate(truncOffset); err != nil {
				f.Close()
				return err
			}
			f.Close()
			seg.offsets = seg.offsets[:idx]
			if idx == 0 {
				seg.startLSN, seg.endLSN = 0, 0
			} else {
				seg.endLSN = fromLSN - 1
			}
			kept = append(kept, seg)
			active = seg
		default:
			kept = append(kept, seg)
		}
	}
	w.segments = kept
	w.lastLogSequenceNumber = fromLSN - 1

	if active == nil && len(kept) > 0 {
		active = kept[len(kept)-1]
	}
	if active == nil {
		return w.createSegment(w.currentSegmentIndex + 1)
	}
	f, err := os.OpenFile(active.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	w.currentSegment = f
	w.currentSegmentIndex = active.number
	w.currentSegmentBytes = end
	w.bufWriter = bufio.NewWriter(f)
	return nil
}

// LatestSnapshot returns the most recent snapshot for recovery.
func (w *WAL) LatestSnapshot() ([]byte, uint64, error) {
	w.snapshotLock.Lock()
	defer w.snapshotLock.Unlock()
	return loadLatestSnapshot(w.snapshotDirectory)
}

// CompactAndSnapshot forces a compaction now.
func (w *WAL) CompactAndSnapshot() error {
	return w.runCompaction()
}

// Close flushes, syncs, and releases resources.
func (w *WAL) Close() error {
	w.flushLock.Lock()
	if w.closed {
		w.flushLock.Unlock()
		return nil
	}
	w.closed = true
	var err error
	if w.bufWriter != nil {
		if e := w.bufWriter.Flush(); e != nil {
			err = e
		}
	}
	if w.currentSegment != nil {
		if e := w.currentSegment.Sync(); e != nil && err == nil {
			err = e
		}
		if e := w.currentSegment.Close(); e != nil && err == nil {
			err = e
		}
	}
	w.syncCond.Broadcast()
	w.flushLock.Unlock()

	w.cancel()
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	w.bgWg.Wait()
	return err
}

// appendLocked appends one record to the active segment.
func (w *WAL) appendLocked(entry []byte) (uint64, error) {
	lsn := w.lastLogSequenceNumber + 1
	frame, err := encodeRecord(lsn, entry)
	if err != nil {
		return 0, err
	}
	if w.currentSegmentBytes > 0 && w.currentSegmentBytes+int64(len(frame)) > w.maxFileSize {
		if err := w.rollSegmentLocked(); err != nil {
			return 0, err
		}
	}
	offset := w.currentSegmentBytes
	if _, err := w.bufWriter.Write(frame); err != nil {
		return 0, err
	}
	seg := w.segments[len(w.segments)-1]
	if len(seg.offsets) == 0 {
		seg.startLSN = lsn
	}
	seg.endLSN = lsn
	seg.offsets = append(seg.offsets, offset)
	w.currentSegmentBytes += int64(len(frame))
	w.lastLogSequenceNumber = lsn
	return lsn, nil
}

// syncLocked flushes and fsyncs.
func (w *WAL) syncLocked() error {
	if w.bufWriter == nil || w.currentSegment == nil {
		return nil
	}
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if err := w.currentSegment.Sync(); err != nil {
		return err
	}
	w.lastSyncedLogSequenceNumber = w.lastLogSequenceNumber
	w.syncCond.Broadcast()
	return nil
}

// rollSegmentLocked seals the active segment and opens a new one.
func (w *WAL) rollSegmentLocked() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if err := w.currentSegment.Sync(); err != nil {
		return err
	}
	if err := w.currentSegment.Close(); err != nil {
		return err
	}
	w.lastSyncedLogSequenceNumber = w.lastLogSequenceNumber
	return w.createSegment(w.currentSegmentIndex + 1)
}

// shouldCompactLocked reports whether auto-compaction should run.
func (w *WAL) shouldCompactLocked() bool {
	return w.snapshotProvider != nil && w.sealedSegmentCount() >= w.maxSegmentsUntilSnapshot
}

// sealedSegmentCount is the number of sealed segments.
func (w *WAL) sealedSegmentCount() int {
	if len(w.segments) == 0 {
		return 0
	}
	return len(w.segments) - 1
}

// findSegmentLocked returns the segment holding lsn.
func (w *WAL) findSegmentLocked(lsn uint64) *segment {
	for _, seg := range w.segments {
		if len(seg.offsets) > 0 && lsn >= seg.startLSN && lsn <= seg.endLSN {
			return seg
		}
	}
	return nil
}

// readAtLocked reads the record for lsn from seg.
func (w *WAL) readAtLocked(seg *segment, lsn uint64) ([]byte, error) {
	idx := int(lsn - seg.startLSN)
	if idx < 0 || idx >= len(seg.offsets) {
		return nil, fmt.Errorf("wal: lsn %d not in segment %d", lsn, seg.number)
	}
	offset := seg.offsets[idx]
	if seg.number == w.currentSegmentIndex && w.currentSegment != nil {
		rec, _, err := decodeRecordAt(w.currentSegment, offset)
		if err != nil {
			return nil, err
		}
		return rec.Data, nil
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rec, _, err := decodeRecordAt(f, offset)
	if err != nil {
		return nil, err
	}
	return rec.Data, nil
}

// runCompaction snapshots via the provider and drops obsolete segments.
func (w *WAL) runCompaction() error {
	w.snapshotLock.Lock()
	defer w.snapshotLock.Unlock()

	if w.snapshotProvider == nil {
		return errNoSnapshotProvider
	}
	data, uptoLSN, err := w.snapshotProvider()
	if err != nil {
		return err
	}

	w.flushLock.Lock()
	stale := uptoLSN <= w.compactionPoint
	w.flushLock.Unlock()
	if stale {
		return nil
	}

	if err := writeSnapshot(w.snapshotDirectory, uptoLSN, data); err != nil {
		return err
	}

	w.flushLock.Lock()
	var kept, toDelete []*segment
	for i, seg := range w.segments {
		active := i == len(w.segments)-1
		if !active && len(seg.offsets) > 0 && seg.endLSN <= uptoLSN {
			toDelete = append(toDelete, seg)
		} else {
			kept = append(kept, seg)
		}
	}
	w.segments = kept
	w.compactionPoint = uptoLSN
	w.firstLogSequenceNumber = uptoLSN + 1
	for _, seg := range kept {
		if len(seg.offsets) > 0 {
			w.firstLogSequenceNumber = seg.startLSN
			break
		}
	}
	w.flushLock.Unlock()

	for _, seg := range toDelete {
		os.Remove(seg.path)
	}
	return nil
}

// triggerCompaction signals the background goroutine to compact (coalesced).
func (w *WAL) triggerCompaction() {
	if w.compactTrigger == nil {
		return
	}
	select {
	case w.compactTrigger <- struct{}{}:
	default:
	}
}

// compactionLoop runs requested compactions until close.
func (w *WAL) compactionLoop() {
	defer w.bgWg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.compactTrigger:
			_ = w.runCompaction()
		}
	}
}
