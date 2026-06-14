package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// segmentFileName returns the zero-padded file name for a segment number.
func segmentFileName(number int) string {
	return fmt.Sprintf("%020d%s", number, SegmentExtension)
}

// parseSegmentNumber extracts the segment number from a file name.
func parseSegmentNumber(name string) (int, bool) {
	if !strings.HasSuffix(name, SegmentExtension) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(name, SegmentExtension))
	if err != nil {
		return 0, false
	}
	return n, true
}

// listSegmentNumbers returns the segment numbers in dir, sorted ascending.
func listSegmentNumbers(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if n, ok := parseSegmentNumber(e.Name()); ok {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	return nums, nil
}

// scanSegment rebuilds a segment's index, discarding any torn tail.
func scanSegment(dir string, number int) (*segment, int64, error) {
	path := filepath.Join(dir, segmentFileName(number))
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()

	seg := &segment{number: number, path: path}
	var offset int64
	for offset < size {
		rec, frameSize, derr := decodeRecordAt(f, offset)
		if derr != nil {
			break // torn/corrupt tail
		}
		if len(seg.offsets) == 0 {
			seg.startLSN = rec.LogSequenceNumber
		}
		seg.endLSN = rec.LogSequenceNumber
		seg.offsets = append(seg.offsets, offset)
		offset += frameSize
	}
	if offset < size {
		if err := f.Truncate(offset); err != nil {
			return nil, 0, err
		}
	}
	return seg, offset, nil
}
