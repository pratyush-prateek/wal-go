package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pratyush-prateek/wal-go/walpb"
	"google.golang.org/protobuf/proto"
)

// snapshotFileName names a snapshot by the LSN it covers up to.
func snapshotFileName(uptoLSN uint64) string {
	return fmt.Sprintf("%020d%s", uptoLSN, SnapshotExtension)
}

// writeSnapshot persists a snapshot atomically (temp file + rename).
func writeSnapshot(dir string, uptoLSN uint64, data []byte) error {
	snap := &walpb.WalSnapshot{
		UptoLogSequenceNumber: uptoLSN,
		Data:                  data,
		Checksum:              crc32.ChecksumIEEE(data),
	}
	payload, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	final := filepath.Join(dir, snapshotFileName(uptoLSN))
	tmp := final + ".tmp"

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// loadLatestSnapshot returns the snapshot with the highest upto-LSN, or
// (nil, 0, nil) if none exist.
func loadLatestSnapshot(dir string) ([]byte, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	found := false
	var bestLSN uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), SnapshotExtension) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), SnapshotExtension)
		n, perr := strconv.ParseUint(base, 10, 64)
		if perr != nil {
			continue
		}
		if !found || n > bestLSN {
			found, bestLSN = true, n
		}
	}
	if !found {
		return nil, 0, nil
	}

	payload, err := os.ReadFile(filepath.Join(dir, snapshotFileName(bestLSN)))
	if err != nil {
		return nil, 0, err
	}
	snap := &walpb.WalSnapshot{}
	if err := proto.Unmarshal(payload, snap); err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(snap.Data) != snap.Checksum {
		return nil, 0, fmt.Errorf("wal: snapshot checksum mismatch")
	}
	return snap.Data, snap.UptoLogSequenceNumber, nil
}
