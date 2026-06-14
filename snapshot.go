package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/pratyush-prateek/wal-go/walpb"
	"google.golang.org/protobuf/proto"
)

// snapshotFile is the single snapshot file, overwritten on each compaction.
const snapshotFile = "snapshot" + SnapshotExtension

// writeSnapshot atomically overwrites the snapshot file (temp + rename + dir fsync).
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
	final := filepath.Join(dir, snapshotFile)
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
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so a rename within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// loadLatestSnapshot returns the stored snapshot, or (nil, 0, nil) if none.
func loadLatestSnapshot(dir string) ([]byte, uint64, error) {
	payload, err := os.ReadFile(filepath.Join(dir, snapshotFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
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
