package waltest

import (
	"os"
	"strings"
	"testing"

	wal "github.com/pratyush-prateek/wal-go"
)

// TestSnapshotFileNotLeaked verifies repeated compactions overwrite a single
// snapshot file rather than leaving superseded ones behind.
func TestSnapshotFileNotLeaked(t *testing.T) {
	snapDir := t.TempDir()
	var upto uint64
	cfg := wal.WalConfig{
		DataDirectory:            t.TempDir(),
		SnapshotDirectory:        snapDir,
		Mode:                     wal.MODE_MANUAL_FLUSH,
		MaxFileSize:              60,
		MaxSegmentsUntilSnapshot: 10000, // disable auto-compaction; trigger manually
		SnapshotProvider:         func() ([]byte, uint64, error) { return []byte("S"), upto, nil },
	}
	w := newTestWAL(t, cfg)
	defer w.Close()

	for round := 0; round < 5; round++ {
		for j := 0; j < 10; j++ {
			if _, err := w.WriteAndSync([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		upto = w.LastIndex()
		if err := w.CompactAndSnapshot(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), wal.SnapshotExtension) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 snapshot file after 5 compactions, got %d", n)
	}
	if _, gotUpto, err := w.LatestSnapshot(); err != nil || gotUpto != upto {
		t.Fatalf("LatestSnapshot upto = %d (err %v), want %d", gotUpto, err, upto)
	}
}
