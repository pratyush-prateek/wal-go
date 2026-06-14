package waltest

import (
	"fmt"
	"os"
	"testing"
	"time"

	wal "github.com/pratyush-prateek/wal-go"
)

func TestEmpty(t *testing.T) {
	w := newTestWAL(t, wal.WalConfig{Mode: wal.MODE_MANUAL_FLUSH})
	defer w.Close()
	if got := w.LastIndex(); got != 0 {
		t.Fatalf("LastIndex empty = %d, want 0", got)
	}
	if got := w.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex empty = %d, want 1", got)
	}
	if _, err := w.Read(1); err == nil {
		t.Fatalf("Read(1) on empty should error")
	}
}

func TestWriteReadBack(t *testing.T) {
	w := newTestWAL(t, wal.WalConfig{Mode: wal.MODE_MANUAL_FLUSH})
	defer w.Close()
	for i := 1; i <= 5; i++ {
		lsn, err := w.Write([]byte(fmt.Sprintf("entry-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if lsn != uint64(i) {
			t.Fatalf("Write returned lsn %d, want %d", lsn, i)
		}
	}
	if w.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", w.LastIndex())
	}
	for i := 1; i <= 5; i++ {
		data, err := w.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if want := fmt.Sprintf("entry-%d", i); string(data) != want {
			t.Fatalf("Read(%d) = %q, want %q", i, data, want)
		}
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH})
	for i := 1; i <= 10; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2 := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH})
	defer w2.Close()
	if w2.LastIndex() != 10 {
		t.Fatalf("after reopen LastIndex = %d, want 10", w2.LastIndex())
	}
	for i := 1; i <= 10; i++ {
		data, err := w2.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after reopen: %v", i, err)
		}
		if string(data) != fmt.Sprintf("v%d", i) {
			t.Fatalf("Read(%d) = %q", i, data)
		}
	}
	lsn, err := w2.Write([]byte("v11"))
	if err != nil || lsn != 11 {
		t.Fatalf("append after reopen lsn=%d err=%v", lsn, err)
	}
}

// TestWaitUntilSyncedAutoFlush verifies (black-box) that in auto-flush mode,
// WaitUntilSynced makes writes durable on disk: a second WAL opened on the same
// directory (before the first is closed) recovers every entry.
func TestWaitUntilSyncedAutoFlush(t *testing.T) {
	dir := t.TempDir()
	w := newTestWAL(t, wal.WalConfig{
		DataDirectory: dir,
		Mode:          wal.MODE_AUTO_FLUSH,
		FlushInterval: 10 * time.Millisecond,
	})
	defer w.Close()
	for i := 0; i < 20; i++ {
		if _, err := w.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WaitUntilSynced(); err != nil {
		t.Fatal(err)
	}
	// Open a second WAL on the same dir (first still open) — it can only see what
	// was fsync'd, so all 20 must be present.
	w2, err := wal.NewWal(wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.LastIndex() != 20 {
		t.Fatalf("durable entries after WaitUntilSynced = %d, want 20", w2.LastIndex())
	}
}

// TestSegmentRollover checks (black-box) that the log rolls into multiple
// segment files and reads/recovers correctly across them.
func TestSegmentRollover(t *testing.T) {
	dir := t.TempDir()
	w := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH, MaxFileSize: 64})
	for i := 1; i <= 50; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("entry-number-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()
	if n := countSegmentFiles(t, dir); n < 2 {
		t.Fatalf("expected multiple segment files, got %d", n)
	}
	for i := 1; i <= 50; i++ {
		data, err := w.Read(uint64(i))
		if err != nil || string(data) != fmt.Sprintf("entry-number-%d", i) {
			t.Fatalf("Read(%d) = %q, %v", i, data, err)
		}
	}
	w.Close()

	w2 := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH, MaxFileSize: 64})
	defer w2.Close()
	if w2.LastIndex() != 50 {
		t.Fatalf("recovered LastIndex = %d, want 50", w2.LastIndex())
	}
	if d, err := w2.Read(25); err != nil || string(d) != "entry-number-25" {
		t.Fatalf("Read(25) after recover = %q, %v", d, err)
	}
}

func TestTruncateSuffix(t *testing.T) {
	w := newTestWAL(t, wal.WalConfig{Mode: wal.MODE_MANUAL_FLUSH, MaxFileSize: 80})
	defer w.Close()
	for i := 1; i <= 20; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()

	if err := w.TruncateSuffix(11); err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 10 {
		t.Fatalf("after truncate LastIndex = %d, want 10", w.LastIndex())
	}
	if _, err := w.Read(11); err == nil {
		t.Fatalf("Read(11) after truncate should error")
	}
	if d, err := w.Read(10); err != nil || string(d) != "e10" {
		t.Fatalf("Read(10) = %q, %v", d, err)
	}
	lsn, err := w.Write([]byte("e11-new"))
	if err != nil || lsn != 11 {
		t.Fatalf("append after truncate lsn=%d err=%v", lsn, err)
	}
	if d, _ := w.Read(11); string(d) != "e11-new" {
		t.Fatalf("Read(11) = %q, want e11-new", d)
	}
}

func TestCompactionAndRecovery(t *testing.T) {
	dir := t.TempDir()
	provider := func() ([]byte, uint64, error) { return []byte("STATE"), 20, nil }

	cfg := wal.WalConfig{
		DataDirectory:            dir,
		Mode:                     wal.MODE_MANUAL_FLUSH,
		MaxFileSize:              40,
		MaxSegmentsUntilSnapshot: 1000, // manual compaction in this test
		SnapshotProvider:         provider,
	}
	w := newTestWAL(t, cfg)
	for i := 1; i <= 30; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()

	if err := w.CompactAndSnapshot(); err != nil {
		t.Fatal(err)
	}
	snap, upto, err := w.LatestSnapshot()
	if err != nil || string(snap) != "STATE" || upto != 20 {
		t.Fatalf("LatestSnapshot = %q upto=%d err=%v", snap, upto, err)
	}
	if w.FirstIndex() <= 1 {
		t.Fatalf("FirstIndex should advance after compaction, got %d", w.FirstIndex())
	}
	if _, err := w.Read(1); err == nil {
		t.Fatalf("Read(1) after compaction should error (compacted away)")
	}
	if d, err := w.Read(30); err != nil || string(d) != "e30" {
		t.Fatalf("Read(30) = %q, %v", d, err)
	}
	w.Close()

	w2 := newTestWAL(t, cfg)
	defer w2.Close()
	if w2.LastIndex() != 30 {
		t.Fatalf("recovered LastIndex = %d, want 30", w2.LastIndex())
	}
	if s, u, _ := w2.LatestSnapshot(); string(s) != "STATE" || u != 20 {
		t.Fatalf("recovered snapshot = %q upto=%d", s, u)
	}
	if d, err := w2.Read(30); err != nil || string(d) != "e30" {
		t.Fatalf("Read(30) after recover = %q, %v", d, err)
	}
	if _, err := w2.Read(1); err == nil {
		t.Fatalf("Read(1) after recover should error")
	}
}

// TestCorruptionRecovery checks (black-box) that a torn/garbage tail is
// discarded on replay while valid records survive.
func TestCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()
	w := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH})
	for i := 1; i <= 5; i++ {
		if _, err := w.WriteAndSync([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segPath := firstSegmentFile(t, dir)
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("garbage-bytes-not-a-valid-frame"))
	f.Close()

	w2 := newTestWAL(t, wal.WalConfig{DataDirectory: dir, Mode: wal.MODE_MANUAL_FLUSH})
	defer w2.Close()
	if w2.LastIndex() != 5 {
		t.Fatalf("after corruption recovery LastIndex = %d, want 5", w2.LastIndex())
	}
	for i := 1; i <= 5; i++ {
		if d, err := w2.Read(uint64(i)); err != nil || string(d) != fmt.Sprintf("e%d", i) {
			t.Fatalf("Read(%d) = %q, %v", i, d, err)
		}
	}
}
