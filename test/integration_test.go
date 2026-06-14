package waltest

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	wal "github.com/pratyush-prateek/wal-go"
)

// TestKVStoreIntegration drives a KV store on top of the WAL using BACKGROUND
// (auto) compaction, and verifies the recovered state after a restart — first
// once auto-compaction has fired, then again after it has fired a second time.
func TestKVStoreIntegration(t *testing.T) {
	dir := t.TempDir()
	base := wal.WalConfig{
		DataDirectory:            dir,
		Mode:                     wal.MODE_MANUAL_FLUSH,
		MaxFileSize:              150, // small -> several segments
		MaxSegmentsUntilSnapshot: 2,   // auto-compaction fires after 2 sealed segments
	}

	open := func(s *kvStore) *wal.WAL {
		c := base
		c.SnapshotProvider = s.snapshot
		w, err := wal.NewWal(c)
		if err != nil {
			t.Fatalf("NewWal: %v", err)
		}
		return w
	}

	recoverStore := func() (*kvStore, *wal.WAL) {
		s := newKVStore()
		w := open(s)
		blob, upto, err := w.LatestSnapshot()
		if err != nil {
			t.Fatalf("LatestSnapshot: %v", err)
		}
		s.restore(blob, upto)
		for lsn := upto + 1; lsn <= w.LastIndex(); lsn++ {
			raw, err := w.Read(lsn)
			if err != nil {
				t.Fatalf("Read(%d): %v", lsn, err)
			}
			s.apply(lsn, raw)
		}
		return s, w
	}

	waitForCompaction := func(w *wal.WAL, prevUpto uint64) uint64 {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, upto, err := w.LatestSnapshot()
			if err != nil {
				t.Fatalf("LatestSnapshot: %v", err)
			}
			if upto > prevUpto {
				return upto
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("auto-compaction did not advance snapshot past %d in time", prevUpto)
		return 0
	}

	apply := func(s *kvStore, w *wal.WAL, op, key, value string) {
		cmd := encodeKV(op, key, value)
		lsn, err := w.WriteAndSync(cmd)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		s.apply(lsn, cmd)
	}

	// ===== phase 1: write until auto-compaction happens ONCE =====
	store := newKVStore()
	w := open(store)
	for i := 0; i < 40; i++ {
		apply(store, w, "put", fmt.Sprintf("k%d", i%8), fmt.Sprintf("v-%d", i))
	}
	apply(store, w, "del", "k3", "")
	waitForCompaction(w, 0)
	want1 := cloneStringMap(store.data)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// ----- restart #1: state recovered via the auto-compaction snapshot + tail -----
	rs1, rw1 := recoverStore()
	if !reflect.DeepEqual(rs1.data, want1) {
		t.Fatalf("restart after compaction #1: got %v want %v", rs1.data, want1)
	}
	if _, ok := rs1.data["k3"]; ok {
		t.Fatalf("k3 should be deleted after compaction #1")
	}
	_, base2, _ := rw1.LatestSnapshot()

	// ===== phase 2: write more until auto-compaction happens AGAIN (twice) =====
	for i := 40; i < 90; i++ {
		apply(rs1, rw1, "put", fmt.Sprintf("k%d", i%8), fmt.Sprintf("v-%d", i))
	}
	apply(rs1, rw1, "put", "k3", "back")
	upto2 := waitForCompaction(rw1, base2)
	if upto2 <= base2 {
		t.Fatalf("second compaction did not advance: base2=%d upto2=%d", base2, upto2)
	}
	want2 := cloneStringMap(rs1.data)
	if err := rw1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// ----- restart #2: state recovered after TWO auto-compaction cycles -----
	rs2, rw2 := recoverStore()
	defer rw2.Close()
	if !reflect.DeepEqual(rs2.data, want2) {
		t.Fatalf("restart after compaction #2: got %v want %v", rs2.data, want2)
	}
	if rs2.data["k3"] != "back" {
		t.Fatalf("k3 = %q, want back", rs2.data["k3"])
	}
}
