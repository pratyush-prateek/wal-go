package waltest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	wal "github.com/pratyush-prateek/wal-go"
)

func newTestWAL(t *testing.T, cfg wal.WalConfig) *wal.WAL {
	t.Helper()
	if cfg.DataDirectory == "" {
		cfg.DataDirectory = t.TempDir()
	}
	w, err := wal.NewWal(cfg)
	if err != nil {
		t.Fatalf("NewWal: %v", err)
	}
	return w
}

func benchPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte('a' + i%26)
	}
	return p
}

func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	c := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), wal.SegmentExtension) {
			c++
		}
	}
	return c
}

func firstSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), wal.SegmentExtension) {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no segment file found in %s", dir)
	return ""
}

// ---- a tiny key-value state machine used by the integration test ----

type kvCommand struct {
	Op    string `json:"op"` // "put" | "del"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func encodeKV(op, key, value string) []byte {
	b, _ := json.Marshal(kvCommand{Op: op, Key: key, Value: value})
	return b
}

type kvStore struct {
	mu         sync.Mutex
	data       map[string]string
	appliedLSN uint64
}

func newKVStore() *kvStore { return &kvStore{data: make(map[string]string)} }

func (s *kvStore) apply(lsn uint64, raw []byte) {
	var c kvCommand
	if err := json.Unmarshal(raw, &c); err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "put":
		s.data[c.Key] = c.Value
	case "del":
		delete(s.data, c.Key)
	}
	s.appliedLSN = lsn
}

// snapshot is the WAL SnapshotProvider.
func (s *kvStore) snapshot() ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, err := json.Marshal(s.data)
	return blob, s.appliedLSN, err
}

func (s *kvStore) restore(blob []byte, upto uint64) {
	m := make(map[string]string)
	if len(blob) > 0 {
		if err := json.Unmarshal(blob, &m); err != nil {
			panic(err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = m
	s.appliedLSN = upto
}

func cloneStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
