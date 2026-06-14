package waltest

import (
	"fmt"
	"testing"

	wal "github.com/pratyush-prateek/wal-go"
)

func benchWAL(b *testing.B) *wal.WAL {
	b.Helper()
	w, err := wal.NewWal(wal.WalConfig{
		DataDirectory: b.TempDir(),
		Mode:          wal.MODE_MANUAL_FLUSH,
		MaxFileSize:   1 << 30, // single segment, isolate the write path
	})
	if err != nil {
		b.Fatal(err)
	}
	return w
}

// ---- single-threaded ----

func BenchmarkWrite(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteAndSync(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.WriteAndSync(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupCommit(b *testing.B) {
	for _, batch := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			w := benchWAL(b)
			defer w.Close()
			p := benchPayload(128)
			b.SetBytes(int64(len(p)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Write(p); err != nil {
					b.Fatal(err)
				}
				if i%batch == batch-1 {
					if err := w.Sync(); err != nil {
						b.Fatal(err)
					}
				}
			}
			w.Sync()
		})
	}
}

func BenchmarkWriteSizes(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			w := benchWAL(b)
			defer w.Close()
			p := benchPayload(size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Write(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	const n = 50000
	for i := 0; i < n; i++ {
		if _, err := w.Write(p); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	var i uint64
	for j := 0; j < b.N; j++ {
		i++
		if _, err := w.Read((i % n) + 1); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- concurrent ----

// BenchmarkConcurrentWrite: many goroutines appending (buffered, no fsync).
func BenchmarkConcurrentWrite(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := w.Write(p); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkConcurrentWriteAndSync: many goroutines doing durable writes.
func BenchmarkConcurrentWriteAndSync(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := w.WriteAndSync(p); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkConcurrentRead: many goroutines reading from a pre-populated log.
func BenchmarkConcurrentRead(b *testing.B) {
	w := benchWAL(b)
	defer w.Close()
	p := benchPayload(128)
	const n = 50000
	for i := 0; i < n; i++ {
		if _, err := w.Write(p); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			i++
			if _, err := w.Read((i % n) + 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}
