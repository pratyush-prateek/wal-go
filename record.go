package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/pratyush-prateek/wal-go/walpb"
	"google.golang.org/protobuf/proto"
)

// maxRecordBytes guards against corrupt length prefixes.
const maxRecordBytes = 64 << 20

// encodeRecord frames a record for disk: [length][protobuf record].
func encodeRecord(lsn uint64, data []byte) ([]byte, error) {
	rec := &walpb.WalRecord{
		LogSequenceNumber: lsn,
		Data:              data,
		Checksum:          crc32.ChecksumIEEE(data),
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, lengthPrefixBytes+len(payload))
	binary.BigEndian.PutUint32(frame[:lengthPrefixBytes], uint32(len(payload)))
	copy(frame[lengthPrefixBytes:], payload)
	return frame, nil
}

// decodeRecordAt reads and validates the record at offset.
func decodeRecordAt(r io.ReaderAt, offset int64) (*walpb.WalRecord, int64, error) {
	var lenBuf [lengthPrefixBytes]byte
	if _, err := r.ReadAt(lenBuf[:], offset); err != nil {
		return nil, 0, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 || n > maxRecordBytes {
		return nil, 0, fmt.Errorf("wal: implausible record length %d at offset %d", n, offset)
	}
	payload := make([]byte, n)
	if _, err := r.ReadAt(payload, offset+lengthPrefixBytes); err != nil {
		return nil, 0, err
	}
	rec := &walpb.WalRecord{}
	if err := proto.Unmarshal(payload, rec); err != nil {
		return nil, 0, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
	}
	if crc32.ChecksumIEEE(rec.Data) != rec.Checksum {
		return nil, 0, fmt.Errorf("wal: checksum mismatch at offset %d", offset)
	}
	return rec, int64(lengthPrefixBytes) + int64(n), nil
}
