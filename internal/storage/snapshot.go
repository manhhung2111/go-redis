package storage

import (
	"errors"
	"fmt"
	"hash/crc64"
	"time"

	"github.com/manhhung2111/go-redis/internal/codec"
	"github.com/manhhung2111/go-redis/internal/storage/types"
)

// Snapshot file layout (see docs/design/snapshot.md §6):
//
//	Header   := magic "GREDIS" | version uint16 | createdAtMs uint64
//	Entry    := opEntry | objType uint8 | encoding uint8 | expireAtMs uint64
//	            | key (uvarint len + bytes) | value (uint64 len + bytes)
//	EOF      := opEOF
//	Checksum := CRC-64/ECMA uint64 of every preceding byte
const (
	SnapshotVersion uint16 = 1

	snapshotOpEntry uint8 = 0x01
	snapshotOpEOF   uint8 = 0xFF

	snapshotMagic        = "GREDIS"
	snapshotHeaderSize   = len(snapshotMagic) + 2 + 8
	snapshotChecksumSize = 8
)

var (
	snapshotCRCTable = crc64.MakeTable(crc64.ECMA)

	ErrSnapshotNotEmpty = errors.New("snapshot: store must be empty before loading")
)

func (s *store) KeyCount() int {
	return s.data.Len()
}

func (s *store) Snapshot() ([]byte, error) {
	nowMs := uint64(time.Now().UnixMilli())

	e := codec.NewEncoder(s.snapshotSizeHint())
	e.WriteRawString(snapshotMagic)
	e.WriteUint16(SnapshotVersion)
	e.WriteUint64(nowMs)

	var err error
	s.data.ForEach(func(key string, obj *RObj) {
		if err != nil {
			return
		}

		expireAt, hasExpire := s.expires.Get(key)
		if hasExpire && expireAt <= nowMs {
			return
		}
		if !hasExpire {
			expireAt = 0
		}

		e.WriteUint8(snapshotOpEntry)
		e.WriteUint8(uint8(obj.objType))
		e.WriteUint8(uint8(obj.encoding))
		e.WriteUint64(expireAt)
		e.WriteString(key)

		lenOffset := e.ReserveUint64()
		start := e.Len()
		if err = encodeValue(e, obj); err != nil {
			err = fmt.Errorf("snapshot: key %q: %w", key, err)
			return
		}
		e.PatchUint64(lenOffset, uint64(e.Len()-start))
	})
	if err != nil {
		return nil, err
	}

	e.WriteUint8(snapshotOpEOF)
	e.WriteUint64(crc64.Checksum(e.Bytes(), snapshotCRCTable))
	return e.Bytes(), nil
}

// snapshotSizeHint estimates the encoded size from tracked memory usage to
// avoid repeated buffer growth. Encoded data is usually smaller than its
// in-memory representation, so the hint is capped.
func (s *store) snapshotSizeHint() int {
	const maxHint = 64 << 20
	return int(min(max(s.usedMemory/2, 4096), maxHint))
}

func encodeValue(e *codec.Encoder, obj *RObj) error {
	switch obj.encoding {
	case EncRaw:
		str, ok := obj.value.(string)
		if !ok {
			return fmt.Errorf("unexpected raw string value %T", obj.value)
		}
		e.WriteRawString(str)
	case EncInt:
		n, ok := obj.value.(int64)
		if !ok {
			return fmt.Errorf("unexpected int value %T", obj.value)
		}
		e.WriteVarint(n)
	default:
		v, ok := obj.value.(types.Encodable)
		if !ok {
			return fmt.Errorf("value %T is not encodable", obj.value)
		}
		v.Encode(e)
	}
	return nil
}

func (s *store) LoadSnapshot(data []byte) error {
	if !s.data.Empty() {
		return ErrSnapshotNotEmpty
	}

	if len(data) < snapshotHeaderSize+1+snapshotChecksumSize {
		return errors.New("snapshot: file too short")
	}
	if string(data[:len(snapshotMagic)]) != snapshotMagic {
		return errors.New("snapshot: invalid magic header")
	}

	body := data[:len(data)-snapshotChecksumSize]
	wantCRC := codec.NewDecoder(data[len(body):]).ReadUint64()
	if crc64.Checksum(body, snapshotCRCTable) != wantCRC {
		return errors.New("snapshot: checksum mismatch")
	}

	d := codec.NewDecoder(body[len(snapshotMagic):])
	if version := d.ReadUint16(); version != SnapshotVersion {
		return fmt.Errorf("snapshot: unsupported version %d (supported: %d)", version, SnapshotVersion)
	}
	d.ReadUint64() // createdAtMs, informational only

	// Load into fresh dicts so a failure leaves the store untouched
	newData, dataDelta := newDict[string, *RObj]()
	newExpires, expiresDelta := newDict[string, uint64]()
	usedMemory := dataDelta + expiresDelta
	nowMs := uint64(time.Now().UnixMilli())

	for {
		op := d.ReadUint8()
		if err := d.Err(); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		if op == snapshotOpEOF {
			break
		}
		if op != snapshotOpEntry {
			return fmt.Errorf("snapshot: unknown opcode 0x%02x", op)
		}

		objType := ObjectType(d.ReadUint8())
		encoding := ObjectEncoding(d.ReadUint8())
		expireAt := d.ReadUint64()
		key := d.ReadString()
		valueLen := d.ReadUint64()
		if err := d.Err(); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		if valueLen > uint64(d.Remaining()) {
			return fmt.Errorf("snapshot: key %q: value length %d exceeds file size", key, valueLen)
		}
		valueBytes := d.ReadRaw(int(valueLen))

		if expireAt != 0 && expireAt <= nowMs {
			continue
		}

		value, err := decodeValue(objType, encoding, valueBytes)
		if err != nil {
			return fmt.Errorf("snapshot: key %q: %w", key, err)
		}

		if _, exists := newData.Get(key); exists {
			return fmt.Errorf("snapshot: duplicate key %q", key)
		}
		usedMemory += newData.Set(key, &RObj{
			objType:  objType,
			encoding: encoding,
			value:    value,
		})
		if expireAt != 0 {
			usedMemory += newExpires.Set(key, expireAt)
		}
	}

	if d.Remaining() != 0 {
		return errors.New("snapshot: trailing data after EOF marker")
	}

	s.data = newData
	s.expires = newExpires
	s.usedMemory = usedMemory
	s.evictionPool = s.evictionPool[:0]
	return nil
}

func decodeValue(objType ObjectType, encoding ObjectEncoding, data []byte) (any, error) {
	d := codec.NewDecoder(data)

	var value any
	var err error
	switch {
	case objType == ObjString && encoding == EncRaw:
		value = string(data)
		d.ReadRaw(len(data))
	case objType == ObjString && encoding == EncInt:
		value = d.ReadVarint()
		err = d.Err()
	case objType == ObjSet && encoding == EncIntSet:
		value, err = types.DecodeIntSet(d)
	case objType == ObjSet && encoding == EncHashTable:
		value, err = types.DecodeSimpleSet(d)
	case objType == ObjList && encoding == EncQuickList:
		value, err = types.DecodeQuickList(d)
	case objType == ObjHash && encoding == EncHashTable:
		value, err = types.DecodeHash(d)
	case objType == ObjZSet && encoding == EncSortedSet:
		value, err = types.DecodeZSet(d)
	case objType == ObjBloomFilter && encoding == EncBloomFilter:
		value, err = types.DecodeScalableBloomFilter(d)
	case objType == ObjCuckooFilter && encoding == EncCuckooFilter:
		value, err = types.DecodeCuckooFilter(d)
	case objType == ObjHyperLogLog && encoding == EncHyperLogLog:
		value, err = types.DecodeHyperLogLog(d)
	case objType == ObjCountMinSketch && encoding == EncCountMinSketch:
		value, err = types.DecodeCountMinSketch(d)
	default:
		return nil, fmt.Errorf("unsupported type %d / encoding %d", objType, encoding)
	}

	if err != nil {
		return nil, err
	}
	if d.Remaining() != 0 {
		return nil, fmt.Errorf("%d unexpected trailing bytes in value", d.Remaining())
	}
	return value, nil
}
