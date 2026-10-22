// Package codec provides the low-level binary primitives used by the snapshot
// format. All fixed-width integers are little-endian; lengths and counts are
// unsigned varints.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var ErrUnexpectedEOF = errors.New("codec: unexpected end of data")

// Encoder appends binary values to a growable byte slice.
type Encoder struct {
	buf []byte
}

func NewEncoder(capacity int) *Encoder {
	return &Encoder{buf: make([]byte, 0, capacity)}
}

func (e *Encoder) Bytes() []byte {
	return e.buf
}

func (e *Encoder) Len() int {
	return len(e.buf)
}

func (e *Encoder) WriteRaw(b []byte) {
	e.buf = append(e.buf, b...)
}

// WriteRawString appends s without a length prefix.
func (e *Encoder) WriteRawString(s string) {
	e.buf = append(e.buf, s...)
}

func (e *Encoder) WriteUint8(v uint8) {
	e.buf = append(e.buf, v)
}

func (e *Encoder) WriteUint16(v uint16) {
	e.buf = binary.LittleEndian.AppendUint16(e.buf, v)
}

func (e *Encoder) WriteUint64(v uint64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}

func (e *Encoder) WriteFloat64(v float64) {
	e.WriteUint64(math.Float64bits(v))
}

func (e *Encoder) WriteUvarint(v uint64) {
	e.buf = binary.AppendUvarint(e.buf, v)
}

func (e *Encoder) WriteVarint(v int64) {
	e.buf = binary.AppendVarint(e.buf, v)
}

func (e *Encoder) WriteString(s string) {
	e.WriteUvarint(uint64(len(s)))
	e.buf = append(e.buf, s...)
}

// ReserveUint64 writes an 8-byte placeholder and returns its offset so the
// value can be filled in later with PatchUint64.
func (e *Encoder) ReserveUint64() int {
	offset := len(e.buf)
	e.WriteUint64(0)
	return offset
}

func (e *Encoder) PatchUint64(offset int, v uint64) {
	binary.LittleEndian.PutUint64(e.buf[offset:], v)
}

// Decoder reads binary values from a byte slice. The first error is sticky:
// once a read fails every following read returns a zero value, so callers can
// decode a whole structure and check Err() once.
type Decoder struct {
	buf []byte
	pos int
	err error
}

func NewDecoder(b []byte) *Decoder {
	return &Decoder{buf: b}
}

func (d *Decoder) Err() error {
	return d.err
}

// Fail records err as the decoder error if none is set yet.
func (d *Decoder) Fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

func (d *Decoder) Remaining() int {
	return len(d.buf) - d.pos
}

func (d *Decoder) next(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || d.Remaining() < n {
		d.err = ErrUnexpectedEOF
		return nil
	}
	b := d.buf[d.pos : d.pos+n]
	d.pos += n
	return b
}

func (d *Decoder) ReadRaw(n int) []byte {
	return d.next(n)
}

func (d *Decoder) ReadUint8() uint8 {
	b := d.next(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (d *Decoder) ReadUint16() uint16 {
	b := d.next(2)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (d *Decoder) ReadUint64() uint64 {
	b := d.next(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *Decoder) ReadFloat64() float64 {
	return math.Float64frombits(d.ReadUint64())
}

func (d *Decoder) ReadUvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		d.err = fmt.Errorf("codec: invalid uvarint at offset %d", d.pos)
		return 0
	}
	d.pos += n
	return v
}

func (d *Decoder) ReadVarint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.buf[d.pos:])
	if n <= 0 {
		d.err = fmt.Errorf("codec: invalid varint at offset %d", d.pos)
		return 0
	}
	d.pos += n
	return v
}

func (d *Decoder) ReadString() string {
	n := d.ReadLen(1)
	return string(d.next(n))
}

// ReadLen reads a uvarint count of items that each occupy at least
// minItemSize bytes, rejecting counts the remaining data cannot hold. This
// keeps corrupt input from triggering huge allocations.
func (d *Decoder) ReadLen(minItemSize int) int {
	n := d.ReadUvarint()
	if d.err != nil {
		return 0
	}
	if minItemSize < 1 {
		minItemSize = 1
	}
	if n > uint64(d.Remaining()/minItemSize) {
		d.err = fmt.Errorf("codec: length %d exceeds remaining data", n)
		return 0
	}
	return int(n)
}
