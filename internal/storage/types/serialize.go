package types

import (
	"fmt"

	"github.com/manhhung2111/go-redis/internal/codec"
)

// Binary serialization of every value type for snapshots.
// Layouts are part of the on-disk format (see docs/design/snapshot.md §7):
// change them only together with a snapshot format version bump.

// Encodable is implemented by every value type stored in the keyspace.
type Encodable interface {
	Encode(e *codec.Encoder)
}

var (
	_ Encodable = (*intSet)(nil)
	_ Encodable = (*simpleSet)(nil)
	_ Encodable = (*simpleHash)(nil)
	_ Encodable = (*quickList)(nil)
	_ Encodable = (*zSet)(nil)
	_ Encodable = (*scalableBloomFilter)(nil)
	_ Encodable = (*scalableCuckooFilter)(nil)
	_ Encodable = (*hyperLogLog)(nil)
	_ Encodable = (*countMinSketch)(nil)
)

/* ----- Set (intset) ----- */

func (set *intSet) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(len(set.contents)))
	for _, v := range set.contents {
		e.WriteUint64(uint64(v))
	}
}

func DecodeIntSet(d *codec.Decoder) (Set, error) {
	n := d.ReadLen(8)
	contents := make([]int64, n, max(n, defaultMaxIntsetEntries))
	for i := range n {
		contents[i] = int64(d.ReadUint64())
		if i > 0 && contents[i] <= contents[i-1] {
			d.Fail(fmt.Errorf("intset: members not strictly ascending"))
		}
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return &intSet{contents: contents}, nil
}

/* ----- Set (hashtable) ----- */

func (s *simpleSet) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(len(s.contents)))
	for m := range s.contents {
		e.WriteString(m)
	}
}

func DecodeSimpleSet(d *codec.Decoder) (Set, error) {
	n := d.ReadLen(1)
	contents := make(map[string]struct{}, n)
	for range n {
		contents[d.ReadString()] = struct{}{}
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return &simpleSet{contents: contents}, nil
}

/* ----- Hash ----- */

func (s *simpleHash) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(len(s.contents)))
	for k, v := range s.contents {
		e.WriteString(k)
		e.WriteString(v)
	}
}

func DecodeHash(d *codec.Decoder) (Hash, error) {
	n := d.ReadLen(2)
	contents := make(map[string]string, n)
	for range n {
		k := d.ReadString()
		contents[k] = d.ReadString()
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return &simpleHash{contents: contents}, nil
}

/* ----- List ----- */

func (q *quickList) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(q.size))
	for node := q.head.next; node != q.tail; node = node.next {
		for _, elem := range node.listPack.data {
			e.WriteString(elem)
		}
	}
}

func DecodeQuickList(d *codec.Decoder) (QuickList, error) {
	n := d.ReadLen(1)
	elements := make([]string, n)
	for i := range n {
		elements[i] = d.ReadString()
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	// Rebuild through RPush so listpack node sizing applies as usual
	q := NewQuickList()
	q.RPush(elements)
	return q, nil
}

/* ----- Sorted set (also backs geo) ----- */

func (zset *zSet) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(zset.skipList.length))
	for node := zset.skipList.head.levels[0].forward; node != nil; node = node.levels[0].forward {
		e.WriteString(node.value)
		e.WriteFloat64(node.score)
	}
}

func DecodeZSet(d *codec.Decoder) (ZSet, error) {
	n := d.ReadLen(9)
	zset := &zSet{
		skipList: newSkipList(),
		data:     make(map[string]float64, n),
	}
	for range n {
		member := d.ReadString()
		score := d.ReadFloat64()
		if d.Err() != nil {
			break
		}
		if _, exists := zset.data[member]; exists {
			return nil, fmt.Errorf("zset: duplicate member %q", member)
		}
		zset.skipList.insert(member, score)
		zset.data[member] = score
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return zset, nil
}

/* ----- Bloom filter ----- */

func (sbf *scalableBloomFilter) Encode(e *codec.Encoder) {
	e.WriteUint64(sbf.initialCapacity)
	e.WriteFloat64(sbf.initialErrRate)
	e.WriteVarint(int64(sbf.expansionRate))
	e.WriteFloat64(sbf.tighteningRatio)
	e.WriteUint64(sbf.totalItems)
	e.WriteUvarint(uint64(len(sbf.filters)))
	for _, f := range sbf.filters {
		e.WriteUint64(f.k)
		e.WriteUint64(f.numBits)
		e.WriteUint64(f.capacity)
		e.WriteUint64(f.insertedItems)
		e.WriteFloat64(f.errorRate)
		e.WriteUvarint(uint64(len(f.bits)))
		for _, w := range f.bits {
			e.WriteUint64(w)
		}
	}
}

func DecodeScalableBloomFilter(d *codec.Decoder) (ScalableBloomFilter, error) {
	sbf := &scalableBloomFilter{
		initialCapacity: d.ReadUint64(),
		initialErrRate:  d.ReadFloat64(),
		expansionRate:   int(d.ReadVarint()),
		tighteningRatio: d.ReadFloat64(),
		totalItems:      d.ReadUint64(),
	}
	numFilters := d.ReadLen(41)
	sbf.filters = make([]*subFilter, 0, numFilters)
	for range numFilters {
		f := &subFilter{
			k:             d.ReadUint64(),
			numBits:       d.ReadUint64(),
			capacity:      d.ReadUint64(),
			insertedItems: d.ReadUint64(),
			errorRate:     d.ReadFloat64(),
		}
		numWords := d.ReadLen(8)
		if d.Err() != nil {
			break
		}
		if f.k == 0 || f.numBits == 0 || uint64(numWords) != (f.numBits+bitsPerWord-1)/bitsPerWord {
			return nil, fmt.Errorf("bloom filter: invalid sub-filter dimensions")
		}
		f.bits = make([]uint64, numWords)
		for i := range f.bits {
			f.bits[i] = d.ReadUint64()
		}
		sbf.filters = append(sbf.filters, f)
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	if len(sbf.filters) == 0 {
		return nil, fmt.Errorf("bloom filter: no sub-filters")
	}
	return sbf, nil
}

/* ----- Cuckoo filter ----- */

func (scf *scalableCuckooFilter) Encode(e *codec.Encoder) {
	e.WriteUint64(scf.initialCapacity)
	e.WriteUint64(scf.initialBucketSize)
	e.WriteUint64(scf.maxIterations)
	e.WriteVarint(int64(scf.expansionRate))
	e.WriteUint64(scf.totalItems)
	e.WriteUint64(scf.totalDeletes)
	e.WriteUvarint(uint64(len(scf.filters)))
	for _, f := range scf.filters {
		e.WriteUint64(f.bucketSize)
		e.WriteUint64(f.numBuckets)
		e.WriteUint64(f.fingerprintSize)
		e.WriteUint64(f.maxKicks)
		e.WriteUint64(f.insertedItems)
		e.WriteUint64(f.deletedItems)
		for _, b := range f.buckets {
			for _, fp := range b.fingerprints {
				e.WriteUint16(fp)
			}
		}
	}
}

func DecodeCuckooFilter(d *codec.Decoder) (CuckooFilter, error) {
	scf := &scalableCuckooFilter{
		initialCapacity:   d.ReadUint64(),
		initialBucketSize: d.ReadUint64(),
		maxIterations:     d.ReadUint64(),
		expansionRate:     int(d.ReadVarint()),
		totalItems:        d.ReadUint64(),
		totalDeletes:      d.ReadUint64(),
	}
	numFilters := d.ReadLen(48)
	scf.filters = make([]*subCuckooFilter, 0, numFilters)
	for range numFilters {
		f := &subCuckooFilter{
			bucketSize:      d.ReadUint64(),
			numBuckets:      d.ReadUint64(),
			fingerprintSize: d.ReadUint64(),
			maxKicks:        d.ReadUint64(),
			insertedItems:   d.ReadUint64(),
			deletedItems:    d.ReadUint64(),
		}
		if d.Err() != nil {
			break
		}
		remainingSlots := uint64(d.Remaining()) / 2
		if f.bucketSize == 0 || f.numBuckets == 0 ||
			f.bucketSize > remainingSlots || f.numBuckets > remainingSlots/f.bucketSize {
			return nil, fmt.Errorf("cuckoo filter: invalid sub-filter dimensions")
		}
		f.buckets = make([]bucket, f.numBuckets)
		for i := range f.buckets {
			fps := make([]uint16, f.bucketSize)
			for j := range fps {
				fps[j] = d.ReadUint16()
			}
			f.buckets[i] = bucket{fingerprints: fps}
		}
		scf.filters = append(scf.filters, f)
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	if len(scf.filters) == 0 {
		return nil, fmt.Errorf("cuckoo filter: no sub-filters")
	}
	return scf, nil
}

/* ----- HyperLogLog ----- */

func (h *hyperLogLog) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(len(h.registers)))
	e.WriteRaw(h.registers)
}

func DecodeHyperLogLog(d *codec.Decoder) (HyperLogLog, error) {
	n := d.ReadLen(1)
	raw := d.ReadRaw(n)
	if err := d.Err(); err != nil {
		return nil, err
	}
	if n != hllM {
		return nil, fmt.Errorf("hyperloglog: expected %d registers, got %d", hllM, n)
	}
	registers := make([]uint8, hllM)
	copy(registers, raw)
	return &hyperLogLog{registers: registers, dirty: true}, nil
}

/* ----- Count-min sketch ----- */

func (cms *countMinSketch) Encode(e *codec.Encoder) {
	e.WriteUvarint(uint64(len(cms.grid[0])))
	e.WriteUvarint(uint64(len(cms.grid)))
	e.WriteUint64(cms.totalCount)
	for _, row := range cms.grid {
		for _, c := range row {
			e.WriteUint64(c)
		}
	}
}

func DecodeCountMinSketch(d *codec.Decoder) (CountMinSketch, error) {
	width := d.ReadUvarint()
	depth := d.ReadUvarint()
	totalCount := d.ReadUint64()
	if err := d.Err(); err != nil {
		return nil, err
	}
	if width == 0 || depth == 0 || width > uint64(d.Remaining())/8/depth {
		return nil, fmt.Errorf("count-min sketch: invalid dimensions %dx%d", width, depth)
	}
	grid := make([][]uint64, depth)
	for i := range grid {
		grid[i] = make([]uint64, width)
		for j := range grid[i] {
			grid[i][j] = d.ReadUint64()
		}
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return &countMinSketch{grid: grid, totalCount: totalCount}, nil
}
