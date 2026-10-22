package types

import (
	"fmt"
	"math"
	"testing"

	"github.com/manhhung2111/go-redis/internal/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encode(v Encodable) []byte {
	e := codec.NewEncoder(64)
	v.Encode(e)
	return e.Bytes()
}

// roundTrip encodes v, decodes it with decodeFn and checks the decoder
// consumed exactly the encoded bytes.
func roundTrip[T any](t *testing.T, v Encodable, decodeFn func(*codec.Decoder) (T, error)) T {
	t.Helper()
	d := codec.NewDecoder(encode(v))
	out, err := decodeFn(d)
	require.NoError(t, err)
	require.Equal(t, 0, d.Remaining(), "decoder must consume all bytes")
	return out
}

func TestSerializeIntSet(t *testing.T) {
	for _, n := range []int{0, 1, 100, defaultMaxIntsetEntries} {
		set := NewIntSet()
		members := make([]string, n)
		for i := range members {
			members[i] = fmt.Sprint(i*7 - 300)
		}
		set.Add(members...)

		out := roundTrip(t, set.(Encodable), DecodeIntSet)
		assert.Equal(t, set.Members(), out.Members())

		// Decoded intset must keep normal growth/upgrade behaviour
		_, ok, _ := out.Add("not-an-int")
		assert.False(t, ok)
	}
}

func TestSerializeIntSetRejectsUnsorted(t *testing.T) {
	e := codec.NewEncoder(32)
	e.WriteUvarint(2)
	e.WriteUint64(5)
	e.WriteUint64(3)
	_, err := DecodeIntSet(codec.NewDecoder(e.Bytes()))
	assert.Error(t, err)
}

func TestSerializeSimpleSet(t *testing.T) {
	set := NewSimpleSet()
	set.Add("a", "", "hello world", "🙂")
	out := roundTrip(t, set.(Encodable), DecodeSimpleSet)
	assert.ElementsMatch(t, set.Members(), out.Members())

	empty := roundTrip(t, NewSimpleSet().(Encodable), DecodeSimpleSet)
	assert.Equal(t, int64(0), empty.Size())
}

func TestSerializeHash(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"f1": "v1", "f2": "", "": "empty-field", "n": "42"})
	out := roundTrip(t, h.(Encodable), DecodeHash)
	assert.ElementsMatch(t, h.GetAll(), out.GetAll())

	v, _, err := out.IncBy("n", 8)
	require.NoError(t, err)
	assert.Equal(t, int64(50), v)
}

func TestSerializeQuickList(t *testing.T) {
	q := NewQuickList()
	// Large enough to span many listpack nodes
	elems := make([]string, 5000)
	for i := range elems {
		elems[i] = fmt.Sprintf("element-%d", i)
	}
	q.RPush(elems[2500:])
	q.LPush(elems[:2500])

	out := roundTrip(t, q.(Encodable), DecodeQuickList)
	assert.Equal(t, q.Size(), out.Size())
	assert.Equal(t, q.LRange(0, -1), out.LRange(0, -1))

	empty := roundTrip(t, NewQuickList().(Encodable), DecodeQuickList)
	assert.Equal(t, uint32(0), empty.Size())
}

func TestSerializeZSet(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1, "b": 1, "c": -2.5, "d": math.Inf(1), "e": math.Inf(-1), "f": 0,
	}, ZAddOptions{})

	out := roundTrip(t, z.(Encodable), DecodeZSet)
	assert.Equal(t, z.ZCard(), out.ZCard())
	assert.Equal(t, z.ZRangeByRank(0, -1, true), out.ZRangeByRank(0, -1, true))
	assert.Equal(t, z.ZRank("b", true), out.ZRank("b", true))
}

func TestSerializeZSetGeo(t *testing.T) {
	z := NewZSet()
	z.GeoAdd([]GeoPoint{
		{Longitude: 13.361389, Latitude: 38.115556, Member: "Palermo"},
		{Longitude: 15.087269, Latitude: 37.502669, Member: "Catania"},
	}, ZAddOptions{})

	out := roundTrip(t, z.(Encodable), DecodeZSet)
	assert.Equal(t, z.GeoPos([]string{"Palermo", "Catania"}), out.GeoPos([]string{"Palermo", "Catania"}))
	assert.Equal(t, z.GeoDist("Palermo", "Catania", "km"), out.GeoDist("Palermo", "Catania", "km"))
}

func TestSerializeZSetRejectsDuplicate(t *testing.T) {
	e := codec.NewEncoder(32)
	e.WriteUvarint(2)
	e.WriteString("m")
	e.WriteFloat64(1)
	e.WriteString("m")
	e.WriteFloat64(2)
	_, err := DecodeZSet(codec.NewDecoder(e.Bytes()))
	assert.Error(t, err)
}

func TestSerializeBloomFilter(t *testing.T) {
	bf := NewScalableBloomFilter(0.01, 50, 2)
	items := make([]string, 400) // forces several sub-filters
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	bf.MAdd(items)
	require.Greater(t, len(bf.(*scalableBloomFilter).filters), 1)

	out := roundTrip(t, bf.(Encodable), DecodeScalableBloomFilter)
	assert.Equal(t, bf, out)
	assert.Equal(t, bf.Info(BloomFilterInfoAll), out.Info(BloomFilterInfoAll))

	probes := append(items, "missing-1", "missing-2", "missing-3")
	assert.Equal(t, bf.MExists(probes), out.MExists(probes))
}

func TestSerializeCuckooFilter(t *testing.T) {
	cf := NewCuckooFilter(64, 2, 50, 2)
	for i := range 300 { // forces expansion
		cf.Add(fmt.Sprintf("item-%d", i))
	}
	for i := range 20 {
		cf.Del(fmt.Sprintf("item-%d", i))
	}
	require.Greater(t, len(cf.(*scalableCuckooFilter).filters), 1)

	out := roundTrip(t, cf.(Encodable), DecodeCuckooFilter)
	assert.Equal(t, cf, out)
	assert.Equal(t, cf.Info(), out.Info())
	for i := range 320 {
		item := fmt.Sprintf("item-%d", i)
		assert.Equal(t, cf.Count(item), out.Count(item), item)
	}
}

func TestSerializeHyperLogLog(t *testing.T) {
	h := NewHyperLogLog()
	items := make([]string, 10000)
	for i := range items {
		items[i] = fmt.Sprint(i)
	}
	h.PFAdd(items)
	want := h.PFCount(nil)

	out := roundTrip(t, h.(Encodable), DecodeHyperLogLog)
	assert.Equal(t, want, out.PFCount(nil))
	assert.Equal(t, h.(*hyperLogLog).registers, out.(*hyperLogLog).registers)
}

func TestSerializeHyperLogLogRejectsWrongSize(t *testing.T) {
	e := codec.NewEncoder(16)
	e.WriteUvarint(3)
	e.WriteRaw([]byte{1, 2, 3})
	_, err := DecodeHyperLogLog(codec.NewDecoder(e.Bytes()))
	assert.Error(t, err)
}

func TestSerializeCountMinSketch(t *testing.T) {
	cms := NewCountMinSketchByDim(200, 5)
	cms.IncrBy(map[string]uint64{"a": 3, "b": 10, "c": 1 << 40})

	out := roundTrip(t, cms.(Encodable), DecodeCountMinSketch)
	assert.Equal(t, cms, out)
	assert.Equal(t, cms.Info(), out.Info())
	assert.Equal(t, cms.Query([]string{"a", "b", "c", "zz"}), out.Query([]string{"a", "b", "c", "zz"}))
}

// Truncated input must produce an error, never a panic.
func TestSerializeTruncated(t *testing.T) {
	bf := NewScalableBloomFilter(0.01, 100, 2)
	bf.Add("x")
	cf := NewCuckooFilter(100, 4, 20, 1)
	cf.Add("x")
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1, "b": 2}, ZAddOptions{})
	q := NewQuickList()
	q.RPush([]string{"a", "b"})
	h := NewHash()
	h.Set(map[string]string{"a": "b"})
	ss := NewSimpleSet()
	ss.Add("a")
	is := NewIntSet()
	is.Add("1", "2")

	cases := []struct {
		name   string
		v      Encodable
		decode func(*codec.Decoder) error
	}{
		{"intset", is.(Encodable), func(d *codec.Decoder) error { _, err := DecodeIntSet(d); return err }},
		{"simpleset", ss.(Encodable), func(d *codec.Decoder) error { _, err := DecodeSimpleSet(d); return err }},
		{"hash", h.(Encodable), func(d *codec.Decoder) error { _, err := DecodeHash(d); return err }},
		{"quicklist", q.(Encodable), func(d *codec.Decoder) error { _, err := DecodeQuickList(d); return err }},
		{"zset", z.(Encodable), func(d *codec.Decoder) error { _, err := DecodeZSet(d); return err }},
		{"bloom", bf.(Encodable), func(d *codec.Decoder) error { _, err := DecodeScalableBloomFilter(d); return err }},
		{"cuckoo", cf.(Encodable), func(d *codec.Decoder) error { _, err := DecodeCuckooFilter(d); return err }},
		{"hll", NewHyperLogLog().(Encodable), func(d *codec.Decoder) error { _, err := DecodeHyperLogLog(d); return err }},
		{"cms", NewCountMinSketchByDim(4, 2).(Encodable), func(d *codec.Decoder) error { _, err := DecodeCountMinSketch(d); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := encode(tc.v)
			for cut := 0; cut < len(data); cut++ {
				assert.Error(t, tc.decode(codec.NewDecoder(data[:cut])), "cut=%d", cut)
			}
		})
	}
}
