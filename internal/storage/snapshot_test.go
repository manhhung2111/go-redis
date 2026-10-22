package storage

import (
	"fmt"
	"hash/crc64"
	"testing"
	"time"

	"github.com/manhhung2111/go-redis/internal/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func populateAllTypes(t *testing.T, s Store) {
	t.Helper()

	s.Set("str", "hello")
	s.Set("int", "12345")
	s.SetEx("str:ttl", "temp", 1000)

	_, err := s.SAdd("set:int", "1", "2", "3")
	require.NoError(t, err)
	_, err = s.SAdd("set:str", "a", "b", "c")
	require.NoError(t, err)

	elems := make([]string, 2000)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%d", i)
	}
	_, err = s.RPush("list", elems...)
	require.NoError(t, err)

	_, err = s.HSet("hash", map[string]string{"f1": "v1", "f2": "v2"})
	require.NoError(t, err)

	_, err = s.ZAdd("zset", map[string]float64{"a": 1, "b": 2.5, "c": -1}, types.ZAddOptions{})
	require.NoError(t, err)
	_, err = s.GeoAdd("geo", []types.GeoPoint{
		{Longitude: 13.361389, Latitude: 38.115556, Member: "Palermo"},
	}, types.ZAddOptions{})
	require.NoError(t, err)

	_, err = s.BFMAdd("bf", []string{"x", "y", "z"})
	require.NoError(t, err)
	_, err = s.CFAdd("cf", "x")
	require.NoError(t, err)
	_, err = s.PFAdd("hll", []string{"a", "b", "c", "d"})
	require.NoError(t, err)
	require.NoError(t, s.CMSInitByDim("cms", 100, 4))
	_, err = s.CMSIncrBy("cms", map[string]uint64{"x": 7})
	require.NoError(t, err)
}

func TestSnapshotRoundTripAllTypes(t *testing.T) {
	src := newTestStore()
	populateAllTypes(t, src)

	data, err := src.Snapshot()
	require.NoError(t, err)

	dst := newTestStore()
	require.NoError(t, dst.LoadSnapshot(data))
	assert.Equal(t, src.KeyCount(), dst.KeyCount())

	str, _ := dst.Get("str")
	assert.Equal(t, "hello", *str)
	n, err := dst.IncrBy("int", 1)
	require.NoError(t, err)
	assert.Equal(t, int64(12346), *n)
	assert.InDelta(t, 1000, dst.TTL("str:ttl"), 2)
	assert.Equal(t, int64(-1), dst.TTL("str"))

	srcMembers, _ := src.SMembers("set:int")
	dstMembers, _ := dst.SMembers("set:int")
	assert.ElementsMatch(t, srcMembers, dstMembers)
	assert.Equal(t, EncIntSet, dst.(*store).mustGet(t, "set:int").encoding)
	srcMembers, _ = src.SMembers("set:str")
	dstMembers, _ = dst.SMembers("set:str")
	assert.ElementsMatch(t, srcMembers, dstMembers)

	srcList, _ := src.LRange("list", 0, -1)
	dstList, _ := dst.LRange("list", 0, -1)
	assert.Equal(t, srcList, dstList)

	srcHash, _ := src.HGetAll("hash")
	dstHash, _ := dst.HGetAll("hash")
	assert.ElementsMatch(t, srcHash, dstHash)

	srcZ, _ := src.ZRangeByRank("zset", 0, -1, true)
	dstZ, _ := dst.ZRangeByRank("zset", 0, -1, true)
	assert.Equal(t, srcZ, dstZ)

	srcPos, _ := src.GeoPos("geo", []string{"Palermo"})
	dstPos, _ := dst.GeoPos("geo", []string{"Palermo"})
	assert.Equal(t, srcPos, dstPos)

	srcBF, _ := src.BFMExists("bf", []string{"x", "y", "z", "nope"})
	dstBF, _ := dst.BFMExists("bf", []string{"x", "y", "z", "nope"})
	assert.Equal(t, srcBF, dstBF)

	cfCount, _ := dst.CFCount("cf", "x")
	assert.Equal(t, 1, cfCount)

	srcHLL, _ := src.PFCount([]string{"hll"})
	dstHLL, _ := dst.PFCount([]string{"hll"})
	assert.Equal(t, srcHLL, dstHLL)

	cms, _ := dst.CMSQuery("cms", []string{"x"})
	assert.Equal(t, []uint64{7}, cms)

	assert.Greater(t, dst.(*store).usedMemory, int64(0))
}

func (s *store) mustGet(t *testing.T, key string) *RObj {
	t.Helper()
	obj, ok := s.data.Get(key)
	require.True(t, ok, key)
	return obj
}

func TestSnapshotEmptyStore(t *testing.T) {
	data, err := newTestStore().Snapshot()
	require.NoError(t, err)

	dst := newTestStore()
	require.NoError(t, dst.LoadSnapshot(data))
	assert.Equal(t, 0, dst.KeyCount())
}

func TestSnapshotSkipsExpiredKeys(t *testing.T) {
	src := newTestStore()
	src.Set("alive", "1")
	src.Set("dead", "1")
	src.(*store).expires.Set("dead", uint64(time.Now().Add(-time.Second).UnixMilli()))

	data, err := src.Snapshot()
	require.NoError(t, err)

	dst := newTestStore()
	require.NoError(t, dst.LoadSnapshot(data))
	assert.Equal(t, 1, dst.KeyCount())
	assert.False(t, dst.Exists("dead"))
}

func TestLoadSnapshotDropsKeysExpiredSinceSave(t *testing.T) {
	src := newTestStore()
	src.Set("soon", "1")
	src.(*store).expires.Set("soon", uint64(time.Now().Add(50*time.Millisecond).UnixMilli()))

	data, err := src.Snapshot()
	require.NoError(t, err)
	time.Sleep(60 * time.Millisecond)

	dst := newTestStore()
	require.NoError(t, dst.LoadSnapshot(data))
	assert.Equal(t, 0, dst.KeyCount())
}

func TestLoadSnapshotRequiresEmptyStore(t *testing.T) {
	data, err := newTestStore().Snapshot()
	require.NoError(t, err)

	dst := newTestStore()
	dst.Set("k", "v")
	assert.ErrorIs(t, dst.LoadSnapshot(data), ErrSnapshotNotEmpty)
}

func TestLoadSnapshotRejectsCorruption(t *testing.T) {
	src := newTestStore()
	populateAllTypes(t, src)
	data, err := src.Snapshot()
	require.NoError(t, err)

	t.Run("truncated", func(t *testing.T) {
		for _, cut := range []int{0, 5, snapshotHeaderSize, len(data) / 2, len(data) - 1} {
			assert.Error(t, newTestStore().LoadSnapshot(data[:cut]), "cut=%d", cut)
		}
	})

	t.Run("flipped byte", func(t *testing.T) {
		for _, pos := range []int{snapshotHeaderSize + 3, len(data) / 2, len(data) - 10} {
			corrupt := append([]byte(nil), data...)
			corrupt[pos] ^= 0xFF
			dst := newTestStore()
			assert.ErrorContains(t, dst.LoadSnapshot(corrupt), "checksum", "pos=%d", pos)
			assert.Equal(t, 0, dst.KeyCount(), "failed load must leave store empty")
		}
	})

	t.Run("bad magic", func(t *testing.T) {
		corrupt := append([]byte(nil), data...)
		corrupt[0] = 'X'
		assert.ErrorContains(t, newTestStore().LoadSnapshot(corrupt), "magic")
	})

	t.Run("future version", func(t *testing.T) {
		e := newTestStore().(*store)
		empty, err := e.Snapshot()
		require.NoError(t, err)
		corrupt := append([]byte(nil), empty...)
		corrupt[len(snapshotMagic)] = 99
		resealSnapshot(corrupt)
		assert.ErrorContains(t, newTestStore().LoadSnapshot(corrupt), "unsupported version")
	})
}

// resealSnapshot recomputes the trailing checksum after a test edits the body.
func resealSnapshot(data []byte) {
	body := data[:len(data)-snapshotChecksumSize]
	crc := crc64.Checksum(body, snapshotCRCTable)
	for i := range 8 {
		data[len(body)+i] = byte(crc >> (8 * i))
	}
}

func BenchmarkSnapshot100kStrings(b *testing.B) {
	s := newTestStore()
	for i := range 100_000 {
		s.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value-%d-padding-padding", i))
	}
	b.ResetTimer()
	for range b.N {
		data, err := s.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(data)))
	}
}
