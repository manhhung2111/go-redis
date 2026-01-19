package types

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZSet_ZAdd_BasicInsert(t *testing.T) {
	z := NewZSet()

	res, _ := z.ZAdd(map[string]float64{
		"one": 1,
		"two": 2,
	}, ZAddOptions{})

	require.NotNil(t, res)
	assert.Equal(t, uint32(2), *res)
	assert.Equal(t, uint32(2), z.ZCard())
}

func TestZSet_ZAdd_NX(t *testing.T) {
	z := NewZSet().(*zSet)

	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})

	res, _ := z.ZAdd(map[string]float64{
		"a": 2, // should be ignored
		"b": 3,
	}, ZAddOptions{NX: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, uint32(2), z.ZCard())
	assert.Equal(t, float64(1), z.data["a"])
}

func TestZSet_ZAdd_XX(t *testing.T) {
	z := NewZSet().(*zSet)

	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})

	// update without CH → result must be 0
	res, _ := z.ZAdd(map[string]float64{
		"a": 2,
	}, ZAddOptions{XX: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, uint32(1), z.ZCard())
	assert.Equal(t, float64(2), z.data["a"])

	// update with CH → result increments
	res, _ = z.ZAdd(map[string]float64{
		"a": 3,
	}, ZAddOptions{XX: true, CH: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(3), z.data["a"])
}

func TestZSet_ZAdd_GT_LT(t *testing.T) {
	z := NewZSet().(*zSet)
	z.ZAdd(map[string]float64{"a": 5}, ZAddOptions{})

	// GT reject
	res, _ := z.ZAdd(map[string]float64{"a": 3}, ZAddOptions{GT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, float64(5), z.data["a"])

	// GT accept, no CH → 0
	res, _ = z.ZAdd(map[string]float64{"a": 7}, ZAddOptions{GT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, float64(7), z.data["a"])

	// GT accept with CH → 1
	res, _ = z.ZAdd(map[string]float64{"a": 9}, ZAddOptions{GT: true, CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(9), z.data["a"])

	// LT reject
	res, _ = z.ZAdd(map[string]float64{"a": 10}, ZAddOptions{LT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)

	// LT accept with CH
	res, _ = z.ZAdd(map[string]float64{"a": 8}, ZAddOptions{LT: true, CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(8), z.data["a"])
}

func TestZSet_ZAdd_CH(t *testing.T) {
	z := NewZSet()

	res, _ := z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)

	// same score → no change
	res, _ = z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)

	// score change
	res, _ = z.ZAdd(map[string]float64{"a": 2}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
}

func TestZSet_ZAdd_InvalidOptions(t *testing.T) {
	z := NewZSet()

	r1, _ := z.ZAdd(
		map[string]float64{"a": 1},
		ZAddOptions{NX: true, XX: true},
	)
	assert.Nil(t, r1)

	r2, _ := z.ZAdd(
		map[string]float64{"a": 1},
		ZAddOptions{GT: true, LT: true},
	)
	assert.Nil(t, r2)

	r3, _ := z.ZAdd(
		map[string]float64{"a": 1},
		ZAddOptions{NX: true, GT: true},
	)
	assert.Nil(t, r3)
}

func TestZSet_ZCard_AfterUpdates(t *testing.T) {
	z := NewZSet()

	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, ZAddOptions{})

	assert.Equal(t, uint32(3), z.ZCard())

	z.ZAdd(map[string]float64{"b": 5}, ZAddOptions{})
	assert.Equal(t, uint32(3), z.ZCard())

	z.ZAdd(map[string]float64{"d": 4}, ZAddOptions{})
	assert.Equal(t, uint32(4), z.ZCard())
}

func TestZSet_ZCount(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)
	z.ZIncrBy("c", 3)
	z.ZIncrBy("d", 3)
	z.ZIncrBy("e", 5)

	tests := []struct {
		min, max float64
		expected uint32
	}{
		{0, 10, 5},
		{1, 1, 1},
		{3, 3, 2},
		{2, 4, 3},
		{6, 10, 0},
		{4, 2, 0},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, z.ZCount(tt.min, tt.max))
	}
}

func TestZSet_ZCount_Empty(t *testing.T) {
	z := NewZSet()
	assert.Equal(t, uint32(0), z.ZCount(-100, 100))
}

func TestZSet_ZCount_Boundaries(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1.0)
	z.ZIncrBy("b", 2.0)
	z.ZIncrBy("c", 2.0)
	z.ZIncrBy("d", 3.0)

	assert.Equal(t, uint32(2), z.ZCount(2.0, 2.0))
	assert.Equal(t, uint32(1), z.ZCount(1.0, 1.0))
	assert.Equal(t, uint32(4), z.ZCount(1.0, 3.0))
}

func TestZSet_ZCount_NegativeScores(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", -5)
	z.ZIncrBy("b", -1)
	z.ZIncrBy("c", 0)

	assert.Equal(t, uint32(2), z.ZCount(-5, -1))
	assert.Equal(t, uint32(3), z.ZCount(-10, 0))
}

func TestZSet_ZIncrBy(t *testing.T) {
	z := NewZSet()

	score, ok, _ := z.ZIncrBy("a", 1.5)
	require.True(t, ok)
	assert.Equal(t, 1.5, score)

	score, ok, _ = z.ZIncrBy("a", 2.5)
	require.True(t, ok)
	assert.Equal(t, 4.0, score)

	// negative increment
	score, ok, _ = z.ZIncrBy("a", -1.0)
	require.True(t, ok)
	assert.Equal(t, 3.0, score)
}

func TestZSet_ZIncrBy_Invalid(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", math.MaxFloat64)

	_, ok, _ := z.ZIncrBy("a", math.MaxFloat64)
	assert.False(t, ok)

	_, ok, _ = z.ZIncrBy("a", math.NaN())
	assert.False(t, ok)
}

func TestZSet_ZIncrBy_ZeroIncrement(t *testing.T) {
	z := NewZSet()

	score, ok, _ := z.ZIncrBy("a", 0)
	require.True(t, ok)
	assert.Equal(t, 0.0, score)

	score, ok, _ = z.ZIncrBy("a", 0)
	require.True(t, ok)
	assert.Equal(t, 0.0, score)
}

func TestZSet_ZIncrBy_RepeatedUpdates(t *testing.T) {
	z := NewZSet()

	for i := 0; i < 10; i++ {
		score, ok, _ := z.ZIncrBy("counter", 1)
		require.True(t, ok)
		assert.Equal(t, float64(i+1), score)
	}

	assert.Equal(t, uint32(1), z.ZCount(10, 10))
}

func TestZSet_ZIncrBy_DecreaseBelowZero(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 5)
	score, ok, _ := z.ZIncrBy("a", -10)

	require.True(t, ok)
	assert.Equal(t, -5.0, score)
}

func TestZSet_ZLexCount(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("apple", 0)
	z.ZIncrBy("banana", 0)
	z.ZIncrBy("cherry", 0)
	z.ZIncrBy("date", 0)

	assert.Equal(t, uint32(4), z.ZLexCount("apple", "date"))
	assert.Equal(t, uint32(2), z.ZLexCount("banana", "cherry"))
	assert.Equal(t, uint32(0), z.ZLexCount("x", "z"))
}

func TestZSet_ZLexCount_Empty(t *testing.T) {
	z := NewZSet()
	assert.Equal(t, uint32(0), z.ZLexCount("a", "z"))
}

func TestZSet_ZLexCount_ExactMiss(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("apple", 0)
	z.ZIncrBy("banana", 0)

	assert.Equal(t, uint32(0), z.ZLexCount("apricot", "apricot"))
}

func TestZSet_ZLexCount_PrefixOverlap(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 0)
	z.ZIncrBy("aa", 0)
	z.ZIncrBy("aaa", 0)
	z.ZIncrBy("b", 0)

	assert.Equal(t, uint32(3), z.ZLexCount("a", "aaa"))
}

func TestZSet_ZMScore(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)

	scores := z.ZMScore([]string{"a", "x", "b"})

	require.Len(t, scores, 3)

	require.NotNil(t, scores[0])
	assert.Equal(t, 1.0, *scores[0])

	assert.Nil(t, scores[1])

	require.NotNil(t, scores[2])
	assert.Equal(t, 2.0, *scores[2])
}

func TestZSet_ZMScore_EmptyInput(t *testing.T) {
	z := NewZSet()
	res := z.ZMScore([]string{})
	assert.Empty(t, res)
}

func TestZSet_ZMScore_AllMissing(t *testing.T) {
	z := NewZSet()

	res := z.ZMScore([]string{"a", "b"})
	assert.Len(t, res, 2)
	assert.Nil(t, res[0])
	assert.Nil(t, res[1])
}

func TestZSet_ZMScore_Duplicates(t *testing.T) {
	z := NewZSet()
	z.ZIncrBy("a", 1)

	res := z.ZMScore([]string{"a", "a"})
	require.Len(t, res, 2)

	assert.NotNil(t, res[0])
	assert.NotNil(t, res[1])
	assert.Equal(t, 1.0, *res[0])
	assert.Equal(t, 1.0, *res[1])
}

func TestZSet_ZPopMax(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)
	z.ZIncrBy("c", 3)

	res, _ := z.ZPopMax(2)

	require.Equal(t, []string{"c", "3", "b", "2"}, res)
	assert.Equal(t, uint32(1), z.ZCount(-100, 100))
}

func TestZSet_ZPopMin(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)
	z.ZIncrBy("c", 3)

	res, _ := z.ZPopMin(2)

	require.Equal(t, []string{"a", "1", "b", "2"}, res)
	assert.Equal(t, uint32(1), z.ZCount(-100, 100))
}

func TestZSet_ZPop_OverCount(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)

	r1, _ := z.ZPopMax(10)
	assert.Len(t, r1, 4)
	r2, _ := z.ZPopMin(10)
	assert.Len(t, r2, 0)
}

func TestZSet_ZPop_CountZero(t *testing.T) {
	z := NewZSet()
	z.ZIncrBy("a", 1)

	r1, _ := z.ZPopMax(0)
	assert.Empty(t, r1)
	r2, _ := z.ZPopMin(0)
	assert.Empty(t, r2)
}

func TestZSet_ZPop_ExactSize(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)

	res, _ := z.ZPopMin(2)
	assert.Equal(t, []string{"a", "1", "b", "2"}, res)

	assert.Equal(t, uint32(0), z.ZCount(-100, 100))
}

func TestZSet_ZPop_OrderAfterPartialPop(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 1)
	z.ZIncrBy("b", 2)
	z.ZIncrBy("c", 3)
	z.ZIncrBy("d", 4)

	z.ZPopMax(1) // removes d
	z.ZPopMin(1) // removes a

	assert.Equal(t, uint32(2), z.ZCount(-100, 100))
	assert.Equal(t, uint32(1), z.ZCount(2, 2))
	assert.Equal(t, uint32(1), z.ZCount(3, 3))
}

func TestZSet_ZPop_ReducesCountCorrectly(t *testing.T) {
	z := NewZSet()

	for i := 0; i < 10; i++ {
		z.ZIncrBy(fmt.Sprintf("k%d", i), float64(i))
	}

	z.ZPopMin(3)
	assert.Equal(t, uint32(7), z.ZCount(-100, 100))

	z.ZPopMax(4)
	assert.Equal(t, uint32(3), z.ZCount(-100, 100))
}

func TestZRandMember_ZeroAndEmpty(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1, "b": 2}, ZAddOptions{})

	assert.Empty(t, z.ZRandMember(0, false))
	assert.Empty(t, NewZSet().ZRandMember(5, false))
}

func TestZRandMember_PositiveCount(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string]float64
		count      int
		withScores bool
		wantLen    int
	}{
		{"lt size no score", map[string]float64{"a": 1, "b": 2, "c": 3}, 2, false, 2},
		{"eq size no score", map[string]float64{"a": 1, "b": 2}, 2, false, 2},
		{"gt size no score", map[string]float64{"a": 1, "b": 2}, 10, false, 2},
		{"lt size with score", map[string]float64{"a": 1, "b": 2, "c": 3}, 2, true, 4},
		{"gt size with score", map[string]float64{"a": 1, "b": 2}, 10, true, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := NewZSet().(*zSet)
			z.ZAdd(tt.data, ZAddOptions{})
			res := z.ZRandMember(tt.count, tt.withScores)

			require.Len(t, res, tt.wantLen)

			if tt.withScores {
				assertMemberScorePairs(t, z, res)
			} else {
				assertMembersExist(t, z, res)
				assert.Len(t, unique(res), len(res))
			}
		})
	}
}

func TestZRandMember_NegativeCount(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string]float64
		count      int
		withScores bool
		wantLen    int
	}{
		{"dup no score", map[string]float64{"a": 1, "b": 2}, -5, false, 5},
		{"dup with score", map[string]float64{"a": 1}, -3, true, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := NewZSet().(*zSet)
			z.ZAdd(tt.data, ZAddOptions{})
			res := z.ZRandMember(tt.count, tt.withScores)

			require.Len(t, res, tt.wantLen)

			if tt.withScores {
				assertMemberScorePairs(t, z, res)
			} else {
				assertMembersExist(t, z, res)
			}
		})
	}
}

func TestZRandMember_ScoreFormatting(t *testing.T) {
	z := NewZSet().(*zSet)
	z.ZAdd(map[string]float64{"a": 1.0, "b": -2.5, "c": 3.14159}, ZAddOptions{})

	res := z.ZRandMember(3, true)
	assertMemberScorePairs(t, z, res)
}

func assertMembersExist(t *testing.T, z *zSet, members []string) {
	for _, m := range members {
		_, ok := z.data[m]
		assert.True(t, ok, "member %s should exist", m)
	}
}

func TestZRandMember_Uniqueness(t *testing.T) {
	z := NewZSet()
	for i := 0; i < 100; i++ {
		z.ZAdd(map[string]float64{strconv.Itoa(i): float64(i)}, ZAddOptions{})
	}

	res := z.ZRandMember(20, false)
	assert.Len(t, unique(res), 20)

	res2 := z.ZRandMember(-100, false)
	assert.Len(t, res2, 100)
}

func assertMemberScorePairs(t *testing.T, z *zSet, result []string) {
	require.True(t, len(result)%2 == 0)
	for i := 0; i < len(result); i += 2 {
		score, err := strconv.ParseFloat(result[i+1], 64)
		require.NoError(t, err)
		assert.Equal(t, z.data[result[i]], score)
	}
}

func unique(arr []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, v := range arr {
		m[v] = struct{}{}
	}
	return m
}

func TestZSet_ZRangeByRank_Basic(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
	}, ZAddOptions{})

	assert.Equal(t,
		[]string{"a", "b", "c"},
		z.ZRangeByRank(0, 2, false),
	)

	assert.Equal(t,
		[]string{"b", "2", "c", "3"},
		z.ZRangeByRank(1, 2, true),
	)
}

func TestZSet_ZRangeByRank_NegativeIndices(t *testing.T) {
	z := NewZSet()
	for i := 0; i < 5; i++ {
		z.ZIncrBy(string(rune('a'+i)), float64(i))
	}

	assert.Equal(t,
		[]string{"d", "e"},
		z.ZRangeByRank(-2, -1, false),
	)
}

func TestZSet_ZRangeByRank_OutOfBounds(t *testing.T) {
	z := NewZSet()
	z.ZIncrBy("a", 1)

	assert.Empty(t, z.ZRangeByRank(10, 20, false))
	assert.Empty(t, z.ZRangeByRank(2, 1, false))
	assert.Empty(t, z.ZRangeByRank(-100, -200, false))
}

func TestZSet_ZRevRangeByRank_Basic(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
	}, ZAddOptions{})

	assert.Equal(t,
		[]string{"d", "c"},
		z.ZRevRangeByRank(0, 1, false),
	)

	assert.Equal(t,
		[]string{"c", "3", "b", "2"},
		z.ZRevRangeByRank(1, 2, true),
	)
}

func TestZSet_ZRevRangeByRank_Negative(t *testing.T) {
	z := NewZSet()
	for i := 0; i < 4; i++ {
		z.ZIncrBy(string(rune('a'+i)), float64(i))
	}

	assert.Equal(t,
		[]string{"c", "b"},
		z.ZRevRangeByRank(-3, -2, false),
	)
}

func TestZSet_ZRangeByScore(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
	}, ZAddOptions{})

	assert.Equal(t,
		[]string{"b", "c"},
		z.ZRangeByScore(2, 3, false),
	)

	assert.Equal(t,
		[]string{"c", "3"},
		z.ZRangeByScore(3, 3, true),
	)
}

func TestZSet_ZRangeByScore_Empty(t *testing.T) {
	z := NewZSet()
	assert.Empty(t, z.ZRangeByScore(0, 100, false))
}

func TestZSet_ZRevRangeByScore(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
	}, ZAddOptions{})

	assert.Equal(t,
		[]string{"d", "c"},
		z.ZRevRangeByScore(4, 3, false),
	)

	assert.Equal(t,
		[]string{"b", "2", "a", "1"},
		z.ZRevRangeByScore(2, 1, true),
	)
}

func TestZSet_ZRangeByLex(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("apple", 0)
	z.ZIncrBy("banana", 0)
	z.ZIncrBy("cherry", 0)
	z.ZIncrBy("date", 0)

	assert.Equal(t,
		[]string{"banana", "cherry"},
		z.ZRangeByLex("banana", "cherry", false),
	)

	assert.Equal(t,
		[]string{"apple", "0", "banana", "0"},
		z.ZRangeByLex("apple", "banana", true),
	)
}

func TestZSet_ZRangeByLex_Empty(t *testing.T) {
	z := NewZSet()
	assert.Empty(t, z.ZRangeByLex("a", "z", false))
}

func TestZSet_ZRevRangeByLex(t *testing.T) {
	z := NewZSet()

	z.ZIncrBy("a", 0)
	z.ZIncrBy("b", 0)
	z.ZIncrBy("c", 0)
	z.ZIncrBy("d", 0)

	assert.Equal(t,
		[]string{"d", "c"},
		z.ZRevRangeByLex("d", "c", false),
	)

	assert.Equal(t,
		[]string{"b", "0", "a", "0"},
		z.ZRevRangeByLex("b", "a", true),
	)
}

func TestZSet_ZRank_Basic(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, ZAddOptions{})

	res := z.ZRank("a", false)
	require.NotNil(t, res)
	assert.Equal(t, []any{0}, res)

	res = z.ZRank("c", false)
	assert.Equal(t, []any{2}, res)
}

func TestZSet_ZRank_WithScore(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"b": 2,
		"a": 1,
	}, ZAddOptions{})

	res := z.ZRank("b", true)
	require.NotNil(t, res)

	assert.Len(t, res, 2)
	assert.Equal(t, 1, res[0])
	assert.Equal(t, "2", res[1])
}

func TestZSet_ZRank_NotFound(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})

	assert.Nil(t, z.ZRank("missing", false))
}

func TestZSet_ZRevRank_Basic(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, ZAddOptions{})

	res := z.ZRevRank("c", false)
	require.NotNil(t, res)
	assert.Equal(t, []any{0}, res) // highest score

	res = z.ZRevRank("a", false)
	assert.Equal(t, []any{2}, res) // lowest score
}

func TestZSet_ZRevRank_WithScore(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, ZAddOptions{})

	res := z.ZRevRank("b", true)
	require.NotNil(t, res)

	assert.Len(t, res, 2)
	assert.Equal(t, 1, res[0])
	assert.Equal(t, "2", res[1])
}

func TestZSet_ZRevRank_NotFound(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})

	assert.Nil(t, z.ZRevRank("x", false))
}

func TestZSet_ZRem_Single(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
	}, ZAddOptions{})

	removed, _ := z.ZRem([]string{"a"})
	assert.Equal(t, 1, removed)

	assert.Nil(t, z.ZRank("a", false))
	assert.Equal(t, uint32(1), z.ZCard())
}

func TestZSet_ZRem_Multiple(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, ZAddOptions{})

	removed, _ := z.ZRem([]string{"a", "c", "x"})
	assert.Equal(t, 2, removed)

	assert.Nil(t, z.ZRank("a", false))
	assert.Nil(t, z.ZRank("c", false))
	assert.NotNil(t, z.ZRank("b", false))
}

func TestZSet_ZRem_Idempotent(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})

	r1, _ := z.ZRem([]string{"a"})
	assert.Equal(t, 1, r1)
	r2, _ := z.ZRem([]string{"a"})
	assert.Equal(t, 0, r2)
}

func TestZSet_ZScore_Basic(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1.5}, ZAddOptions{})

	score := z.ZScore("a")
	require.NotNil(t, score)
	assert.Equal(t, 1.5, *score)
}

func TestZSet_ZScore_NotFound(t *testing.T) {
	z := NewZSet()
	assert.Nil(t, z.ZScore("missing"))
}

func TestZSet_ZScore_AfterUpdate(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})
	z.ZAdd(map[string]float64{"a": 5}, ZAddOptions{})

	score := z.ZScore("a")
	require.NotNil(t, score)
	assert.Equal(t, 5.0, *score)
}

func TestZSet_ZScore_AfterRemove(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[string]float64{"a": 1}, ZAddOptions{})
	z.ZRem([]string{"a"})

	assert.Nil(t, z.ZScore("a"))
}
