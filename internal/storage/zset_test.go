package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestZAdd
func TestZAdd_NewKey(t *testing.T) {
	s := NewStore()

	added, err := s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), *added)
}

func TestZAdd_ExistingKey(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	added, err := s.ZAdd("z", map[string]float64{"b": 2, "c": 3}, data_structure.ZAddOptions{})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), *added)
}

func TestZAdd_UpdateExistingMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	added, err := s.ZAdd("z", map[string]float64{"a": 5}, data_structure.ZAddOptions{})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), *added)

	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 5.0, *score)
}

func TestZAdd_NXOption(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	added, err := s.ZAdd("z", map[string]float64{"a": 2}, data_structure.ZAddOptions{NX: true})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), *added)

	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 1.0, *score)
}

func TestZAdd_XXOption(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	added, err := s.ZAdd("z", map[string]float64{"a": 2, "b": 3}, data_structure.ZAddOptions{XX: true})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), *added)

	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 2.0, *score)

	scoreB, _ := s.ZScore("z", "b")
	assert.Nil(t, scoreB)
}

func TestZAdd_GTOption(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 5}, data_structure.ZAddOptions{})

	added, _ := s.ZAdd("z", map[string]float64{"a": 3}, data_structure.ZAddOptions{GT: true})
	assert.Equal(t, uint32(0), *added)

	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 5.0, *score)

	added, _ = s.ZAdd("z", map[string]float64{"a": 10}, data_structure.ZAddOptions{GT: true})
	assert.Equal(t, uint32(0), *added)

	score, _ = s.ZScore("z", "a")
	assert.Equal(t, 10.0, *score)
}

func TestZAdd_LTOption(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 5}, data_structure.ZAddOptions{})

	added, _ := s.ZAdd("z", map[string]float64{"a": 10}, data_structure.ZAddOptions{LT: true})
	assert.Equal(t, uint32(0), *added)

	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 5.0, *score)

	added, _ = s.ZAdd("z", map[string]float64{"a": 3}, data_structure.ZAddOptions{LT: true})
	assert.Equal(t, uint32(0), *added)

	score, _ = s.ZScore("z", "a")
	assert.Equal(t, 3.0, *score)
}

func TestZAdd_CHOption(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	added, _ := s.ZAdd("z", map[string]float64{"a": 2, "b": 3}, data_structure.ZAddOptions{CH: true})
	assert.Equal(t, uint32(2), *added)
}

func TestZAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	added, err := s.ZAdd("key1", map[string]float64{"a": 1}, data_structure.ZAddOptions{})
	assert.Error(t, err)
	assert.Nil(t, added)
}

// TestZCard
func TestZCard_ExistingKey(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	card, err := s.ZCard("z")
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), card)
}

func TestZCard_NonExistentKey(t *testing.T) {
	s := NewStore()

	card, err := s.ZCard("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), card)
}

func TestZCard_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	card, err := s.ZCard("key1")
	assert.Error(t, err)
	assert.Equal(t, uint32(0), card)
}

// TestZCount
func TestZCount_InRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3, "d": 4}, data_structure.ZAddOptions{})

	count, err := s.ZCount("z", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), count)
}

func TestZCount_NoMembersInRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	count, err := s.ZCount("z", 5, 10)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestZCount_NonExistentKey(t *testing.T) {
	s := NewStore()

	count, err := s.ZCount("nonexistent", 0, 10)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestZCount_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	count, err := s.ZCount("key1", 0, 10)
	assert.Error(t, err)
	assert.Equal(t, uint32(0), count)
}

// TestZIncrBy
func TestZIncrBy_NewMember(t *testing.T) {
	s := NewStore()

	score, err := s.ZIncrBy("z", "a", 5)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, score)
}

func TestZIncrBy_ExistingMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 3}, data_structure.ZAddOptions{})

	score, err := s.ZIncrBy("z", "a", 2)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, score)
}

func TestZIncrBy_NegativeIncrement(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 10}, data_structure.ZAddOptions{})

	score, err := s.ZIncrBy("z", "a", -3)
	assert.NoError(t, err)
	assert.Equal(t, 7.0, score)
}

func TestZIncrBy_InfinityResult(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1e308}, data_structure.ZAddOptions{})

	_, err := s.ZIncrBy("z", "a", 1e308)
	assert.Error(t, err)
}

func TestZIncrBy_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	score, err := s.ZIncrBy("key1", "a", 5)
	assert.Error(t, err)
	assert.Equal(t, 0.0, score)
}

// TestZScore
func TestZScore_ExistingMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 5.5}, data_structure.ZAddOptions{})

	score, err := s.ZScore("z", "a")
	assert.NoError(t, err)
	require.NotNil(t, score)
	assert.Equal(t, 5.5, *score)
}

func TestZScore_NonExistentMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	score, err := s.ZScore("z", "b")
	assert.NoError(t, err)
	assert.Nil(t, score)
}

func TestZScore_NonExistentKey(t *testing.T) {
	s := NewStore()

	score, err := s.ZScore("nonexistent", "a")
	assert.NoError(t, err)
	assert.Nil(t, score)
}

func TestZScore_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	score, err := s.ZScore("key1", "a")
	assert.Error(t, err)
	assert.Nil(t, score)
}

// TestZMScore
func TestZMScore_AllExisting(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	scores, err := s.ZMScore("z", []string{"a", "b", "c"})
	assert.NoError(t, err)
	require.Len(t, scores, 3)
	assert.Equal(t, 1.0, *scores[0])
	assert.Equal(t, 2.0, *scores[1])
	assert.Equal(t, 3.0, *scores[2])
}

func TestZMScore_MixedExistingAndMissing(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "c": 3}, data_structure.ZAddOptions{})

	scores, err := s.ZMScore("z", []string{"a", "b", "c"})
	assert.NoError(t, err)
	require.Len(t, scores, 3)
	assert.Equal(t, 1.0, *scores[0])
	assert.Nil(t, scores[1])
	assert.Equal(t, 3.0, *scores[2])
}

func TestZMScore_NonExistentKey(t *testing.T) {
	s := NewStore()

	scores, err := s.ZMScore("nonexistent", []string{"a", "b"})
	assert.NoError(t, err)
	require.Len(t, scores, 2)
	assert.Nil(t, scores[0])
	assert.Nil(t, scores[1])
}

func TestZMScore_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	scores, err := s.ZMScore("key1", []string{"a", "b"})
	assert.Error(t, err)
	assert.Nil(t, scores)
}

// TestZRem
func TestZRem_ExistingMembers(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	removed, err := s.ZRem("z", []string{"a", "b"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), removed)

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(1), card)
}

func TestZRem_MixedExistingAndMissing(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	removed, err := s.ZRem("z", []string{"a", "c"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), removed)
}

func TestZRem_NonExistentMembers(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	removed, err := s.ZRem("z", []string{"b", "c"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), removed)
}

func TestZRem_NonExistentKey(t *testing.T) {
	s := NewStore()

	removed, err := s.ZRem("nonexistent", []string{"a"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), removed)
}

func TestZRem_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	removed, err := s.ZRem("key1", []string{"a"})
	assert.Error(t, err)
	assert.Equal(t, uint32(0), removed)
}

// TestZRank
func TestZRank_ExistingMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	rank, err := s.ZRank("z", "b", false)
	assert.NoError(t, err)
	require.NotNil(t, rank)
	assert.Equal(t, []any{1}, rank)
}

func TestZRank_WithScore(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	rank, err := s.ZRank("z", "b", true)
	assert.NoError(t, err)
	require.NotNil(t, rank)
	assert.Equal(t, []any{1, "2"}, rank)
}

func TestZRank_NonExistentMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	rank, err := s.ZRank("z", "x", false)
	assert.NoError(t, err)
	assert.Nil(t, rank)
}

func TestZRank_NonExistentKey(t *testing.T) {
	s := NewStore()

	rank, err := s.ZRank("nonexistent", "a", false)
	assert.NoError(t, err)
	assert.Nil(t, rank)
}

func TestZRank_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	rank, err := s.ZRank("key1", "a", false)
	assert.Error(t, err)
	assert.Nil(t, rank)
}

// TestZRevRank
func TestZRevRank_ExistingMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	rank, err := s.ZRevRank("z", "b", false)
	assert.NoError(t, err)
	require.NotNil(t, rank)
	assert.Equal(t, []any{1}, rank)
}

func TestZRevRank_WithScore(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	rank, err := s.ZRevRank("z", "b", true)
	assert.NoError(t, err)
	require.NotNil(t, rank)
	assert.Equal(t, []any{1, "2"}, rank)
}

func TestZRevRank_NonExistentMember(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	rank, err := s.ZRevRank("z", "x", false)
	assert.NoError(t, err)
	assert.Nil(t, rank)
}

func TestZRevRank_NonExistentKey(t *testing.T) {
	s := NewStore()

	rank, err := s.ZRevRank("nonexistent", "a", false)
	assert.NoError(t, err)
	assert.Nil(t, rank)
}

func TestZRevRank_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	rank, err := s.ZRevRank("key1", "a", false)
	assert.Error(t, err)
	assert.Nil(t, rank)
}

// TestZRangeByRank
func TestZRangeByRank_WithoutScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByRank("z", 0, 1, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, result)
}

func TestZRangeByRank_WithScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByRank("z", 0, -1, true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "1", "b", "2"}, result)
}

func TestZRangeByRank_NegativeIndices(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByRank("z", -2, -1, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c"}, result)
}

func TestZRangeByRank_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRangeByRank("nonexistent", 0, -1, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRangeByRank_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRangeByRank("key1", 0, -1, false)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZRangeByScore
func TestZRangeByScore_WithoutScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByScore("z", 1.5, 3.0, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c"}, result)
}

func TestZRangeByScore_WithScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByScore("z", 0, 10, true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "1", "b", "2"}, result)
}

func TestZRangeByScore_NoMembersInRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByScore("z", 5, 10, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRangeByScore_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRangeByScore("nonexistent", 0, 10, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRangeByScore_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRangeByScore("key1", 0, 10, false)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZRevRangeByRank
func TestZRevRangeByRank_WithoutScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByRank("z", 0, 1, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b"}, result)
}

func TestZRevRangeByRank_WithScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByRank("z", 0, -1, true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "2", "a", "1"}, result)
}

func TestZRevRangeByRank_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRevRangeByRank("nonexistent", 0, -1, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRevRangeByRank_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRevRangeByRank("key1", 0, -1, false)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZRevRangeByScore
func TestZRevRangeByScore_WithoutScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByScore("z", 3, 1.5, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b"}, result)
}

func TestZRevRangeByScore_WithScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByScore("z", 10, 0, true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "2", "a", "1"}, result)
}

func TestZRevRangeByScore_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRevRangeByScore("nonexistent", 10, 0, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRevRangeByScore_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRevRangeByScore("key1", 10, 0, false)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZPopMin
func TestZPopMin_SingleElement(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZPopMin("z", 1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "1"}, result)

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(2), card)
}

func TestZPopMin_MultipleElements(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZPopMin("z", 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "1", "b", "2"}, result)

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(1), card)
}

func TestZPopMin_MoreThanAvailable(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZPopMin("z", 5)
	assert.NoError(t, err)
	assert.Len(t, result, 4)
}

func TestZPopMin_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZPopMin("nonexistent", 1)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZPopMin_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZPopMin("key1", 1)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZPopMax
func TestZPopMax_SingleElement(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZPopMax("z", 1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "3"}, result)

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(2), card)
}

func TestZPopMax_MultipleElements(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3}, data_structure.ZAddOptions{})

	result, err := s.ZPopMax("z", 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "3", "b", "2"}, result)

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(1), card)
}

func TestZPopMax_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZPopMax("nonexistent", 1)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZPopMax_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZPopMax("key1", 1)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZRandMember
func TestZRandMember_WithoutScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRandMember("z", 1, false)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
}

func TestZRandMember_WithScores(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	result, err := s.ZRandMember("z", 1, true)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestZRandMember_NegativeCountAllowsDuplicates(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{})

	result, err := s.ZRandMember("z", -5, false)
	assert.NoError(t, err)
	assert.Len(t, result, 5)
}

func TestZRandMember_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRandMember("nonexistent", 1, false)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRandMember_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRandMember("key1", 1, false)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZLexCount
func TestZLexCount_InRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"apple": 0, "banana": 0, "cherry": 0, "date": 0}, data_structure.ZAddOptions{})

	count, err := s.ZLexCount("z", "a", "c")
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), count)
}

func TestZLexCount_AllMembers(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 0, "b": 0, "c": 0}, data_structure.ZAddOptions{})

	count, err := s.ZLexCount("z", "", "~")
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), count)
}

func TestZLexCount_NonExistentKey(t *testing.T) {
	s := NewStore()

	count, err := s.ZLexCount("nonexistent", "a", "z")
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestZLexCount_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	count, err := s.ZLexCount("key1", "a", "z")
	assert.Error(t, err)
	assert.Equal(t, uint32(0), count)
}

// TestZRangeByLex
func TestZRangeByLex_InRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"apple": 0, "banana": 0, "cherry": 0, "date": 0}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByLex("z", "b", "d")
	assert.NoError(t, err)
	assert.Equal(t, []string{"banana", "cherry"}, result)
}

func TestZRangeByLex_AllMembers(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 0, "b": 0, "c": 0}, data_structure.ZAddOptions{})

	result, err := s.ZRangeByLex("z", "", "~")
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestZRangeByLex_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRangeByLex("nonexistent", "a", "z")
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRangeByLex_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRangeByLex("key1", "a", "z")
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestZRevRangeByLex
func TestZRevRangeByLex_InRange(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"apple": 0, "banana": 0, "cherry": 0, "date": 0}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByLex("z", "d", "b")
	assert.NoError(t, err)
	assert.Equal(t, []string{"cherry", "banana"}, result)
}

func TestZRevRangeByLex_AllMembersReversed(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 0, "b": 0, "c": 0}, data_structure.ZAddOptions{})

	result, err := s.ZRevRangeByLex("z", "~", "")
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, result)
}

func TestZRevRangeByLex_NonExistentKey(t *testing.T) {
	s := NewStore()

	result, err := s.ZRevRangeByLex("nonexistent", "z", "a")
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestZRevRangeByLex_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data.Set("key1", &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"})

	result, err := s.ZRevRangeByLex("key1", "z", "a")
	assert.Error(t, err)
	assert.Nil(t, result)
}

// Integration tests
func TestZSetOperationsIntegration(t *testing.T) {
	s := NewStore()

	s.ZAdd("z", map[string]float64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, data_structure.ZAddOptions{})

	card, _ := s.ZCard("z")
	assert.Equal(t, uint32(5), card)

	count, _ := s.ZCount("z", 2, 4)
	assert.Equal(t, uint32(3), count)

	s.ZIncrBy("z", "a", 10)
	score, _ := s.ZScore("z", "a")
	assert.Equal(t, 11.0, *score)

	rank, _ := s.ZRank("z", "b", false)
	assert.Equal(t, []any{0}, rank)

	revRank, _ := s.ZRevRank("z", "b", false)
	assert.Equal(t, []any{4}, revRank) // After ZIncrBy("a", 10), order is b=2,c=3,d=4,e=5,a=11; revRank of b is 4

	s.ZRem("z", []string{"e"})
	card, _ = s.ZCard("z")
	assert.Equal(t, uint32(4), card)
}
