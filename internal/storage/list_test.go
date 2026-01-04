package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLPush_NewKey(t *testing.T) {
	s := NewStore()

	count := s.LPush("mylist", "world", "hello")
	assert.Equal(t, uint32(2), count)

	result := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestLPush_ExistingKey(t *testing.T) {
	s := NewStore()
	s.LPush("mylist", "world")

	count := s.LPush("mylist", "hello")
	assert.Equal(t, uint32(2), count)

	result := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestLPush_MultipleElements(t *testing.T) {
	s := NewStore()

	count := s.LPush("mylist", "three", "two", "one")
	assert.Equal(t, uint32(3), count)

	result := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"one", "two", "three"}, result)
}

func TestLPush_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	count := s.LPush("mykey", "value")
	assert.Equal(t, uint32(0), count)
}

func TestRPush_NewKey(t *testing.T) {
	s := NewStore()

	count := s.RPush("mylist", "hello", "world")
	assert.Equal(t, uint32(2), count)

	result := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestRPush_ExistingKey(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "hello")

	count := s.RPush("mylist", "world")
	assert.Equal(t, uint32(2), count)

	result := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestRPush_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	count := s.RPush("mykey", "value")
	assert.Equal(t, uint32(0), count)
}

func TestLPop_SingleElement(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	result := s.LPop("mylist", 1)
	assert.Equal(t, []string{"one"}, result)

	assert.Len(t, s.LRange("mylist", 0, -1), 2)
}

func TestLPop_MultipleElements(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four")

	result := s.LPop("mylist", 2)
	assert.Equal(t, []string{"one", "two"}, result)
}

func TestLPop_DeleteKeyWhenEmpty(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two")

	s.LPop("mylist", 2)
	_, exists := s.Get("mylist")
	assert.False(t, exists)
}

func TestLPop_NonExistentKey(t *testing.T) {
	s := NewStore()

	result := s.LPop("nonexistent", 1)
	assert.Nil(t, result)
}

func TestLPop_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	result := s.LPop("mykey", 1)
	assert.Nil(t, result)
}

func TestRPop_SingleElement(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	result := s.RPop("mylist", 1)
	assert.Equal(t, []string{"three"}, result)
}

func TestRPop_MultipleElements(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four")

	result := s.RPop("mylist", 2)
	assert.Equal(t, []string{"four", "three"}, result)
}

func TestRPop_DeleteKeyWhenEmpty(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two")

	s.RPop("mylist", 2)
	_, exists := s.Get("mylist")
	assert.False(t, exists)
}

func TestRPop_NonExistentKey(t *testing.T) {
	s := NewStore()

	result := s.RPop("nonexistent", 1)
	assert.Nil(t, result)
}

func TestLRange_PositiveIndices(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	result := s.LRange("mylist", 1, 3)
	assert.Equal(t, []string{"two", "three", "four"}, result)
}

func TestLRange_NegativeIndices(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	result := s.LRange("mylist", 0, -1)
	assert.Len(t, result, 5)
}

func TestLRange_NonExistentKey(t *testing.T) {
	s := NewStore()

	result := s.LRange("nonexistent", 0, -1)
	assert.Empty(t, result)
}

func TestLRange_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	result := s.LRange("mykey", 0, -1)
	assert.Nil(t, result)
}

func TestLIndex_PositiveIndex(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	val, ok := s.LIndex("mylist", 0)
	assert.True(t, ok)
	assert.Equal(t, "one", val)
}

func TestLIndex_NegativeIndex(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	val, ok := s.LIndex("mylist", -1)
	assert.True(t, ok)
	assert.Equal(t, "three", val)
}

func TestLIndex_NonExistentKey(t *testing.T) {
	s := NewStore()

	_, ok := s.LIndex("nonexistent", 0)
	assert.False(t, ok)
}

func TestLIndex_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	_, ok := s.LIndex("mykey", 0)
	assert.False(t, ok)
}

func TestLLen_WithElements(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	assert.Equal(t, uint32(3), s.LLen("mylist"))
}

func TestLLen_NonExistentKey(t *testing.T) {
	s := NewStore()

	assert.Equal(t, uint32(0), s.LLen("nonexistent"))
}

func TestLLen_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	assert.Equal(t, uint32(0), s.LLen("mykey"))
}

func TestLRem_RemoveAll(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed := s.LRem("mylist", 0, "a")
	assert.Equal(t, uint32(3), removed)

	assert.Equal(t, []string{"b", "c"}, s.LRange("mylist", 0, -1))
}

func TestLRem_FromHead(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed := s.LRem("mylist", 2, "a")
	assert.Equal(t, uint32(2), removed)

	assert.Equal(t, []string{"b", "c", "a"}, s.LRange("mylist", 0, -1))
}

func TestLRem_FromTail(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed := s.LRem("mylist", -2, "a")
	assert.Equal(t, uint32(2), removed)

	assert.Equal(t, []string{"a", "b", "c"}, s.LRange("mylist", 0, -1))
}

func TestLSet_PositiveIndex(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	err := s.LSet("mylist", 1, "new")
	require.NoError(t, err)

	assert.Equal(t, []string{"one", "new", "three"}, s.LRange("mylist", 0, -1))
}

func TestLSet_NegativeIndex(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	err := s.LSet("mylist", -1, "new")
	require.NoError(t, err)

	assert.Equal(t, "new", s.LRange("mylist", 0, -1)[2])
}

func TestLSet_NonExistentKey(t *testing.T) {
	s := NewStore()

	err := s.LSet("nonexistent", 0, "value")
	assert.Error(t, err)
}

func TestLSet_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	err := s.LSet("mykey", 0, "value")
	assert.Error(t, err)
}

func TestLTrim_PositiveIndices(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	s.LTrim("mylist", 1, 3)
	assert.Equal(t, []string{"two", "three", "four"}, s.LRange("mylist", 0, -1))
}

func TestLTrim_NegativeIndices(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	s.LTrim("mylist", -3, -1)
	assert.Equal(t, []string{"three", "four", "five"}, s.LRange("mylist", 0, -1))
}

func TestLTrim_DeleteKeyWhenEmpty(t *testing.T) {
	s := NewStore()
	s.RPush("mylist", "one", "two", "three")

	s.LTrim("mylist", 5, 10)
	_, exists := s.Get("mylist")
	assert.False(t, exists)
}

func TestListOperationsIntegration(t *testing.T) {
	s := NewStore()

	s.RPush("mylist", "a", "b", "c")
	s.LPush("mylist", "z")

	assert.Equal(t, uint32(4), s.LLen("mylist"))
	assert.Equal(t, []string{"z", "a", "b", "c"}, s.LRange("mylist", 0, -1))

	s.LPop("mylist", 1)
	s.RPop("mylist", 1)

	assert.Equal(t, uint32(2), s.LLen("mylist"))

	s.LSet("mylist", 0, "new")
	val, ok := s.LIndex("mylist", 0)
	require.True(t, ok)
	assert.Equal(t, "new", val)
}
