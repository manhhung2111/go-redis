package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStoreList() Store {
	return NewStore(config.NewConfig())
}

func TestLPush_NewKey(t *testing.T) {
	s := newTestStoreList()

	count, _ := s.LPush("mylist", "world", "hello")
	assert.Equal(t, uint32(2), count)

	result, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestLPush_ExistingKey(t *testing.T) {
	s := newTestStoreList()
	s.LPush("mylist", "world")

	count, _ := s.LPush("mylist", "hello")
	assert.Equal(t, uint32(2), count)

	result, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestLPush_MultipleElements(t *testing.T) {
	s := newTestStoreList()

	count, _ := s.LPush("mylist", "three", "two", "one")
	assert.Equal(t, uint32(3), count)

	result, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"one", "two", "three"}, result)
}

func TestLPush_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	count, err := s.LPush("mykey", "value")
	assert.Equal(t, uint32(0), count)
	assert.Error(t, err)
}

func TestRPush_NewKey(t *testing.T) {
	s := newTestStoreList()

	count, _ := s.RPush("mylist", "hello", "world")
	assert.Equal(t, uint32(2), count)

	result, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestRPush_ExistingKey(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "hello")

	count, _ := s.RPush("mylist", "world")
	assert.Equal(t, uint32(2), count)

	result, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"hello", "world"}, result)
}

func TestRPush_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	count, _ := s.RPush("mykey", "value")
	assert.Equal(t, uint32(0), count)
}

func TestLPop_SingleElement(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	result, _ := s.LPop("mylist", 1)
	assert.Equal(t, []string{"one"}, result)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Len(t, list, 2)
}

func TestLPop_MultipleElements(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four")

	result, _ := s.LPop("mylist", 2)
	assert.Equal(t, []string{"one", "two"}, result)
}

func TestLPop_DeleteKeyWhenEmpty(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two")

	s.LPop("mylist", 2)
	_, exists := s.(*store).data.Get("mylist")
	assert.False(t, exists)
}

func TestLPop_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	result, _ := s.LPop("nonexistent", 1)
	assert.Nil(t, result)
}

func TestLPop_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	result, err := s.LPop("mykey", 1)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestRPop_SingleElement(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	result, _ := s.RPop("mylist", 1)
	assert.Equal(t, []string{"three"}, result)
}

func TestRPop_MultipleElements(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four")

	result, _ := s.RPop("mylist", 2)
	assert.Equal(t, []string{"four", "three"}, result)
}

func TestRPop_DeleteKeyWhenEmpty(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two")

	s.RPop("mylist", 2)
	_, exists := s.(*store).data.Get("mylist")
	assert.False(t, exists)
}

func TestRPop_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	result, _ := s.RPop("nonexistent", 1)
	assert.Nil(t, result)
}

func TestLRange_PositiveIndices(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	result, _ := s.LRange("mylist", 1, 3)
	assert.Equal(t, []string{"two", "three", "four"}, result)
}

func TestLRange_NegativeIndices(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	result, _ := s.LRange("mylist", 0, -1)
	assert.Len(t, result, 5)
}

func TestLRange_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	result, _ := s.LRange("nonexistent", 0, -1)
	assert.Empty(t, result)
}

func TestLRange_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	result, _ := s.LRange("mykey", 0, -1)
	assert.Nil(t, result)
}

func TestLIndex_PositiveIndex(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	val, err := s.LIndex("mylist", 0)
	assert.NoError(t, err)
	assert.Equal(t, "one", *val)
}

func TestLIndex_NegativeIndex(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	val, err := s.LIndex("mylist", -1)
	assert.NoError(t, err)
	assert.Equal(t, "three", *val)
}

func TestLIndex_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	val, err := s.LIndex("nonexistent", 0)
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestLIndex_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	val, err := s.LIndex("mykey", 0)
	assert.Error(t, err)
	assert.Nil(t, val)
}

func TestLLen_WithElements(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	listLen, err := s.LLen("mylist")
	assert.Equal(t, uint32(3), listLen)
	assert.NoError(t, err)
}

func TestLLen_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	listLen, err := s.LLen("nonexistent")
	assert.Equal(t, uint32(0), listLen)
	assert.NoError(t, err)
}

func TestLLen_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	listLen, err := s.LLen("mykey")
	assert.Equal(t, uint32(0), listLen)
	assert.Error(t, err)
}

func TestLRem_RemoveAll(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed, _ := s.LRem("mylist", 0, "a")
	assert.Equal(t, uint32(3), removed)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"b", "c"}, list)
}

func TestLRem_FromHead(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed, _ := s.LRem("mylist", 2, "a")
	assert.Equal(t, uint32(2), removed)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"b", "c", "a"}, list)
}

func TestLRem_FromTail(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "a", "b", "a", "c", "a")

	removed, _ := s.LRem("mylist", -2, "a")
	assert.Equal(t, uint32(2), removed)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, list)
}

func TestLSet_PositiveIndex(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	err := s.LSet("mylist", 1, "new")
	require.NoError(t, err)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"one", "new", "three"}, list)
}

func TestLSet_NegativeIndex(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	err := s.LSet("mylist", -1, "new")
	require.NoError(t, err)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, "new", list[2])
}

func TestLSet_NonExistentKey(t *testing.T) {
	s := newTestStoreList()

	err := s.LSet("nonexistent", 0, "value")
	assert.Error(t, err)
}

func TestLSet_WrongType(t *testing.T) {
	s := newTestStoreList()
	s.Set("mykey", "string_value")

	err := s.LSet("mykey", 0, "value")
	assert.Error(t, err)
}

func TestLTrim_PositiveIndices(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	s.LTrim("mylist", 1, 3)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"two", "three", "four"}, list)
}

func TestLTrim_NegativeIndices(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three", "four", "five")

	s.LTrim("mylist", -3, -1)

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"three", "four", "five"}, list)
}

func TestLTrim_DeleteKeyWhenEmpty(t *testing.T) {
	s := newTestStoreList()
	s.RPush("mylist", "one", "two", "three")

	s.LTrim("mylist", 5, 10)
	_, exists := s.(*store).data.Get("mylist")
	assert.False(t, exists)
}

func TestListOperationsIntegration(t *testing.T) {
	s := newTestStoreList()

	s.RPush("mylist", "a", "b", "c")
	s.LPush("mylist", "z")

	list, _ := s.LRange("mylist", 0, -1)
	assert.Equal(t, 4, len(list))
	assert.Equal(t, []string{"z", "a", "b", "c"}, list)

	s.LPop("mylist", 1)
	s.RPop("mylist", 1)

	size, _ := s.LLen("mylist")
	assert.Equal(t, uint32(2), size)

	s.LSet("mylist", 0, "new")
	val, err := s.LIndex("mylist", 0)
	require.NoError(t, err)
	assert.Equal(t, "new", *val)
}
