package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStoreCF() Store {
	return NewStore(config.NewConfig())
}

func TestCFAdd_NewKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, err := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for new item")
	assert.NoError(t, err)

	// Verify cuckoo filter was created
	rObj, exists := s.data.Get("cf")
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.objType)
	assert.Equal(t, EncCuckooFilter, rObj.encoding)
}

func TestCFAdd_ExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Add first item
	result1, err := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result1)
	assert.NoError(t, err)

	// Add same item again (cuckoo filter allows duplicates with Add)
	result2, err := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result2, "should return 1 for duplicate item (allowed)")
	assert.NoError(t, err)

	// Add different item
	result3, err := s.CFAdd("cf", "item2")
	assert.Equal(t, 1, result3, "should return 1 for new item")
	assert.NoError(t, err)
}

func TestCFAdd_MultipleItems(t *testing.T) {
	s := newTestStoreCF().(*store)

	items := []string{"apple", "banana", "cherry"}
	for _, item := range items {
		s.CFAdd("cf", item)
	}

	// Verify all items exist
	for _, item := range items {
		exists, err := s.CFExists("cf", item)
		assert.Equal(t, 1, exists, "item should exist: %s", item)
		assert.NoError(t, err)
	}
}

func TestCFAdd_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Create a string key
	s.Set("mykey", "value")

	_, err := s.CFAdd("mykey", "item")
	assert.Error(t, err)
}

func TestCFAdd_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Create cuckoo filter and set it as expired
	s.CFAdd("cf", "old_item")
	s.expires.Set("cf", 1) // expired timestamp

	// Add new item - should create new filter since old one expired
	result, err := s.CFAdd("cf", "new_item")
	assert.Equal(t, 1, result)
	assert.NoError(t, err)

	// Old item should not exist (filter was recreated)
	exists, err := s.CFExists("cf", "old_item")
	assert.Equal(t, 0, exists)

	exists, err = s.CFExists("cf", "new_item")
	assert.Equal(t, 1, exists)
}

func TestCFAddNx_NewKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, err := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for new item")
	assert.NoError(t, err)

	// Verify cuckoo filter was created
	rObj, exists := s.data.Get("cf")
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.objType)
}

func TestCFAddNx_ExistingItem(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Add first item
	result1, err := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result1, "should return 1 for new item")
	assert.NoError(t, err)

	// Try to add same item again
	result2, err := s.CFAddNx("cf", "item1")
	assert.Equal(t, 0, result2, "should return 0 for existing item")
	assert.NoError(t, err)

	// Add different item
	result3, err := s.CFAddNx("cf", "item2")
	assert.Equal(t, 1, result3, "should return 1 for new item")
	assert.NoError(t, err)
}

func TestCFAddNx_MultipleItems(t *testing.T) {
	s := newTestStoreCF().(*store)

	items := []string{"a", "b", "c", "d"}
	for _, item := range items {
		result, err := s.CFAddNx("cf", item)
		assert.Equal(t, 1, result, "should return 1 for new item: %s", item)
		assert.NoError(t, err)
	}

	// Add again - all should return 0
	for _, item := range items {
		result, err := s.CFAddNx("cf", item)
		assert.Equal(t, 0, result, "should return 0 for existing item: %s", item)
		assert.NoError(t, err)
	}
}

func TestCFAddNx_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Create a list key
	s.LPush("mylist", "value")

	_, err := s.CFAddNx("mylist", "item1")
	assert.Error(t, err)
}

func TestCFAddNx_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	// Should succeed since key expired
	result, err := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result)
	assert.NoError(t, err)
}

func TestCFCount_NonExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, err := s.CFCount("nonexistent", "item")
	assert.Equal(t, 0, result)
	assert.NoError(t, err)
}

func TestCFCount_ExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	count, err := s.CFCount("cf", "item1")
	assert.Equal(t, 1, count)
	assert.NoError(t, err)

	// Add duplicate
	s.CFAdd("cf", "item1")
	count, err = s.CFCount("cf", "item1")
	assert.Equal(t, 2, count)
	assert.NoError(t, err)
}

func TestCFCount_ItemNotExists(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	count, err := s.CFCount("cf", "item2")
	assert.Equal(t, 0, count)
	assert.NoError(t, err)
}

func TestCFCount_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.Set("mykey", "value")

	_, err := s.CFCount("mykey", "item")
	assert.Error(t, err)
}

func TestCFCount_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	result, err := s.CFCount("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
	assert.NoError(t, err)
}

func TestCFDel_NonExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, err := s.CFDel("nonexistent", "item")
	assert.Equal(t, 0, result)
	assert.Error(t, err)
}

func TestCFDel_ExistingItem(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	exists, err := s.CFExists("cf", "item1")
	assert.Equal(t, 1, exists)
	assert.NoError(t, err)

	result, _ := s.CFDel("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for successful deletion")
	exists, err = s.CFExists("cf", "item1")
	assert.Equal(t, 0, exists, "item should not exist after deletion")
	assert.NoError(t, err)
}

func TestCFDel_ItemNotExists(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	result, _ := s.CFDel("cf", "item2")
	assert.Equal(t, 0, result, "should return 0 for non-existent item")
}

func TestCFDel_MultipleItems(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Add items
	items := []string{"a", "b", "c", "d", "e"}
	for _, item := range items {
		s.CFAdd("cf", item)
	}

	// Delete some items
	deleted, err := s.CFDel("cf", "b")
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	deleted, err = s.CFDel("cf", "d")
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// Verify state
	exists, _ := s.CFExists("cf", "a")
	assert.Equal(t, 1, exists)

	exists, _ = s.CFExists("cf", "b")
	assert.Equal(t, 0, exists)

	exists, _ = s.CFExists("cf", "c")
	assert.Equal(t, 1, exists)

	exists, _ = s.CFExists("cf", "d")
	assert.Equal(t, 0, exists)

	exists, _ = s.CFExists("cf", "e")
	assert.Equal(t, 1, exists)
}

func TestCFDel_Duplicates(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Add same item twice
	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")

	count, _ := s.CFCount("cf", "item1")
	assert.Equal(t, 2, count)

	// Delete once
	result, _ := s.CFDel("cf", "item1")
	assert.Equal(t, 1, result)
	count, _ = s.CFCount("cf", "item1")
	assert.Equal(t, 1, count)

	// Delete again
	result, _ = s.CFDel("cf", "item1")
	assert.Equal(t, 1, result)
	count, _ = s.CFCount("cf", "item1")
	assert.Equal(t, 0, count)
}

func TestCFDel_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.Set("mykey", "value")

	_, err := s.CFDel("mykey", "item")
	assert.Error(t, err)
}

func TestCFDel_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	result, _ := s.CFDel("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestCFExists_NonExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, _ := s.CFExists("nonexistent", "item")
	assert.Equal(t, 0, result)
}

func TestCFExists_ItemExists(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	exists, err := s.CFExists("cf", "item1")
	assert.Equal(t, 1, exists)
	assert.NoError(t, err)
}

func TestCFExists_ItemNotExists(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	exists, err := s.CFExists("cf", "item2")
	assert.Equal(t, 0, exists)
	assert.NoError(t, err)
}

func TestCFExists_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.Set("mykey", "value")

	_, err := s.CFExists("mykey", "item")
	assert.Error(t, err)
}

func TestCFExists_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	result, _ := s.CFExists("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestCFInfo_NonExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	result, _ := s.CFInfo("nonexistent")
	assert.Nil(t, result)
}

func TestCFInfo_ExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item2")
	s.CFDel("cf", "item1")

	info, err := s.CFInfo("cf")
	require.Len(t, info, 16)
	require.NoError(t, err)

	// Verify structure: key-value pairs
	assert.Equal(t, "Size", info[0])
	assert.Equal(t, "Number of buckets", info[2])
	assert.Equal(t, "Number of filters", info[4])
	assert.Equal(t, 1, info[5])
	assert.Equal(t, "Number of items inserted", info[6])
	assert.Equal(t, uint64(1), info[7]) // 2 added - 1 deleted
	assert.Equal(t, "Number of items deleted", info[8])
	assert.Equal(t, uint64(1), info[9])
	assert.Equal(t, "Bucket size", info[10])
	assert.Equal(t, "Expansion rate", info[12])
	assert.Equal(t, "Max iterations", info[14])
}

func TestCFInfo_Size(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	info, err := s.CFInfo("cf")
	require.NoError(t, err)
	size := info[1].(uint64)
	assert.Greater(t, size, uint64(0))
}

func TestCFInfo_NumBuckets(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	info, err := s.CFInfo("cf")
	require.NoError(t, err)
	numBuckets := info[3].(uint64)
	assert.Greater(t, numBuckets, uint64(0))
}

func TestCFInfo_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.Set("mykey", "value")

	_, err := s.CFInfo("mykey")
	assert.Error(t, err)
}

func TestCFInfo_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	result, err := s.CFInfo("cf")
	assert.Nil(t, result, "expired key should return nil")
	assert.Error(t, err)
}

func TestCFMExists_NonExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	items := []string{"item1", "item2", "item3"}
	results, err := s.CFMExists("nonexistent", items)

	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
	assert.NoError(t, err)
}

func TestCFMExists_ExistingKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item3")

	items := []string{"item1", "item2", "item3", "item4"}
	results, err := s.CFMExists("cf", items)

	expected := []int{1, 0, 1, 0}
	assert.Equal(t, expected, results)
	assert.NoError(t, err)
}

func TestCFMExists_Empty(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")

	results, err := s.CFMExists("cf", []string{})
	assert.Empty(t, results)
	assert.NoError(t, err)
}

func TestCFMExists_WrongType(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.Set("mykey", "value")

	_, err := s.CFMExists("mykey", []string{"item1"})
	assert.Error(t, err)
}

func TestCFMExists_ExpiredKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	s.CFAdd("cf", "item1")
	s.expires.Set("cf", 1) // expired

	results, err := s.CFMExists("cf", []string{"item1", "item2"})
	expected := []int{0, 0}
	assert.Equal(t, expected, results)
	assert.NoError(t, err)
}

func TestCFReserve_NewKey(t *testing.T) {
	s := newTestStoreCF().(*store)

	err := s.CFReserve("cf", 1000, 4, 500, 2)
	require.NoError(t, err)

	// Verify cuckoo filter was created with correct settings
	rObj, exists := s.data.Get("cf")
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.objType)
	assert.Equal(t, EncCuckooFilter, rObj.encoding)

	// Verify settings via Info
	info, err := s.CFInfo("cf")
	assert.Equal(t, uint64(4), info[11])   // Bucket size
	assert.Equal(t, 2, info[13])           // Expansion rate
	assert.Equal(t, uint64(500), info[15]) // Max iterations
	assert.NoError(t, err)
}

func TestCFReserve_ExistingCuckooFilter(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Create first cuckoo filter
	err := s.CFReserve("cf", 100, 4, 500, 2)
	require.NoError(t, err)

	// Try to reserve again
	err = s.CFReserve("cf", 200, 8, 1000, 4)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestCFReserve_ExistingOtherType(t *testing.T) {
	s := newTestStoreCF().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Try to reserve - should fail because key exists
	err := s.CFReserve("mykey", 100, 4, 500, 2)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}