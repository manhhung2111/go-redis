package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCFAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Verify cuckoo filter was created
	rObj, exists := s.data["cf"]
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.Type)
	assert.Equal(t, EncCuckooFilter, rObj.Encoding)
}

func TestCFAdd_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	// Add first item
	result1 := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result1)

	// Add same item again (cuckoo filter allows duplicates with Add)
	result2 := s.CFAdd("cf", "item1")
	assert.Equal(t, 1, result2, "should return 1 for duplicate item (allowed)")

	// Add different item
	result3 := s.CFAdd("cf", "item2")
	assert.Equal(t, 1, result3, "should return 1 for new item")
}

func TestCFAdd_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"apple", "banana", "cherry"}
	for _, item := range items {
		s.CFAdd("cf", item)
	}

	// Verify all items exist
	for _, item := range items {
		assert.Equal(t, 1, s.CFExists("cf", item), "item should exist: %s", item)
	}
}

func TestCFAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Should panic when trying to add to wrong type
	assert.Panics(t, func() {
		s.CFAdd("mykey", "item")
	})
}

func TestCFAdd_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	// Create cuckoo filter and set it as expired
	s.CFAdd("cf", "old_item")
	s.expires["cf"] = 1 // expired timestamp

	// Add new item - should create new filter since old one expired
	result := s.CFAdd("cf", "new_item")
	assert.Equal(t, 1, result)

	// Old item should not exist (filter was recreated)
	assert.Equal(t, 0, s.CFExists("cf", "old_item"))
	assert.Equal(t, 1, s.CFExists("cf", "new_item"))
}

func TestCFAddNx_NewKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Verify cuckoo filter was created
	rObj, exists := s.data["cf"]
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.Type)
}

func TestCFAddNx_ExistingItem(t *testing.T) {
	s := NewStore().(*store)

	// Add first item
	result1 := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result1, "should return 1 for new item")

	// Try to add same item again
	result2 := s.CFAddNx("cf", "item1")
	assert.Equal(t, 0, result2, "should return 0 for existing item")

	// Add different item
	result3 := s.CFAddNx("cf", "item2")
	assert.Equal(t, 1, result3, "should return 1 for new item")
}

func TestCFAddNx_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"a", "b", "c", "d"}
	for _, item := range items {
		result := s.CFAddNx("cf", item)
		assert.Equal(t, 1, result, "should return 1 for new item: %s", item)
	}

	// Add again - all should return 0
	for _, item := range items {
		result := s.CFAddNx("cf", item)
		assert.Equal(t, 0, result, "should return 0 for existing item: %s", item)
	}
}

func TestCFAddNx_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a list key
	s.LPush("mylist", "value")

	assert.Panics(t, func() {
		s.CFAddNx("mylist", "item1")
	})
}

func TestCFAddNx_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	// Should succeed since key expired
	result := s.CFAddNx("cf", "item1")
	assert.Equal(t, 1, result)
}

func TestCFCount_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFCount("nonexistent", "item")
	assert.Equal(t, 0, result)
}

func TestCFCount_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	assert.Equal(t, 1, s.CFCount("cf", "item1"))

	// Add duplicate
	s.CFAdd("cf", "item1")
	assert.Equal(t, 2, s.CFCount("cf", "item1"))
}

func TestCFCount_ItemNotExists(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	assert.Equal(t, 0, s.CFCount("cf", "item2"))
}

func TestCFCount_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CFCount("mykey", "item")
	})
}

func TestCFCount_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	result := s.CFCount("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestCFDel_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFDel("nonexistent", "item")
	assert.Equal(t, 0, result)
}

func TestCFDel_ExistingItem(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	assert.Equal(t, 1, s.CFExists("cf", "item1"))

	result := s.CFDel("cf", "item1")
	assert.Equal(t, 1, result, "should return 1 for successful deletion")
	assert.Equal(t, 0, s.CFExists("cf", "item1"), "item should not exist after deletion")
}

func TestCFDel_ItemNotExists(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	result := s.CFDel("cf", "item2")
	assert.Equal(t, 0, result, "should return 0 for non-existent item")
}

func TestCFDel_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	// Add items
	items := []string{"a", "b", "c", "d", "e"}
	for _, item := range items {
		s.CFAdd("cf", item)
	}

	// Delete some items
	assert.Equal(t, 1, s.CFDel("cf", "b"))
	assert.Equal(t, 1, s.CFDel("cf", "d"))

	// Verify state
	assert.Equal(t, 1, s.CFExists("cf", "a"))
	assert.Equal(t, 0, s.CFExists("cf", "b"))
	assert.Equal(t, 1, s.CFExists("cf", "c"))
	assert.Equal(t, 0, s.CFExists("cf", "d"))
	assert.Equal(t, 1, s.CFExists("cf", "e"))
}

func TestCFDel_Duplicates(t *testing.T) {
	s := NewStore().(*store)

	// Add same item twice
	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")
	assert.Equal(t, 2, s.CFCount("cf", "item1"))

	// Delete once
	result := s.CFDel("cf", "item1")
	assert.Equal(t, 1, result)
	assert.Equal(t, 1, s.CFCount("cf", "item1"))

	// Delete again
	result = s.CFDel("cf", "item1")
	assert.Equal(t, 1, result)
	assert.Equal(t, 0, s.CFCount("cf", "item1"))
}

func TestCFDel_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CFDel("mykey", "item")
	})
}

func TestCFDel_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	result := s.CFDel("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestCFExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFExists("nonexistent", "item")
	assert.Equal(t, 0, result)
}

func TestCFExists_ItemExists(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	assert.Equal(t, 1, s.CFExists("cf", "item1"))
}

func TestCFExists_ItemNotExists(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	assert.Equal(t, 0, s.CFExists("cf", "item2"))
}

func TestCFExists_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CFExists("mykey", "item")
	})
}

func TestCFExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	result := s.CFExists("cf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestCFInfo_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CFInfo("nonexistent")
	assert.Nil(t, result)
}

func TestCFInfo_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item2")
	s.CFDel("cf", "item1")

	info := s.CFInfo("cf")
	require.Len(t, info, 16)

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
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	info := s.CFInfo("cf")
	size := info[1].(uint64)
	assert.Greater(t, size, uint64(0))
}

func TestCFInfo_NumBuckets(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	info := s.CFInfo("cf")
	numBuckets := info[3].(uint64)
	assert.Greater(t, numBuckets, uint64(0))
}

func TestCFInfo_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CFInfo("mykey")
	})
}

func TestCFInfo_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	result := s.CFInfo("cf")
	assert.Nil(t, result, "expired key should return nil")
}

func TestCFMExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"item1", "item2", "item3"}
	results := s.CFMExists("nonexistent", items)

	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
}

func TestCFMExists_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item3")

	items := []string{"item1", "item2", "item3", "item4"}
	results := s.CFMExists("cf", items)

	expected := []int{1, 0, 1, 0}
	assert.Equal(t, expected, results)
}

func TestCFMExists_Empty(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	results := s.CFMExists("cf", []string{})
	assert.Empty(t, results)
}

func TestCFMExists_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CFMExists("mykey", []string{"item1"})
	})
}

func TestCFMExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	results := s.CFMExists("cf", []string{"item1", "item2"})
	expected := []int{0, 0}
	assert.Equal(t, expected, results)
}

func TestCFReserve_NewKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CFReserve("cf", 1000, 4, 500, 2)
	require.NoError(t, err)

	// Verify cuckoo filter was created with correct settings
	rObj, exists := s.data["cf"]
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.Type)
	assert.Equal(t, EncCuckooFilter, rObj.Encoding)

	// Verify settings via Info
	info := s.CFInfo("cf")
	assert.Equal(t, uint64(4), info[11])   // Bucket size
	assert.Equal(t, 2, info[13])           // Expansion rate
	assert.Equal(t, uint64(500), info[15]) // Max iterations
}

func TestCFReserve_ExistingCuckooFilter(t *testing.T) {
	s := NewStore().(*store)

	// Create first cuckoo filter
	err := s.CFReserve("cf", 100, 4, 500, 2)
	require.NoError(t, err)

	// Try to reserve again
	err = s.CFReserve("cf", 200, 8, 1000, 4)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestCFReserve_ExistingOtherType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Try to reserve - should fail because key exists
	err := s.CFReserve("mykey", 100, 4, 500, 2)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestCFReserve_CustomSettings(t *testing.T) {
	s := NewStore().(*store)

	err := s.CFReserve("cf", 5000, 8, 1000, 4)
	require.NoError(t, err)

	// Add items and verify filter works
	s.CFAdd("cf", "item1")
	assert.Equal(t, 1, s.CFExists("cf", "item1"))
	assert.Equal(t, 0, s.CFExists("cf", "item2"))

	// Verify settings
	info := s.CFInfo("cf")
	assert.Equal(t, uint64(8), info[11])    // Bucket size
	assert.Equal(t, 4, info[13])            // Expansion rate
	assert.Equal(t, uint64(1000), info[15]) // Max iterations
}

func TestCFReserve_DefaultValues(t *testing.T) {
	s := NewStore().(*store)

	// Using 0 for optional parameters should use defaults
	err := s.CFReserve("cf", 1024, 0, 0, 0)
	require.NoError(t, err)

	info := s.CFInfo("cf")
	assert.Equal(t, uint64(4), info[11]) // Default bucket size
	assert.Equal(t, 2, info[13])         // Default expansion rate
}

func TestCFReserve_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	// Should succeed since key expired
	err := s.CFReserve("cf", 100, 4, 500, 2)
	assert.NoError(t, err)
}

func TestGetCuckooFilter_NonExisting(t *testing.T) {
	s := NewStore().(*store)

	scf, exists := s.getCuckooFilter("nonexistent")
	assert.Nil(t, scf)
	assert.False(t, exists)
}

func TestGetCuckooFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")

	scf, exists := s.getCuckooFilter("cf")
	require.True(t, exists)
	require.NotNil(t, scf)
	assert.Equal(t, 1, scf.Count("item1"))
}

func TestGetCuckooFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.getCuckooFilter("mykey")
	})
}

func TestGetCuckooFilter_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.expires["cf"] = 1 // expired

	scf, exists := s.getCuckooFilter("cf")
	assert.Nil(t, scf)
	assert.False(t, exists)
}

func TestGetOrCreateCuckooFilter_Create(t *testing.T) {
	s := NewStore().(*store)

	scf := s.getOrCreateCuckooFilter("cf")
	require.NotNil(t, scf)

	// Verify it was stored
	rObj, exists := s.data["cf"]
	require.True(t, exists)
	assert.Equal(t, ObjCuckooFilter, rObj.Type)
}

func TestGetOrCreateCuckooFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	// Create first
	scf1 := s.getOrCreateCuckooFilter("cf")
	scf1.Add("item1")

	// Get again - should return same filter
	scf2 := s.getOrCreateCuckooFilter("cf")
	assert.Equal(t, 1, scf2.Count("item1"), "should be same filter with item added")
}

func TestGetOrCreateCuckooFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.getOrCreateCuckooFilter("mykey")
	})
}

func TestGetOrCreateCuckooFilter_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "old_item")
	s.expires["cf"] = 1 // expired

	// Should create new filter since old one expired
	scf := s.getOrCreateCuckooFilter("cf")
	require.NotNil(t, scf)

	// Old item should not exist
	assert.Equal(t, 0, scf.Exists("old_item"))
}

func TestCuckooFilter_FullWorkflow(t *testing.T) {
	s := NewStore().(*store)

	// Reserve with custom settings
	err := s.CFReserve("myfilter", 1000, 4, 500, 2)
	require.NoError(t, err)

	// Add items
	s.CFAdd("myfilter", "user:1")
	s.CFAdd("myfilter", "user:2")
	s.CFAdd("myfilter", "user:3")

	// Check existence
	assert.Equal(t, 1, s.CFExists("myfilter", "user:1"))
	assert.Equal(t, 1, s.CFExists("myfilter", "user:2"))
	assert.Equal(t, 1, s.CFExists("myfilter", "user:3"))
	assert.Equal(t, 0, s.CFExists("myfilter", "user:999"))

	// Check count
	assert.Equal(t, 1, s.CFCount("myfilter", "user:1"))

	// Add duplicate
	s.CFAdd("myfilter", "user:1")
	assert.Equal(t, 2, s.CFCount("myfilter", "user:1"))

	// Delete one instance
	s.CFDel("myfilter", "user:1")
	assert.Equal(t, 1, s.CFCount("myfilter", "user:1"))
	assert.Equal(t, 1, s.CFExists("myfilter", "user:1"))

	// Check info
	info := s.CFInfo("myfilter")
	require.NotNil(t, info)
	assert.Equal(t, uint64(3), info[7]) // 4 added - 1 deleted
}

func TestCuckooFilter_AddNxAndExists(t *testing.T) {
	s := NewStore().(*store)

	// AddNx new items
	items := []string{"a", "b", "c", "d", "e"}
	for _, item := range items {
		result := s.CFAddNx("cf", item)
		assert.Equal(t, 1, result)
	}

	// Check MExists
	existsResults := s.CFMExists("cf", items)
	for i, r := range existsResults {
		assert.Equal(t, 1, r, "item %d should exist", i)
	}

	// Try AddNx again - all should fail
	for _, item := range items {
		result := s.CFAddNx("cf", item)
		assert.Equal(t, 0, result)
	}
}

func TestCuckooFilter_DeleteAndReAdd(t *testing.T) {
	s := NewStore().(*store)

	// Add, delete, re-add
	s.CFAdd("cf", "item1")
	assert.Equal(t, 1, s.CFExists("cf", "item1"))

	s.CFDel("cf", "item1")
	assert.Equal(t, 0, s.CFExists("cf", "item1"))

	s.CFAdd("cf", "item1")
	assert.Equal(t, 1, s.CFExists("cf", "item1"))
}

func TestCuckooFilter_DefaultSettings(t *testing.T) {
	s := NewStore().(*store)

	// CFAdd creates filter with default settings
	s.CFAdd("cf", "item1")

	info := s.CFInfo("cf")
	// Verify default expansion rate
	assert.Equal(t, 1, info[13], "default expansion should be 1")
}

func TestCuckooFilter_ManyItems(t *testing.T) {
	s := NewStore().(*store)

	// Add many items
	for i := 0; i < 100; i++ {
		result := s.CFAdd("cf", fmt.Sprintf("item%d", i))
		assert.Equal(t, 1, result)
	}

	// Verify all items exist
	for i := 0; i < 100; i++ {
		assert.Equal(t, 1, s.CFExists("cf", fmt.Sprintf("item%d", i)))
	}

	// Verify info shows correct count
	info := s.CFInfo("cf")
	assert.Equal(t, uint64(100), info[7])
}

func TestCuckooFilter_EmptyString(t *testing.T) {
	s := NewStore().(*store)

	// Empty string should work
	result := s.CFAdd("cf", "")
	assert.Equal(t, 1, result)

	assert.Equal(t, 1, s.CFExists("cf", ""))
	assert.Equal(t, 1, s.CFCount("cf", ""))

	// Delete
	result = s.CFDel("cf", "")
	assert.Equal(t, 1, result)
	assert.Equal(t, 0, s.CFExists("cf", ""))
}

func TestCuckooFilter_SpecialCharacters(t *testing.T) {
	s := NewStore().(*store)

	specialItems := []string{
		"hello world",
		"tab\there",
		"newline\nhere",
		"unicode: 你好",
		"emoji: 🎉",
		"null\x00byte",
	}

	for _, item := range specialItems {
		result := s.CFAdd("cf", item)
		assert.Equal(t, 1, result, "should add: %q", item)
	}

	for _, item := range specialItems {
		assert.Equal(t, 1, s.CFExists("cf", item), "should exist: %q", item)
	}

	for _, item := range specialItems {
		result := s.CFDel("cf", item)
		assert.Equal(t, 1, result, "should delete: %q", item)
		assert.Equal(t, 0, s.CFExists("cf", item), "should not exist after delete: %q", item)
	}
}

func TestCuckooFilter_MultipleKeys(t *testing.T) {
	s := NewStore().(*store)

	// Create multiple cuckoo filters
	s.CFAdd("cf1", "item1")
	s.CFAdd("cf2", "item1")
	s.CFAdd("cf3", "item1")

	// They should be independent
	s.CFDel("cf1", "item1")
	assert.Equal(t, 0, s.CFExists("cf1", "item1"))
	assert.Equal(t, 1, s.CFExists("cf2", "item1"))
	assert.Equal(t, 1, s.CFExists("cf3", "item1"))
}

func TestCuckooFilter_ReserveAndAdd(t *testing.T) {
	s := NewStore().(*store)

	// Reserve first
	err := s.CFReserve("cf", 500, 4, 500, 2)
	require.NoError(t, err)

	// Then add items
	for i := 0; i < 50; i++ {
		s.CFAdd("cf", fmt.Sprintf("item%d", i))
	}

	// All should exist
	for i := 0; i < 50; i++ {
		assert.Equal(t, 1, s.CFExists("cf", fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilter_Scaling(t *testing.T) {
	s := NewStore().(*store)

	// Reserve with small capacity to trigger scaling
	err := s.CFReserve("cf", 10, 4, 500, 2)
	require.NoError(t, err)

	// Initially should have 1 filter
	info := s.CFInfo("cf")
	assert.Equal(t, 1, info[5])

	// Add enough items to trigger scaling
	for i := 0; i < 50; i++ {
		s.CFAdd("cf", fmt.Sprintf("item%d", i))
	}

	// Should have more than 1 filter now
	info = s.CFInfo("cf")
	numFilters := info[5].(int)
	assert.GreaterOrEqual(t, numFilters, 2)

	// All items should still exist
	for i := 0; i < 50; i++ {
		assert.Equal(t, 1, s.CFExists("cf", fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilter_NoFalseNegatives(t *testing.T) {
	s := NewStore().(*store)

	// Add items
	items := make([]string, 500)
	for i := 0; i < 500; i++ {
		items[i] = fmt.Sprintf("item%d", i)
		s.CFAdd("cf", items[i])
	}

	// All added items must be found (no false negatives)
	for _, item := range items {
		assert.Equal(t, 1, s.CFExists("cf", item), "false negative for: %s", item)
	}
}

func TestCuckooFilter_CountAfterMultipleAddAndDelete(t *testing.T) {
	s := NewStore().(*store)

	// Add same item multiple times
	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item1")
	assert.Equal(t, 3, s.CFCount("cf", "item1"))

	// Delete one by one
	s.CFDel("cf", "item1")
	assert.Equal(t, 2, s.CFCount("cf", "item1"))

	s.CFDel("cf", "item1")
	assert.Equal(t, 1, s.CFCount("cf", "item1"))

	s.CFDel("cf", "item1")
	assert.Equal(t, 0, s.CFCount("cf", "item1"))

	// Delete again should return 0 and count stays 0
	result := s.CFDel("cf", "item1")
	assert.Equal(t, 0, result)
	assert.Equal(t, 0, s.CFCount("cf", "item1"))
}

func TestCuckooFilter_MExistsPreservesOrder(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "b")
	s.CFAdd("cf", "d")

	items := []string{"a", "b", "c", "d", "e"}
	results := s.CFMExists("cf", items)

	expected := []int{0, 1, 0, 1, 0}
	assert.Equal(t, expected, results, "results should be in same order as input")
}

func TestCuckooFilter_InfoTracksDeletes(t *testing.T) {
	s := NewStore().(*store)

	s.CFAdd("cf", "item1")
	s.CFAdd("cf", "item2")
	s.CFAdd("cf", "item3")

	info := s.CFInfo("cf")
	assert.Equal(t, uint64(3), info[7]) // items inserted
	assert.Equal(t, uint64(0), info[9]) // items deleted

	s.CFDel("cf", "item1")
	s.CFDel("cf", "item2")

	info = s.CFInfo("cf")
	assert.Equal(t, uint64(1), info[7]) // 3 - 2 = 1
	assert.Equal(t, uint64(2), info[9]) // 2 deletions
}
