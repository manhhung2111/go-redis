package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBFAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.BFAdd("bf", "item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Verify bloom filter was created
	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
	assert.Equal(t, EncBloomFilter, rObj.Encoding)
}

func TestBFAdd_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	// Add first item
	result1 := s.BFAdd("bf", "item1")
	assert.Equal(t, 1, result1)

	// Add same item again
	result2 := s.BFAdd("bf", "item1")
	assert.Equal(t, 0, result2, "should return 0 for existing item")

	// Add different item
	result3 := s.BFAdd("bf", "item2")
	assert.Equal(t, 1, result3, "should return 1 for new item")
}

func TestBFAdd_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"apple", "banana", "cherry"}
	for _, item := range items {
		s.BFAdd("bf", item)
	}

	// Verify all items exist
	for _, item := range items {
		assert.Equal(t, 1, s.BFExists("bf", item), "item should exist: %s", item)
	}
}

func TestBFAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Should panic when trying to add to wrong type
	assert.Panics(t, func() {
		s.BFAdd("mykey", "item")
	})
}

func TestBFCard_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.BFCard("nonexistent")
	assert.Equal(t, 0, result)
}

func TestBFCard_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item2")
	s.BFAdd("bf", "item3")

	assert.Equal(t, 3, s.BFCard("bf"))
}

func TestBFCard_WithDuplicates(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item1") // duplicate
	s.BFAdd("bf", "item2")

	assert.Equal(t, 2, s.BFCard("bf"), "duplicates should not increase card")
}

func TestBFExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.BFExists("nonexistent", "item")
	assert.Equal(t, 0, result)
}

func TestBFExists_ItemExists(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	assert.Equal(t, 1, s.BFExists("bf", "item1"))
}

func TestBFExists_ItemNotExists(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	assert.Equal(t, 0, s.BFExists("bf", "item2"))
}

func TestBFInfo_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	assert.Panics(t, func() {
		s.BFInfo("nonexistent", data_structure.BloomFilterInfoAll)
	})
}

func TestBFInfo_Capacity(t *testing.T) {
	s := NewStore().(*store)

	s.BFReserve("bf", 0.01, 1000, 2)

	info := s.BFInfo("bf", data_structure.BloomFilterInfoCapacity)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(1000), info[0])
}

func TestBFInfo_Size(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info := s.BFInfo("bf", data_structure.BloomFilterInfoSize)
	require.Len(t, info, 1)

	size, ok := info[0].(uint64)
	require.True(t, ok)
	assert.Greater(t, size, uint64(0))
}

func TestBFInfo_Filters(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info := s.BFInfo("bf", data_structure.BloomFilterInfoFilters)
	require.Len(t, info, 1)
	assert.Equal(t, 1, info[0])
}

func TestBFInfo_Items(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item2")

	info := s.BFInfo("bf", data_structure.BloomFilterInfoItems)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(2), info[0])
}

func TestBFInfo_Expansion(t *testing.T) {
	s := NewStore().(*store)

	s.BFReserve("bf", 0.01, 100, 4)

	info := s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	require.Len(t, info, 1)
	assert.Equal(t, 4, info[0])
}

func TestBFInfo_All(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info := s.BFInfo("bf", data_structure.BloomFilterInfoAll)
	require.Len(t, info, 10)

	assert.Equal(t, "Capacity", info[0])
	assert.Equal(t, "Size", info[2])
	assert.Equal(t, "Number of filters", info[4])
	assert.Equal(t, "Number of items inserted", info[6])
	assert.Equal(t, "Expansion rate", info[8])
}

func TestBFMAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"item1", "item2", "item3"}
	results := s.BFMAdd("bf", items)

	require.Len(t, results, 3)
	for i, result := range results {
		assert.Equal(t, 1, result, "item %d should be new", i)
	}
}

func TestBFMAdd_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	// Add some items first
	s.BFAdd("bf", "item1")

	// MAdd with mix of new and existing
	items := []string{"item1", "item2", "item3"}
	results := s.BFMAdd("bf", items)

	expected := []int{0, 1, 1}
	assert.Equal(t, expected, results)
}

func TestBFMAdd_Empty(t *testing.T) {
	s := NewStore().(*store)

	results := s.BFMAdd("bf", []string{})
	assert.Empty(t, results)
}

func TestBFMAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a list key
	s.LPush("mylist", "value")

	assert.Panics(t, func() {
		s.BFMAdd("mylist", []string{"item1"})
	})
}

func TestBFMExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"item1", "item2", "item3"}
	results := s.BFMExists("nonexistent", items)

	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
}

func TestBFMExists_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item3")

	items := []string{"item1", "item2", "item3", "item4"}
	results := s.BFMExists("bf", items)

	expected := []int{1, 0, 1, 0}
	assert.Equal(t, expected, results)
}

func TestBFMExists_Empty(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	results := s.BFMExists("bf", []string{})
	assert.Empty(t, results)
}

func TestBFReserve_NewKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("bf", 0.001, 5000, 4)
	require.NoError(t, err)

	// Verify bloom filter was created with correct settings
	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
	assert.Equal(t, EncBloomFilter, rObj.Encoding)

	// Verify settings
	info := s.BFInfo("bf", data_structure.BloomFilterInfoCapacity)
	assert.Equal(t, uint64(5000), info[0])

	info = s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	assert.Equal(t, 4, info[0])
}

func TestBFReserve_ExistingBloomFilter(t *testing.T) {
	s := NewStore().(*store)

	// Create first bloom filter
	err := s.BFReserve("bf", 0.01, 100, 2)
	require.NoError(t, err)

	// Try to reserve again
	err = s.BFReserve("bf", 0.01, 200, 4)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestBFReserve_ExistingOtherType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Try to reserve - should fail because key exists
	err := s.BFReserve("mykey", 0.01, 100, 2)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestBFReserve_CustomSettings(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("bf", 0.0001, 10000, 3)
	require.NoError(t, err)

	// Add items and verify filter works
	s.BFAdd("bf", "item1")
	assert.Equal(t, 1, s.BFExists("bf", "item1"))
	assert.Equal(t, 0, s.BFExists("bf", "item2"))
}

func TestGetBloomFilter_NonExisting(t *testing.T) {
	s := NewStore().(*store)

	sbf, exists := s.getBloomFilter("nonexistent")
	assert.Nil(t, sbf)
	assert.False(t, exists)
}

func TestGetBloomFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	sbf, exists := s.getBloomFilter("bf")
	require.True(t, exists)
	require.NotNil(t, sbf)
	assert.Equal(t, 1, sbf.Card())
}

func TestGetBloomFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.getBloomFilter("mykey")
	})
}

func TestGetOrCreateBloomFilter_Create(t *testing.T) {
	s := NewStore().(*store)

	sbf := s.getOrCreateBloomFilter("bf")
	require.NotNil(t, sbf)

	// Verify it was stored
	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
}

func TestGetOrCreateBloomFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	// Create first
	sbf1 := s.getOrCreateBloomFilter("bf")
	sbf1.Add("item1")

	// Get again - should return same filter
	sbf2 := s.getOrCreateBloomFilter("bf")
	assert.Equal(t, 1, sbf2.Card(), "should be same filter with item added")
}

func TestGetOrCreateBloomFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.getOrCreateBloomFilter("mykey")
	})
}

func TestBFAdd_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	// Create bloom filter and set it as expired
	s.BFAdd("bf", "old_item")
	s.expires["bf"] = 1 // expired timestamp

	// Add new item - should create new filter since old one expired
	result := s.BFAdd("bf", "new_item")
	assert.Equal(t, 1, result)

	// Old item should not exist (filter was recreated)
	assert.Equal(t, 0, s.BFExists("bf", "old_item"))
	assert.Equal(t, 1, s.BFExists("bf", "new_item"))
}

func TestBFCard_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1 // expired

	result := s.BFCard("bf")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestBFExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1 // expired

	result := s.BFExists("bf", "item1")
	assert.Equal(t, 0, result, "expired key should return 0")
}

func TestBFMExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1 // expired

	results := s.BFMExists("bf", []string{"item1", "item2"})
	expected := []int{0, 0}
	assert.Equal(t, expected, results)
}

func TestBFReserve_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1 // expired

	// Should succeed since key expired
	err := s.BFReserve("bf", 0.01, 100, 2)
	assert.NoError(t, err)
}

func TestBloomFilter_FullWorkflow(t *testing.T) {
	s := NewStore().(*store)

	// Reserve with custom settings
	err := s.BFReserve("myfilter", 0.01, 1000, 2)
	require.NoError(t, err)

	// Add items
	s.BFAdd("myfilter", "user:1")
	s.BFAdd("myfilter", "user:2")
	s.BFAdd("myfilter", "user:3")

	// Check existence
	assert.Equal(t, 1, s.BFExists("myfilter", "user:1"))
	assert.Equal(t, 1, s.BFExists("myfilter", "user:2"))
	assert.Equal(t, 1, s.BFExists("myfilter", "user:3"))
	assert.Equal(t, 0, s.BFExists("myfilter", "user:999"))

	// Check card
	assert.Equal(t, 3, s.BFCard("myfilter"))

	// Check info
	info := s.BFInfo("myfilter", data_structure.BloomFilterInfoItems)
	assert.Equal(t, uint64(3), info[0])
}

func TestBloomFilter_MAddAndMExists(t *testing.T) {
	s := NewStore().(*store)

	// Batch add
	items := []string{"a", "b", "c", "d", "e"}
	results := s.BFMAdd("bf", items)

	// All should be new
	for _, r := range results {
		assert.Equal(t, 1, r)
	}

	// Batch check
	checkItems := []string{"a", "x", "c", "y", "e"}
	existsResults := s.BFMExists("bf", checkItems)

	expected := []int{1, 0, 1, 0, 1}
	assert.Equal(t, expected, existsResults)
}

func TestBloomFilter_DefaultSettings(t *testing.T) {
	s := NewStore().(*store)

	// BFAdd creates filter with default settings
	s.BFAdd("bf", "item1")

	// Verify default expansion rate
	info := s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	assert.Equal(t, 2, info[0], "default expansion should be 2")
}
