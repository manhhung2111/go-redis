package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBFAdd
func TestBFAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	result, err := s.BFAdd("bf", "item1")
	assert.NoError(t, err)
	assert.Equal(t, 1, result)

	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
	assert.Equal(t, EncBloomFilter, rObj.Encoding)
}

func TestBFAdd_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result1, err := s.BFAdd("bf", "item1")
	assert.NoError(t, err)
	assert.Equal(t, 1, result1)

	result2, err := s.BFAdd("bf", "item1")
	assert.NoError(t, err)
	assert.Equal(t, 0, result2)

	result3, err := s.BFAdd("bf", "item2")
	assert.NoError(t, err)
	assert.Equal(t, 1, result3)
}

func TestBFAdd_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"apple", "banana", "cherry"}
	for _, item := range items {
		_, err := s.BFAdd("bf", item)
		assert.NoError(t, err)
	}

	for _, item := range items {
		exists, err := s.BFExists("bf", item)
		assert.NoError(t, err)
		assert.Equal(t, 1, exists)
	}
}

func TestBFAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	result, err := s.BFAdd("mykey", "item")
	assert.Error(t, err)
	assert.Equal(t, 0, result)
}

func TestBFAdd_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "old_item")
	s.expires["bf"] = 1

	result, err := s.BFAdd("bf", "new_item")
	assert.NoError(t, err)
	assert.Equal(t, 1, result)

	exists, _ := s.BFExists("bf", "old_item")
	assert.Equal(t, 0, exists)
	exists, _ = s.BFExists("bf", "new_item")
	assert.Equal(t, 1, exists)
}

// TestBFCard
func TestBFCard_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result, err := s.BFCard("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestBFCard_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item2")
	s.BFAdd("bf", "item3")

	card, err := s.BFCard("bf")
	assert.NoError(t, err)
	assert.Equal(t, 3, card)
}

func TestBFCard_WithDuplicates(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item2")

	card, err := s.BFCard("bf")
	assert.NoError(t, err)
	assert.Equal(t, 2, card)
}

func TestBFCard_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	card, err := s.BFCard("mykey")
	assert.Error(t, err)
	assert.Equal(t, 0, card)
}

func TestBFCard_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1

	result, err := s.BFCard("bf")
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}

// TestBFExists
func TestBFExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result, err := s.BFExists("nonexistent", "item")
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestBFExists_ItemExists(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	exists, err := s.BFExists("bf", "item1")
	assert.NoError(t, err)
	assert.Equal(t, 1, exists)
}

func TestBFExists_ItemNotExists(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	exists, err := s.BFExists("bf", "item2")
	assert.NoError(t, err)
	assert.Equal(t, 0, exists)
}

func TestBFExists_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	exists, err := s.BFExists("mykey", "item")
	assert.Error(t, err)
	assert.Equal(t, 0, exists)
}

func TestBFExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1

	result, err := s.BFExists("bf", "item1")
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}

// TestBFInfo
func TestBFInfo_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	info, err := s.BFInfo("nonexistent", data_structure.BloomFilterInfoAll)
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestBFInfo_Capacity(t *testing.T) {
	s := NewStore().(*store)

	s.BFReserve("bf", 0.01, 1000, 2)

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoCapacity)
	assert.NoError(t, err)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(1000), info[0])
}

func TestBFInfo_Size(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoSize)
	assert.NoError(t, err)
	require.Len(t, info, 1)

	size, ok := info[0].(uint64)
	require.True(t, ok)
	assert.Greater(t, size, uint64(0))
}

func TestBFInfo_Filters(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoFilters)
	assert.NoError(t, err)
	require.Len(t, info, 1)
	assert.Equal(t, 1, info[0])
}

func TestBFInfo_Items(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item2")

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoItems)
	assert.NoError(t, err)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(2), info[0])
}

func TestBFInfo_Expansion(t *testing.T) {
	s := NewStore().(*store)

	s.BFReserve("bf", 0.01, 100, 4)

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	assert.NoError(t, err)
	require.Len(t, info, 1)
	assert.Equal(t, 4, info[0])
}

func TestBFInfo_All(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoAll)
	assert.NoError(t, err)
	require.Len(t, info, 10)

	assert.Equal(t, "Capacity", info[0])
	assert.Equal(t, "Size", info[2])
	assert.Equal(t, "Number of filters", info[4])
	assert.Equal(t, "Number of items inserted", info[6])
	assert.Equal(t, "Expansion rate", info[8])
}

func TestBFInfo_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	info, err := s.BFInfo("mykey", data_structure.BloomFilterInfoAll)
	assert.Error(t, err)
	assert.Nil(t, info)
}

// TestBFMAdd
func TestBFMAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"item1", "item2", "item3"}
	results, err := s.BFMAdd("bf", items)

	assert.NoError(t, err)
	require.Len(t, results, 3)
	for i, result := range results {
		assert.Equal(t, 1, result, "item %d should be new", i)
	}
}

func TestBFMAdd_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	items := []string{"item1", "item2", "item3"}
	results, err := s.BFMAdd("bf", items)

	assert.NoError(t, err)
	expected := []int{0, 1, 1}
	assert.Equal(t, expected, results)
}

func TestBFMAdd_Empty(t *testing.T) {
	s := NewStore().(*store)

	results, err := s.BFMAdd("bf", []string{})
	assert.NoError(t, err)
	assert.Empty(t, results)
}

func TestBFMAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.LPush("mylist", "value")

	results, err := s.BFMAdd("mylist", []string{"item1"})
	assert.Error(t, err)
	assert.Nil(t, results)
}

// TestBFMExists
func TestBFMExists_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"item1", "item2", "item3"}
	results, err := s.BFMExists("nonexistent", items)

	assert.NoError(t, err)
	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
}

func TestBFMExists_ExistingKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.BFAdd("bf", "item3")

	items := []string{"item1", "item2", "item3", "item4"}
	results, err := s.BFMExists("bf", items)

	assert.NoError(t, err)
	expected := []int{1, 0, 1, 0}
	assert.Equal(t, expected, results)
}

func TestBFMExists_Empty(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	results, err := s.BFMExists("bf", []string{})
	assert.NoError(t, err)
	assert.Empty(t, results)
}

func TestBFMExists_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	results, err := s.BFMExists("mykey", []string{"item1"})
	assert.Error(t, err)
	assert.Nil(t, results)
}

func TestBFMExists_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1

	results, err := s.BFMExists("bf", []string{"item1", "item2"})
	assert.NoError(t, err)
	expected := []int{0, 0}
	assert.Equal(t, expected, results)
}

// TestBFReserve
func TestBFReserve_NewKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("bf", 0.001, 5000, 4)
	require.NoError(t, err)

	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
	assert.Equal(t, EncBloomFilter, rObj.Encoding)

	info, _ := s.BFInfo("bf", data_structure.BloomFilterInfoCapacity)
	assert.Equal(t, uint64(5000), info[0])

	info, _ = s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	assert.Equal(t, 4, info[0])
}

func TestBFReserve_ExistingBloomFilter(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("bf", 0.01, 100, 2)
	require.NoError(t, err)

	err = s.BFReserve("bf", 0.01, 200, 4)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestBFReserve_ExistingOtherType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	err := s.BFReserve("mykey", 0.01, 100, 2)
	assert.Error(t, err)
	assert.Equal(t, "item exists", err.Error())
}

func TestBFReserve_CustomSettings(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("bf", 0.0001, 10000, 3)
	require.NoError(t, err)

	s.BFAdd("bf", "item1")
	exists, _ := s.BFExists("bf", "item1")
	assert.Equal(t, 1, exists)
	exists, _ = s.BFExists("bf", "item2")
	assert.Equal(t, 0, exists)
}

func TestBFReserve_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")
	s.expires["bf"] = 1

	err := s.BFReserve("bf", 0.01, 100, 2)
	assert.NoError(t, err)
}

// TestGetBloomFilter
func TestGetBloomFilter_NonExisting(t *testing.T) {
	s := NewStore().(*store)

	sbf, err := s.getBloomFilter("nonexistent")
	assert.NoError(t, err)
	assert.Nil(t, sbf)
}

func TestGetBloomFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	sbf, err := s.getBloomFilter("bf")
	assert.NoError(t, err)
	require.NotNil(t, sbf)
	assert.Equal(t, 1, sbf.Card())
}

func TestGetBloomFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	sbf, err := s.getBloomFilter("mykey")
	assert.Error(t, err)
	assert.Nil(t, sbf)
}

// TestGetOrCreateBloomFilter
func TestGetOrCreateBloomFilter_Create(t *testing.T) {
	s := NewStore().(*store)

	sbf, err := s.getOrCreateBloomFilter("bf")
	assert.NoError(t, err)
	require.NotNil(t, sbf)

	rObj, exists := s.data["bf"]
	require.True(t, exists)
	assert.Equal(t, ObjBloomFilter, rObj.Type)
}

func TestGetOrCreateBloomFilter_Existing(t *testing.T) {
	s := NewStore().(*store)

	sbf1, _ := s.getOrCreateBloomFilter("bf")
	sbf1.Add("item1")

	sbf2, err := s.getOrCreateBloomFilter("bf")
	assert.NoError(t, err)
	assert.Equal(t, 1, sbf2.Card())
}

func TestGetOrCreateBloomFilter_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	sbf, err := s.getOrCreateBloomFilter("mykey")
	assert.Error(t, err)
	assert.Nil(t, sbf)
}

// Integration tests
func TestBloomFilter_FullWorkflow(t *testing.T) {
	s := NewStore().(*store)

	err := s.BFReserve("myfilter", 0.01, 1000, 2)
	require.NoError(t, err)

	s.BFAdd("myfilter", "user:1")
	s.BFAdd("myfilter", "user:2")
	s.BFAdd("myfilter", "user:3")

	exists, _ := s.BFExists("myfilter", "user:1")
	assert.Equal(t, 1, exists)
	exists, _ = s.BFExists("myfilter", "user:2")
	assert.Equal(t, 1, exists)
	exists, _ = s.BFExists("myfilter", "user:3")
	assert.Equal(t, 1, exists)
	exists, _ = s.BFExists("myfilter", "user:999")
	assert.Equal(t, 0, exists)

	card, _ := s.BFCard("myfilter")
	assert.Equal(t, 3, card)

	info, _ := s.BFInfo("myfilter", data_structure.BloomFilterInfoItems)
	assert.Equal(t, uint64(3), info[0])
}

func TestBloomFilter_MAddAndMExists(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"a", "b", "c", "d", "e"}
	results, err := s.BFMAdd("bf", items)
	assert.NoError(t, err)

	for _, r := range results {
		assert.Equal(t, 1, r)
	}

	checkItems := []string{"a", "x", "c", "y", "e"}
	existsResults, err := s.BFMExists("bf", checkItems)
	assert.NoError(t, err)

	expected := []int{1, 0, 1, 0, 1}
	assert.Equal(t, expected, existsResults)
}

func TestBloomFilter_DefaultSettings(t *testing.T) {
	s := NewStore().(*store)

	s.BFAdd("bf", "item1")

	info, err := s.BFInfo("bf", data_structure.BloomFilterInfoExpansion)
	assert.NoError(t, err)
	assert.Equal(t, 2, info[0])
}
