package data_structure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewScalableBloomFilter(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)
	require.NotNil(t, sbf)

	// Should start with one filter
	info := sbf.Info(BloomFilterInfoFilters)
	assert.Equal(t, 1, info[0])

	// Should have correct expansion rate
	info = sbf.Info(BloomFilterInfoExpansion)
	assert.Equal(t, 2, info[0])
}

func TestNewScalableBloomFilterInvalidExpansionRate(t *testing.T) {
	// Invalid expansion rate (0) should default to 2
	sbf := NewScalableBloomFilter(0.01, 100, 0)
	require.NotNil(t, sbf)

	info := sbf.Info(BloomFilterInfoExpansion)
	assert.Equal(t, defaultExpansionRate, info[0])
}

func TestNewScalableBloomFilterNegativeExpansionRate(t *testing.T) {
	// Negative expansion rate should default to 2
	sbf := NewScalableBloomFilter(0.01, 100, -5)
	require.NotNil(t, sbf)

	info := sbf.Info(BloomFilterInfoExpansion)
	assert.Equal(t, defaultExpansionRate, info[0])
}

func TestScalableBloomFilterAdd(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Add new item
	result := sbf.Add("item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Add duplicate item
	result = sbf.Add("item1")
	assert.Equal(t, 0, result, "should return 0 for existing item")

	// Add another new item
	result = sbf.Add("item2")
	assert.Equal(t, 1, result, "should return 1 for new item")
}

func TestScalableBloomFilterAddMultipleItems(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, item := range items {
		result := sbf.Add(item)
		assert.Equal(t, 1, result, "should return 1 for new item: %s", item)
	}

	// Verify all items exist
	for _, item := range items {
		assert.Equal(t, 1, sbf.Exists(item), "item should exist: %s", item)
	}

	// Verify card is correct
	assert.Equal(t, len(items), sbf.Card())
}

func TestScalableBloomFilterExists(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Non-existing item
	result := sbf.Exists("nonexistent")
	assert.Equal(t, 0, result, "should return 0 for non-existing item")

	// Add item and check existence
	sbf.Add("exists")
	result = sbf.Exists("exists")
	assert.Equal(t, 1, result, "should return 1 for existing item")

	// Check non-existing item again
	result = sbf.Exists("still_nonexistent")
	assert.Equal(t, 0, result, "should return 0 for non-existing item")
}

func TestScalableBloomFilterExistsEmptyFilter(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Empty filter should not contain anything
	assert.Equal(t, 0, sbf.Exists("anything"))
	assert.Equal(t, 0, sbf.Exists(""))
	assert.Equal(t, 0, sbf.Exists("test"))
}

func TestScalableBloomFilterCard(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Empty filter
	assert.Equal(t, 0, sbf.Card())

	// After adding items
	sbf.Add("item1")
	assert.Equal(t, 1, sbf.Card())

	sbf.Add("item2")
	assert.Equal(t, 2, sbf.Card())

	// Adding duplicate should not increase card
	sbf.Add("item1")
	assert.Equal(t, 2, sbf.Card())

	sbf.Add("item3")
	assert.Equal(t, 3, sbf.Card())
}

func TestScalableBloomFilterMAdd(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	items := []string{"a", "b", "c", "d"}
	results := sbf.MAdd(items)

	assert.Len(t, results, len(items))
	for i, result := range results {
		assert.Equal(t, 1, result, "item %s should be new", items[i])
	}

	// Add again - all should return 0
	results = sbf.MAdd(items)
	for i, result := range results {
		assert.Equal(t, 0, result, "item %s should already exist", items[i])
	}
}

func TestScalableBloomFilterMAddMixed(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Add some items first
	sbf.Add("existing1")
	sbf.Add("existing2")

	// MAdd with mix of new and existing
	items := []string{"existing1", "new1", "existing2", "new2"}
	results := sbf.MAdd(items)

	expected := []int{0, 1, 0, 1}
	assert.Equal(t, expected, results)
}

func TestScalableBloomFilterMAddEmpty(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	results := sbf.MAdd([]string{})
	assert.Empty(t, results)
}

func TestScalableBloomFilterMExists(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Add some items
	sbf.Add("item1")
	sbf.Add("item2")
	sbf.Add("item3")

	// Check multiple items
	items := []string{"item1", "item4", "item2", "item5", "item3"}
	results := sbf.MExists(items)

	expected := []int{1, 0, 1, 0, 1}
	assert.Equal(t, expected, results)
}

func TestScalableBloomFilterMExistsEmpty(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	results := sbf.MExists([]string{})
	assert.Empty(t, results)
}

func TestScalableBloomFilterMExistsEmptyFilter(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	items := []string{"a", "b", "c"}
	results := sbf.MExists(items)

	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
}

func TestScalableBloomFilterInfoCapacity(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	info := sbf.Info(BloomFilterInfoCapacity)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(100), info[0])
}

func TestScalableBloomFilterInfoSize(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	info := sbf.Info(BloomFilterInfoSize)
	require.Len(t, info, 1)
	// Size should be > 0
	size, ok := info[0].(uint64)
	require.True(t, ok)
	assert.Greater(t, size, uint64(0))
}

func TestScalableBloomFilterInfoFilters(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	info := sbf.Info(BloomFilterInfoFilters)
	require.Len(t, info, 1)
	assert.Equal(t, 1, info[0])
}

func TestScalableBloomFilterInfoItems(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Empty
	info := sbf.Info(BloomFilterInfoItems)
	require.Len(t, info, 1)
	assert.Equal(t, uint64(0), info[0])

	// After adding
	sbf.Add("item1")
	sbf.Add("item2")
	info = sbf.Info(BloomFilterInfoItems)
	assert.Equal(t, uint64(2), info[0])
}

func TestScalableBloomFilterInfoExpansion(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 4)

	info := sbf.Info(BloomFilterInfoExpansion)
	require.Len(t, info, 1)
	assert.Equal(t, 4, info[0])
}

func TestScalableBloomFilterInfoAll(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)
	sbf.Add("item1")

	info := sbf.Info(BloomFilterInfoAll)
	require.Len(t, info, 10)

	assert.Equal(t, "Capacity", info[0])
	assert.Equal(t, uint64(100), info[1])
	assert.Equal(t, "Size", info[2])
	assert.Equal(t, "Number of filters", info[4])
	assert.Equal(t, 1, info[5])
	assert.Equal(t, "Number of items inserted", info[6])
	assert.Equal(t, uint64(1), info[7])
	assert.Equal(t, "Expansion rate", info[8])
	assert.Equal(t, 2, info[9])
}

func TestScalableBloomFilterAutoScaling(t *testing.T) {
	// Small capacity to trigger scaling
	sbf := NewScalableBloomFilter(0.01, 10, 2)

	// Initially should have 1 filter
	info := sbf.Info(BloomFilterInfoFilters)
	assert.Equal(t, 1, info[0])

	// Add items to exceed first filter capacity
	for i := 0; i < 15; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Should have scaled to 2 filters
	info = sbf.Info(BloomFilterInfoFilters)
	assert.Equal(t, 2, info[0])

	// Verify all items still exist
	for i := 0; i < 15; i++ {
		assert.Equal(t, 1, sbf.Exists(fmt.Sprintf("item%d", i)), "item%d should exist", i)
	}
}

func TestScalableBloomFilterMultipleScaling(t *testing.T) {
	// Very small capacity to trigger multiple scalings
	sbf := NewScalableBloomFilter(0.01, 5, 2)

	// Add many items to trigger multiple filter creations
	// Filter 0: capacity 5
	// Filter 1: capacity 10
	// Filter 2: capacity 20
	for i := 0; i < 40; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Should have multiple filters
	info := sbf.Info(BloomFilterInfoFilters)
	numFilters, ok := info[0].(int)
	require.True(t, ok)
	assert.GreaterOrEqual(t, numFilters, 3, "should have at least 3 filters")

	// Verify all items exist
	for i := 0; i < 40; i++ {
		assert.Equal(t, 1, sbf.Exists(fmt.Sprintf("item%d", i)), "item%d should exist", i)
	}

	// Verify card is correct
	assert.Equal(t, 40, sbf.Card())
}

func TestScalableBloomFilterCapacityGrowsAfterScaling(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 10, 2)

	initialInfo := sbf.Info(BloomFilterInfoCapacity)
	initialCapacity := initialInfo[0].(uint64)

	// Fill beyond initial capacity
	for i := 0; i < 15; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Capacity should have grown
	newInfo := sbf.Info(BloomFilterInfoCapacity)
	newCapacity := newInfo[0].(uint64)

	// With expansion rate 2: 10 + 20 = 30
	assert.Greater(t, newCapacity, initialCapacity)
	assert.Equal(t, uint64(30), newCapacity)
}

func TestScalableBloomFilterDuplicatesDontTriggerScaling(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 10, 2)

	// Add 5 items
	for i := 0; i < 5; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Add same items again many times
	for j := 0; j < 100; j++ {
		for i := 0; i < 5; i++ {
			sbf.Add(fmt.Sprintf("item%d", i))
		}
	}

	// Should still have 1 filter (duplicates don't count)
	info := sbf.Info(BloomFilterInfoFilters)
	assert.Equal(t, 1, info[0])

	// Card should be 5
	assert.Equal(t, 5, sbf.Card())
}

func TestScalableBloomFilterEmptyString(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Empty string should work
	result := sbf.Add("")
	assert.Equal(t, 1, result)

	assert.Equal(t, 1, sbf.Exists(""))
	assert.Equal(t, 1, sbf.Card())

	// Adding again should return 0
	result = sbf.Add("")
	assert.Equal(t, 0, result)
}

func TestScalableBloomFilterSpecialCharacters(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	specialItems := []string{
		"hello world",
		"tab\there",
		"newline\nhere",
		"unicode: 你好",
		"emoji: 🎉",
		"null\x00byte",
	}

	for _, item := range specialItems {
		result := sbf.Add(item)
		assert.Equal(t, 1, result, "should add: %q", item)
	}

	for _, item := range specialItems {
		assert.Equal(t, 1, sbf.Exists(item), "should exist: %q", item)
	}
}

func TestScalableBloomFilterLongStrings(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 100, 2)

	// Create a long string
	longString := ""
	for i := 0; i < 1000; i++ {
		longString += "a"
	}

	result := sbf.Add(longString)
	assert.Equal(t, 1, result)

	assert.Equal(t, 1, sbf.Exists(longString))

	// Similar but different long string
	differentLongString := longString + "b"
	assert.Equal(t, 0, sbf.Exists(differentLongString))
}

func TestScalableBloomFilterFalsePositiveRate(t *testing.T) {
	// Create filter with known error rate
	errorRate := 0.01
	capacity := uint64(1000)
	sbf := NewScalableBloomFilter(errorRate, capacity, 2)

	// Add items
	for i := uint64(0); i < capacity; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Check for false positives with items that were never added
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		// Use a different prefix to ensure these weren't added
		if sbf.Exists(fmt.Sprintf("nonexistent%d", i)) == 1 {
			falsePositives++
		}
	}

	// False positive rate should be reasonably close to target
	// Allow some margin (3x the target rate) due to statistical variation
	actualRate := float64(falsePositives) / float64(testCount)
	assert.Less(t, actualRate, errorRate*3, "false positive rate too high: %f", actualRate)
}

func TestScalableBloomFilterExistsAcrossFilters(t *testing.T) {
	// Small capacity to force multiple filters
	sbf := NewScalableBloomFilter(0.01, 5, 2)

	// Add items that will span multiple filters
	items := make([]string, 20)
	for i := 0; i < 20; i++ {
		items[i] = fmt.Sprintf("item%d", i)
		sbf.Add(items[i])
	}

	// All items should be found regardless of which filter they're in
	for _, item := range items {
		assert.Equal(t, 1, sbf.Exists(item), "item should exist: %s", item)
	}
}

func TestSubFilterBitOperations(t *testing.T) {
	// Test bit operations directly
	f := &subFilter{
		k:       3,
		bits:    make([]uint64, 2), // 128 bits
		numBits: 128,
	}

	// Set and get bits at various positions
	testPositions := []uint64{0, 1, 63, 64, 65, 127}

	for _, pos := range testPositions {
		assert.False(t, f.getSubFilterBit(pos), "bit %d should be unset initially", pos)
		f.setSubFilterBit(pos)
		assert.True(t, f.getSubFilterBit(pos), "bit %d should be set after setSubFilterBit", pos)
	}

	// Verify other bits are still unset
	assert.False(t, f.getSubFilterBit(2))
	assert.False(t, f.getSubFilterBit(62))
	assert.False(t, f.getSubFilterBit(66))
}

func TestSubFilterHashIndexes(t *testing.T) {
	f := &subFilter{
		k:       5,
		bits:    make([]uint64, 16),
		numBits: 1000,
	}

	// Get hash indexes for an item
	indexes := f.getSubFilterHashIndexes("test_item")

	// Should have k indexes
	assert.Len(t, indexes, 5)

	// All indexes should be within range
	for _, idx := range indexes {
		assert.Less(t, idx, f.numBits, "index should be less than numBits")
	}

	// Same item should produce same indexes
	indexes2 := f.getSubFilterHashIndexes("test_item")
	assert.Equal(t, indexes, indexes2)

	// Different item should produce different indexes (with high probability)
	indexes3 := f.getSubFilterHashIndexes("different_item")
	assert.NotEqual(t, indexes, indexes3)
}

func TestSubFilterExistsInSubFilter(t *testing.T) {
	f := &subFilter{
		k:       3,
		bits:    make([]uint64, 16),
		numBits: 1000,
	}

	// Item not in filter
	assert.False(t, f.existsInSubFilter("test"))

	// Add item by setting its bits
	indexes := f.getSubFilterHashIndexes("test")
	for _, idx := range indexes {
		f.setSubFilterBit(idx)
	}

	// Now should exist
	assert.True(t, f.existsInSubFilter("test"))

	// Other items should not exist (with high probability)
	assert.False(t, f.existsInSubFilter("other"))
}

func TestScalableBloomFilterSizeGrowsAfterScaling(t *testing.T) {
	sbf := NewScalableBloomFilter(0.01, 10, 2)

	initialInfo := sbf.Info(BloomFilterInfoSize)
	initialSize := initialInfo[0].(uint64)

	// Fill beyond initial capacity
	for i := 0; i < 15; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// Size should have grown
	newInfo := sbf.Info(BloomFilterInfoSize)
	newSize := newInfo[0].(uint64)

	assert.Greater(t, newSize, initialSize)
}

func TestScalableBloomFilterExpansionRateOne(t *testing.T) {
	// Expansion rate of 1 means each filter has same capacity
	sbf := NewScalableBloomFilter(0.01, 10, 1)

	info := sbf.Info(BloomFilterInfoExpansion)
	assert.Equal(t, 1, info[0])

	// Add items to trigger scaling
	for i := 0; i < 25; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	// With expansion rate 1, capacity grows linearly
	info = sbf.Info(BloomFilterInfoCapacity)
	capacity := info[0].(uint64)
	// Should have 3 filters with capacity 10 each = 30
	assert.Equal(t, uint64(30), capacity)
}

func TestScalableBloomFilterVeryHighErrorRate(t *testing.T) {
	// Very high error rate to test edge case where k might be very small
	sbf := NewScalableBloomFilter(0.99, 10, 2)
	require.NotNil(t, sbf)

	// Should still work
	sbf.Add("item1")
	assert.Equal(t, 1, sbf.Exists("item1"))
}

func TestScalableBloomFilterVeryLowErrorRate(t *testing.T) {
	// Very low error rate
	sbf := NewScalableBloomFilter(0.0001, 10, 2)
	require.NotNil(t, sbf)

	// Should still work
	sbf.Add("item1")
	assert.Equal(t, 1, sbf.Exists("item1"))
}

func TestScalableBloomFilterSmallCapacity(t *testing.T) {
	// Minimum capacity
	sbf := NewScalableBloomFilter(0.01, 1, 2)
	require.NotNil(t, sbf)

	// Should work with capacity 1
	sbf.Add("item1")
	assert.Equal(t, 1, sbf.Exists("item1"))

	// Adding second item should trigger scaling
	sbf.Add("item2")
	assert.Equal(t, 1, sbf.Exists("item2"))

	info := sbf.Info(BloomFilterInfoFilters)
	assert.GreaterOrEqual(t, info[0].(int), 2)
}

func TestScalableBloomFilterManyFilters(t *testing.T) {
	// Create many filters by using small capacity
	sbf := NewScalableBloomFilter(0.01, 2, 2)

	// Add enough items to create 5+ filters
	for i := 0; i < 100; i++ {
		sbf.Add(fmt.Sprintf("item%d", i))
	}

	info := sbf.Info(BloomFilterInfoFilters)
	numFilters := info[0].(int)
	assert.GreaterOrEqual(t, numFilters, 5)

	// All items should still exist
	for i := 0; i < 100; i++ {
		assert.Equal(t, 1, sbf.Exists(fmt.Sprintf("item%d", i)))
	}
}
