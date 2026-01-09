package data_structure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCountMinSketchByDim(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)
	require.NotNil(t, cms)

	info := cms.Info()
	assert.Equal(t, "width", info[0])
	assert.Equal(t, 100, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 5, info[3])
	assert.Equal(t, "count", info[4])
	assert.Equal(t, uint64(0), info[5])
}

func TestNewCountMinSketchByProb(t *testing.T) {
	// errorRate = 0.01 -> width = ceil(e / 0.01) = ceil(271.8) = 272
	// probability = 0.01 -> depth = ceil(ln(100)) = ceil(4.6) = 5
	cms := NewCountMinSketchByProb(0.01, 0.01)
	require.NotNil(t, cms)

	info := cms.Info()
	assert.Equal(t, "width", info[0])
	assert.Equal(t, 272, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 5, info[3])
}

func TestCountMinSketchIncrBySingleItem(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	result := cms.IncrBy(map[string]uint64{"item1": 1})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(1), result[0])

	// Query should return the same count
	queryResult := cms.Query([]string{"item1"})
	assert.Equal(t, uint64(1), queryResult[0])
}

func TestCountMinSketchIncrByMultipleItems(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	items := map[string]uint64{
		"apple":  1,
		"banana": 1,
		"cherry": 1,
	}

	result := cms.IncrBy(items)
	require.Len(t, result, 3)

	// All items should have count of at least 1
	for _, count := range result {
		assert.GreaterOrEqual(t, count, uint64(1))
	}
}

func TestCountMinSketchIncrByWithIncrement(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	// Add item with increment of 5
	result := cms.IncrBy(map[string]uint64{"item1": 5})
	assert.Equal(t, uint64(5), result[0])

	// Add more to the same item
	result = cms.IncrBy(map[string]uint64{"item1": 3})
	assert.Equal(t, uint64(8), result[0])

	// Query should return 8
	queryResult := cms.Query([]string{"item1"})
	assert.Equal(t, uint64(8), queryResult[0])
}

func TestCountMinSketchIncrByUpdatesTotalCount(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	cms.IncrBy(map[string]uint64{"item1": 5})
	cms.IncrBy(map[string]uint64{"item2": 10})
	cms.IncrBy(map[string]uint64{"item1": 3})

	info := cms.Info()
	assert.Equal(t, uint64(18), info[5]) // 5 + 10 + 3 = 18
}

func TestCountMinSketchQuerySingleItem(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	cms.IncrBy(map[string]uint64{"item1": 10})

	result := cms.Query([]string{"item1"})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(10), result[0])
}

func TestCountMinSketchQueryMultipleItems(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	cms.IncrBy(map[string]uint64{
		"apple":  5,
		"banana": 10,
		"cherry": 15,
	})

	result := cms.Query([]string{"apple", "banana", "cherry"})
	require.Len(t, result, 3)

	// Due to map iteration order, we need to query individually to verify
	appleCount := cms.Query([]string{"apple"})[0]
	bananaCount := cms.Query([]string{"banana"})[0]
	cherryCount := cms.Query([]string{"cherry"})[0]

	assert.Equal(t, uint64(5), appleCount)
	assert.Equal(t, uint64(10), bananaCount)
	assert.Equal(t, uint64(15), cherryCount)
}

func TestCountMinSketchQueryNonExistentItem(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	cms.IncrBy(map[string]uint64{"item1": 10})

	// Query for item that was never added
	result := cms.Query([]string{"nonexistent"})
	require.Len(t, result, 1)
	// Should return 0 (or possibly a small overestimate due to hash collisions)
	assert.Equal(t, uint64(0), result[0])
}

func TestCountMinSketchInfo(t *testing.T) {
	cms := NewCountMinSketchByDim(200, 10)

	cms.IncrBy(map[string]uint64{"item1": 100})

	info := cms.Info()
	require.Len(t, info, 6)

	assert.Equal(t, "width", info[0])
	assert.Equal(t, 200, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 10, info[3])
	assert.Equal(t, "count", info[4])
	assert.Equal(t, uint64(100), info[5])
}

func TestCountMinSketchOverestimation(t *testing.T) {
	// CMS can overestimate but never underestimate
	cms := NewCountMinSketchByDim(100, 5)

	actualCounts := map[string]uint64{
		"item1": 50,
		"item2": 30,
		"item3": 20,
	}

	for item, count := range actualCounts {
		cms.IncrBy(map[string]uint64{item: count})
	}

	// Query and verify counts are >= actual (CMS never underestimates)
	for item, actualCount := range actualCounts {
		result := cms.Query([]string{item})
		assert.GreaterOrEqual(t, result[0], actualCount,
			"CMS should never underestimate count for %s", item)
	}
}

func TestCountMinSketchAccuracy(t *testing.T) {
	// Test with larger dimensions for better accuracy
	cms := NewCountMinSketchByDim(1000, 10)

	// Add many items with known counts
	for i := range 100 {
		item := fmt.Sprintf("item%d", i)
		cms.IncrBy(map[string]uint64{item: uint64(i + 1)})
	}

	// Verify counts are reasonably accurate
	correctCount := 0
	for i := range 100 {
		item := fmt.Sprintf("item%d", i)
		expectedCount := uint64(i + 1)
		result := cms.Query([]string{item})

		// Allow some overestimation but it should be close
		if result[0] == expectedCount {
			correctCount++
		}
	}

	// With good dimensions, most counts should be exact
	assert.Greater(t, correctCount, 90, "at least 90%% of counts should be exact")
}

func TestCountMinSketchEmptyQuery(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	result := cms.Query([]string{})
	assert.Len(t, result, 0)
}

func TestCountMinSketchEmptyIncrBy(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	result := cms.IncrBy(map[string]uint64{})
	assert.Len(t, result, 0)

	info := cms.Info()
	assert.Equal(t, uint64(0), info[5]) // totalCount should still be 0
}

func TestCountMinSketchSpecialCharacters(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	specialItems := map[string]uint64{
		"hello world":   1,
		"tab\there":     2,
		"newline\nhere": 3,
		"unicode: 你好":   4,
		"emoji: 🎉":     5,
		"null\x00byte":  6,
	}

	cms.IncrBy(specialItems)

	for item, expectedCount := range specialItems {
		result := cms.Query([]string{item})
		assert.GreaterOrEqual(t, result[0], expectedCount,
			"count for special item should be at least %d", expectedCount)
	}
}

func TestCountMinSketchEmptyString(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	cms.IncrBy(map[string]uint64{"": 5})

	result := cms.Query([]string{""})
	assert.Equal(t, uint64(5), result[0])
}

func TestCountMinSketchLongStrings(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	// Create a long string
	longString := ""
	for range 1000 {
		longString += "a"
	}

	cms.IncrBy(map[string]uint64{longString: 10})

	result := cms.Query([]string{longString})
	assert.Equal(t, uint64(10), result[0])

	// Different long string should have different count
	differentLongString := longString + "b"
	result = cms.Query([]string{differentLongString})
	assert.Equal(t, uint64(0), result[0])
}

func TestCountMinSketchDeterministic(t *testing.T) {
	// Same items should produce same counts
	cms1 := NewCountMinSketchByDim(100, 5)
	cms2 := NewCountMinSketchByDim(100, 5)

	items := map[string]uint64{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	cms1.IncrBy(items)
	cms2.IncrBy(items)

	for item := range items {
		count1 := cms1.Query([]string{item})[0]
		count2 := cms2.Query([]string{item})[0]
		assert.Equal(t, count1, count2, "same items should produce same count for %s", item)
	}
}

func TestCountMinSketchManyIncrements(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	// Increment same item many times
	for range 1000 {
		cms.IncrBy(map[string]uint64{"hotkey": 1})
	}

	result := cms.Query([]string{"hotkey"})
	assert.Equal(t, uint64(1000), result[0])

	info := cms.Info()
	assert.Equal(t, uint64(1000), info[5])
}

func TestCountMinSketchSmallDimensions(t *testing.T) {
	// Test with minimal dimensions
	cms := NewCountMinSketchByDim(1, 1)
	require.NotNil(t, cms)

	cms.IncrBy(map[string]uint64{"item1": 5})
	cms.IncrBy(map[string]uint64{"item2": 10})

	// With width=1, all items hash to same bucket
	// Both queries should return 15 (sum of all counts)
	result1 := cms.Query([]string{"item1"})
	result2 := cms.Query([]string{"item2"})

	assert.Equal(t, uint64(15), result1[0])
	assert.Equal(t, uint64(15), result2[0])
}

func TestCountMinSketchLargeCounts(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5)

	largeCount := uint64(1 << 32) // 4 billion+
	cms.IncrBy(map[string]uint64{"item1": largeCount})

	result := cms.Query([]string{"item1"})
	assert.Equal(t, largeCount, result[0])

	info := cms.Info()
	assert.Equal(t, largeCount, info[5])
}

func TestCountMinSketchByProbDimensions(t *testing.T) {
	testCases := []struct {
		errorRate   float64
		probability float64
		minWidth    int
		minDepth    int
	}{
		{0.1, 0.1, 27, 2},   // width = ceil(e/0.1) = 28, depth = ceil(ln(10)) = 3
		{0.01, 0.01, 272, 5}, // width = ceil(e/0.01) = 272, depth = ceil(ln(100)) = 5
		{0.001, 0.001, 2719, 7}, // width = ceil(e/0.001) = 2719, depth = ceil(ln(1000)) = 7
	}

	for _, tc := range testCases {
		cms := NewCountMinSketchByProb(tc.errorRate, tc.probability)
		info := cms.Info()

		width := info[1].(int)
		depth := info[3].(int)

		assert.GreaterOrEqual(t, width, tc.minWidth,
			"width should be at least %d for errorRate=%f", tc.minWidth, tc.errorRate)
		assert.GreaterOrEqual(t, depth, tc.minDepth,
			"depth should be at least %d for probability=%f", tc.minDepth, tc.probability)
	}
}

func TestCountMinSketchHashDistribution(t *testing.T) {
	// Test that different items get different hash positions
	cms := NewCountMinSketchByDim(1000, 5).(*countMinSketch)

	items := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	indexSets := make([][]uint64, len(items))

	for i, item := range items {
		indexSets[i] = cms.getIndexes(item)
	}

	// Check that not all items hash to the same positions
	allSame := true
	for i := 1; i < len(indexSets); i++ {
		for j := range indexSets[i] {
			if indexSets[i][j] != indexSets[0][j] {
				allSame = false
				break
			}
		}
		if !allSame {
			break
		}
	}

	assert.False(t, allSame, "different items should have different hash positions")
}

func TestCountMinSketchGetIndexesConsistent(t *testing.T) {
	cms := NewCountMinSketchByDim(100, 5).(*countMinSketch)

	item := "testitem"

	// Get indexes multiple times
	indexes1 := cms.getIndexes(item)
	indexes2 := cms.getIndexes(item)

	assert.Equal(t, indexes1, indexes2, "getIndexes should be deterministic")
}

func TestCountMinSketchGetIndexesBounds(t *testing.T) {
	width := 100
	depth := 5
	cms := NewCountMinSketchByDim(width, depth).(*countMinSketch)

	items := []string{"item1", "item2", "item3", "test", "hello", "world"}

	for _, item := range items {
		indexes := cms.getIndexes(item)

		assert.Len(t, indexes, depth, "should return %d indexes", depth)

		for i, idx := range indexes {
			assert.Less(t, idx, uint64(width),
				"index %d for item %s should be less than width %d", i, item, width)
		}
	}
}
