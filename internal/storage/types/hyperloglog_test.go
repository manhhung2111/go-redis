package types

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog()
	require.NotNil(t, hll)

	// Should start with empty cardinality
	count := hll.PFCount(nil)
	assert.Equal(t, 0, count)
}

func TestHyperLogLogPFAddSingleItem(t *testing.T) {
	hll := NewHyperLogLog()

	// Add new item should return 1
	result, _ := hll.PFAdd([]string{"item1"})
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Cardinality should be ~1
	count := hll.PFCount(nil)
	assert.Equal(t, 1, count)
}

func TestHyperLogLogPFAddDuplicateItem(t *testing.T) {
	hll := NewHyperLogLog()

	// Add item first time
	result, _ := hll.PFAdd([]string{"item1"})
	assert.Equal(t, 1, result)

	// Add same item again - should return 0 (no register updated)
	result, _ = hll.PFAdd([]string{"item1"})
	assert.Equal(t, 0, result, "should return 0 for duplicate item")

	// Cardinality should still be ~1
	count := hll.PFCount(nil)
	assert.Equal(t, 1, count)
}

func TestHyperLogLogPFAddMultipleItems(t *testing.T) {
	hll := NewHyperLogLog()

	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	result, _ := hll.PFAdd(items)
	assert.Equal(t, 1, result, "should return 1 when registers updated")

	// Cardinality should be close to 5
	count := hll.PFCount(nil)
	assert.InDelta(t, 5, count, 1, "cardinality should be approximately 5")
}

func TestHyperLogLogPFAddEmptySlice(t *testing.T) {
	hll := NewHyperLogLog()

	result, _ := hll.PFAdd([]string{})
	assert.Equal(t, 0, result, "should return 0 for empty slice")

	count := hll.PFCount(nil)
	assert.Equal(t, 0, count)
}

func TestHyperLogLogPFAddEmptyString(t *testing.T) {
	hll := NewHyperLogLog()

	result, _ := hll.PFAdd([]string{""})
	assert.Equal(t, 1, result, "should return 1 for empty string item")

	count := hll.PFCount(nil)
	assert.Equal(t, 1, count)
}

func TestHyperLogLogPFCountEmpty(t *testing.T) {
	hll := NewHyperLogLog()

	count := hll.PFCount(nil)
	assert.Equal(t, 0, count, "empty HLL should have count 0")
}

func TestHyperLogLogPFCountSingle(t *testing.T) {
	hll := NewHyperLogLog()

	// Add 100 unique items
	for i := 0; i < 100; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	count := hll.PFCount(nil)
	// HLL has ~0.81% standard error, so allow 5% tolerance for small sets
	assert.InDelta(t, 100, count, 10, "cardinality should be approximately 100")
}

func TestHyperLogLogPFCountMultipleHLLs(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()
	hll3 := NewHyperLogLog()

	// Add different items to each HLL
	for i := 0; i < 100; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 100; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("b%d", i)})
	}
	for i := 0; i < 100; i++ {
		hll3.PFAdd([]string{fmt.Sprintf("c%d", i)})
	}

	// Count union of all three
	count := hll1.PFCount([]HyperLogLog{hll2, hll3})

	// Should be approximately 300
	assert.InDelta(t, 300, count, 30, "union cardinality should be approximately 300")
}

func TestHyperLogLogPFCountMultipleHLLsWithOverlap(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	// Add overlapping items
	for i := 0; i < 100; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}
	for i := 50; i < 150; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	// Count union - should be ~150 (items 0-149)
	count := hll1.PFCount([]HyperLogLog{hll2})
	assert.InDelta(t, 150, count, 15, "union cardinality should be approximately 150")
}

func TestHyperLogLogPFMerge(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()
	hll3 := NewHyperLogLog()

	// Add different items to each
	for i := 0; i < 50; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("b%d", i)})
	}
	for i := 0; i < 50; i++ {
		hll3.PFAdd([]string{fmt.Sprintf("c%d", i)})
	}

	// Merge hll2 and hll3 into hll1
	hll1.PFMerge([]HyperLogLog{hll2, hll3})

	// hll1 should now contain union of all three
	count := hll1.PFCount(nil)
	assert.InDelta(t, 150, count, 15, "merged cardinality should be approximately 150")
}

func TestHyperLogLogPFMergeEmpty(t *testing.T) {
	hll1 := NewHyperLogLog()

	for i := 0; i < 50; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	countBefore := hll1.PFCount(nil)

	// Merge empty list
	hll1.PFMerge([]HyperLogLog{})

	countAfter := hll1.PFCount(nil)
	assert.Equal(t, countBefore, countAfter, "merge with empty list should not change count")
}

func TestHyperLogLogPFMergeWithEmptyHLL(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog() // Empty

	for i := 0; i < 50; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	countBefore := hll1.PFCount(nil)

	// Merge with empty HLL
	hll1.PFMerge([]HyperLogLog{hll2})

	countAfter := hll1.PFCount(nil)
	assert.Equal(t, countBefore, countAfter, "merge with empty HLL should not change count")
}

func TestHyperLogLogCacheHit(t *testing.T) {
	hll := NewHyperLogLog()

	for i := 0; i < 100; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	// First count - calculates and caches
	count1 := hll.PFCount(nil)

	// Second count - should use cache (same result)
	count2 := hll.PFCount(nil)

	assert.Equal(t, count1, count2, "cached count should match")
}

func TestHyperLogLogCacheInvalidationOnAdd(t *testing.T) {
	hll := NewHyperLogLog()

	for i := 0; i < 100; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	// Get initial count
	count1 := hll.PFCount(nil)

	// Add more items
	for i := 100; i < 200; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	// Count should be updated
	count2 := hll.PFCount(nil)

	assert.Greater(t, count2, count1, "count should increase after adding more items")
	assert.InDelta(t, 200, count2, 20, "cardinality should be approximately 200")
}

func TestHyperLogLogCacheInvalidationOnMerge(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	for i := 0; i < 100; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 100; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("b%d", i)})
	}

	// Get initial count
	count1 := hll1.PFCount(nil)

	// Merge hll2 into hll1
	hll1.PFMerge([]HyperLogLog{hll2})

	// Count should be updated
	count2 := hll1.PFCount(nil)

	assert.Greater(t, count2, count1, "count should increase after merge")
	assert.InDelta(t, 200, count2, 20, "cardinality should be approximately 200")
}

func TestHyperLogLogCacheNotInvalidatedOnDuplicate(t *testing.T) {
	hll := NewHyperLogLog()

	// Add items
	hll.PFAdd([]string{"item1", "item2", "item3"})

	// Force cache by calling PFCount
	count1 := hll.PFCount(nil)

	// Add duplicate - should NOT invalidate cache (returns 0)
	result, _ := hll.PFAdd([]string{"item1"})
	assert.Equal(t, 0, result, "adding duplicate should return 0")

	// Cache should still be valid
	h := hll.(*hyperLogLog)
	assert.False(t, h.dirty, "cache should still be valid after adding duplicate")

	count2 := hll.PFCount(nil)
	assert.Equal(t, count1, count2)
}

func TestHyperLogLogAccuracySmallSet(t *testing.T) {
	hll := NewHyperLogLog()

	actualCount := 1000
	for i := 0; i < actualCount; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	estimatedCount := hll.PFCount(nil)

	// Standard error is ~0.81%, allow 5% tolerance
	errorRate := math.Abs(float64(estimatedCount-actualCount)) / float64(actualCount)
	assert.Less(t, errorRate, 0.05, "error rate should be less than 5%% for 1000 items, got %.2f%%", errorRate*100)
}

func TestHyperLogLogAccuracyMediumSet(t *testing.T) {
	hll := NewHyperLogLog()

	actualCount := 10000
	for i := 0; i < actualCount; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	estimatedCount := hll.PFCount(nil)

	// Standard error is ~0.81%, allow 3% tolerance for larger sets
	errorRate := math.Abs(float64(estimatedCount-actualCount)) / float64(actualCount)
	assert.Less(t, errorRate, 0.03, "error rate should be less than 3%% for 10000 items, got %.2f%%", errorRate*100)
}

func TestHyperLogLogAccuracyLargeSet(t *testing.T) {
	hll := NewHyperLogLog()

	actualCount := 100000
	for i := 0; i < actualCount; i++ {
		hll.PFAdd([]string{fmt.Sprintf("item%d", i)})
	}

	estimatedCount := hll.PFCount(nil)

	// Standard error is ~0.81%, allow 2% tolerance for large sets
	errorRate := math.Abs(float64(estimatedCount-actualCount)) / float64(actualCount)
	assert.Less(t, errorRate, 0.02, "error rate should be less than 2%% for 100000 items, got %.2f%%", errorRate*100)
}

func TestHyperLogLogSpecialCharacters(t *testing.T) {
	hll := NewHyperLogLog()

	specialItems := []string{
		"hello world",
		"tab\there",
		"newline\nhere",
		"unicode: 你好",
		"emoji: 🎉",
		"null\x00byte",
	}

	result, _ := hll.PFAdd(specialItems)
	assert.Equal(t, 1, result)

	count := hll.PFCount(nil)
	assert.Equal(t, len(specialItems), count)
}

func TestHyperLogLogLongStrings(t *testing.T) {
	hll := NewHyperLogLog()

	// Create a long string
	longString := ""
	for i := 0; i < 1000; i++ {
		longString += "a"
	}

	result, _ := hll.PFAdd([]string{longString})
	assert.Equal(t, 1, result)

	count := hll.PFCount(nil)
	assert.Equal(t, 1, count)

	// Different long string should be counted separately
	differentLongString := longString + "b"
	result, _ = hll.PFAdd([]string{differentLongString})
	assert.Equal(t, 1, result)

	count = hll.PFCount(nil)
	assert.Equal(t, 2, count)
}

func TestHyperLogLogMergeDoesNotAffectSource(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	for i := 0; i < 50; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("b%d", i)})
	}

	count2Before := hll2.PFCount(nil)

	// Merge hll2 into hll1
	hll1.PFMerge([]HyperLogLog{hll2})

	// hll2 should be unchanged
	count2After := hll2.PFCount(nil)
	assert.Equal(t, count2Before, count2After, "source HLL should not be modified by merge")
}

func TestHyperLogLogPFCountDoesNotModifyOtherHLLs(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	for i := 0; i < 50; i++ {
		hll1.PFAdd([]string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		hll2.PFAdd([]string{fmt.Sprintf("b%d", i)})
	}

	count1Before := hll1.PFCount(nil)
	count2Before := hll2.PFCount(nil)

	// Count union
	hll1.PFCount([]HyperLogLog{hll2})

	// Individual counts should be unchanged
	count1After := hll1.PFCount(nil)
	count2After := hll2.PFCount(nil)

	assert.Equal(t, count1Before, count1After, "hll1 should not be modified by PFCount")
	assert.Equal(t, count2Before, count2After, "hll2 should not be modified by PFCount")
}

func TestHyperLogLogMultipleMerges(t *testing.T) {
	hll := NewHyperLogLog()

	// Merge multiple times
	for batch := 0; batch < 5; batch++ {
		other := NewHyperLogLog()
		for i := 0; i < 20; i++ {
			other.PFAdd([]string{fmt.Sprintf("batch%d_item%d", batch, i)})
		}
		hll.PFMerge([]HyperLogLog{other})
	}

	count := hll.PFCount(nil)
	// 5 batches * 20 items = 100 unique items
	assert.InDelta(t, 100, count, 10, "cardinality should be approximately 100")
}

func TestHyperLogLogConstants(t *testing.T) {
	// Verify constants are as expected
	assert.Equal(t, 14, hllP)
	assert.Equal(t, 16384, hllM)
	assert.Equal(t, 51, hllMaxRegisterValue)

	// Alpha should be approximately 0.7213 / (1 + 1.079/16384) ≈ 0.7213
	expectedAlpha := 0.7213 / (1.0 + 1.079/16384.0)
	assert.InDelta(t, expectedAlpha, hllAlpha, 0.0001)
}

func TestHyperLogLogRegisterUpdate(t *testing.T) {
	hll := NewHyperLogLog().(*hyperLogLog)

	// All registers should start at 0
	for i := 0; i < hllM; i++ {
		assert.Equal(t, uint8(0), hll.registers[i], "register %d should be 0 initially", i)
	}

	// After adding items, some registers should be non-zero
	hll.PFAdd([]string{"test"})

	nonZeroCount := 0
	for i := 0; i < hllM; i++ {
		if hll.registers[i] > 0 {
			nonZeroCount++
		}
	}
	assert.Greater(t, nonZeroCount, 0, "at least one register should be non-zero after adding")
}

func TestHyperLogLogMergeMaxValues(t *testing.T) {
	hll1 := NewHyperLogLog().(*hyperLogLog)
	hll2 := NewHyperLogLog().(*hyperLogLog)

	// Manually set some register values
	hll1.registers[0] = 5
	hll1.registers[1] = 10
	hll1.registers[2] = 3

	hll2.registers[0] = 8
	hll2.registers[1] = 7
	hll2.registers[2] = 15

	hll1.dirty = true
	hll2.dirty = true

	// Merge
	hll1.PFMerge([]HyperLogLog{hll2})

	// Should have max values
	assert.Equal(t, uint8(8), hll1.registers[0], "should take max(5, 8) = 8")
	assert.Equal(t, uint8(10), hll1.registers[1], "should take max(10, 7) = 10")
	assert.Equal(t, uint8(15), hll1.registers[2], "should take max(3, 15) = 15")
}

func TestCalculateCardinalityEmpty(t *testing.T) {
	registers := make([]uint8, hllM)
	count := calculateCardinality(registers)
	assert.Equal(t, 0, count, "empty registers should give 0 cardinality")
}

func TestCalculateCardinalityAllSame(t *testing.T) {
	registers := make([]uint8, hllM)
	for i := range registers {
		registers[i] = 5
	}

	count := calculateCardinality(registers)
	// With all registers at 5, estimate should be significant
	assert.Greater(t, count, 0)
}

func TestHyperLogLogWorkflow(t *testing.T) {
	// Simulate a typical workflow
	hll := NewHyperLogLog()

	// Initial state
	assert.Equal(t, 0, hll.PFCount(nil))

	// Add some items
	hll.PFAdd([]string{"user:1", "user:2", "user:3"})
	count1 := hll.PFCount(nil)
	assert.InDelta(t, 3, count1, 1)

	// Add more items including duplicates
	hll.PFAdd([]string{"user:1", "user:4", "user:5"})
	count2 := hll.PFCount(nil)
	assert.InDelta(t, 5, count2, 1)

	// Merge with another HLL
	other := NewHyperLogLog()
	other.PFAdd([]string{"user:6", "user:7", "user:8", "user:9", "user:10"})

	// Get union count without modifying
	unionCount := hll.PFCount([]HyperLogLog{other})
	assert.InDelta(t, 10, unionCount, 2)

	// Original count should be unchanged
	assert.Equal(t, count2, hll.PFCount(nil))

	// Merge permanently
	hll.PFMerge([]HyperLogLog{other})
	assert.InDelta(t, 10, hll.PFCount(nil), 2)
}

func TestHyperLogLogDeterministic(t *testing.T) {
	// Same items should produce same count
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	items := []string{"a", "b", "c", "d", "e"}

	hll1.PFAdd(items)
	hll2.PFAdd(items)

	count1 := hll1.PFCount(nil)
	count2 := hll2.PFCount(nil)

	assert.Equal(t, count1, count2, "same items should produce same count")
}

func TestHyperLogLogOrderIndependent(t *testing.T) {
	// Order of items shouldn't matter
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	hll1.PFAdd([]string{"a", "b", "c", "d", "e"})
	hll2.PFAdd([]string{"e", "d", "c", "b", "a"})

	count1 := hll1.PFCount(nil)
	count2 := hll2.PFCount(nil)

	assert.Equal(t, count1, count2, "order of items should not affect count")
}
