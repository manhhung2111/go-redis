package data_structure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCuckooFilter(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)
	require.NotNil(t, cf)

	info := cf.Info()
	// Should start with one filter
	assert.Equal(t, 1, info[5]) // Number of filters
	// Should have correct expansion rate
	assert.Equal(t, 2, info[13]) // Expansion rate
	// Should have correct bucket size
	assert.Equal(t, uint64(4), info[11]) // Bucket size
	// Should have correct max iterations
	assert.Equal(t, uint64(500), info[15]) // Max iterations
}

func TestNewCuckooFilterDefaults(t *testing.T) {
	// All zeros should use defaults
	cf := NewCuckooFilter(0, 0, 0, 0)
	require.NotNil(t, cf)

	info := cf.Info()
	assert.Equal(t, uint64(defaultBucketSize), info[11])       // Bucket size
	assert.Equal(t, defaultCuckooExpansionRate, info[13])      // Expansion rate
	assert.Equal(t, uint64(defaultMaxKicks), info[15])         // Max iterations
}

func TestNewCuckooFilterNegativeExpansionRate(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, -5)
	require.NotNil(t, cf)

	info := cf.Info()
	assert.Equal(t, defaultCuckooExpansionRate, info[13])
}

func TestCuckooFilterAdd(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add new item
	result := cf.Add("item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Add same item again (cuckoo filter allows duplicates)
	result = cf.Add("item1")
	assert.Equal(t, 1, result, "should return 1 for duplicate item (allowed)")

	// Add another new item
	result = cf.Add("item2")
	assert.Equal(t, 1, result, "should return 1 for new item")
}

func TestCuckooFilterAddMultipleItems(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, item := range items {
		result := cf.Add(item)
		assert.Equal(t, 1, result, "should return 1 for new item: %s", item)
	}

	// Verify all items exist
	for _, item := range items {
		assert.Equal(t, 1, cf.Exists(item), "item should exist: %s", item)
	}
}

func TestCuckooFilterAddNx(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add new item
	result := cf.AddNx("item1")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Try to add same item again
	result = cf.AddNx("item1")
	assert.Equal(t, 0, result, "should return 0 for existing item")

	// Add another new item
	result = cf.AddNx("item2")
	assert.Equal(t, 1, result, "should return 1 for new item")

	// Verify both exist
	assert.Equal(t, 1, cf.Exists("item1"))
	assert.Equal(t, 1, cf.Exists("item2"))
}

func TestCuckooFilterAddNxMultiple(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	items := []string{"a", "b", "c", "d"}
	for _, item := range items {
		result := cf.AddNx(item)
		assert.Equal(t, 1, result, "should return 1 for new item: %s", item)
	}

	// Add again - all should return 0
	for _, item := range items {
		result := cf.AddNx(item)
		assert.Equal(t, 0, result, "should return 0 for existing item: %s", item)
	}
}

func TestCuckooFilterExists(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Non-existing item
	result := cf.Exists("nonexistent")
	assert.Equal(t, 0, result, "should return 0 for non-existing item")

	// Add item and check existence
	cf.Add("exists")
	result = cf.Exists("exists")
	assert.Equal(t, 1, result, "should return 1 for existing item")

	// Check non-existing item again
	result = cf.Exists("still_nonexistent")
	assert.Equal(t, 0, result, "should return 0 for non-existing item")
}

func TestCuckooFilterExistsEmptyFilter(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Empty filter should not contain anything
	assert.Equal(t, 0, cf.Exists("anything"))
	assert.Equal(t, 0, cf.Exists(""))
	assert.Equal(t, 0, cf.Exists("test"))
}

func TestCuckooFilterMExists(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add some items
	cf.Add("item1")
	cf.Add("item2")
	cf.Add("item3")

	// Check multiple items
	items := []string{"item1", "item4", "item2", "item5", "item3"}
	results := cf.MExists(items)

	expected := []int{1, 0, 1, 0, 1}
	assert.Equal(t, expected, results)
}

func TestCuckooFilterMExistsEmpty(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	results := cf.MExists([]string{})
	assert.Empty(t, results)
}

func TestCuckooFilterMExistsEmptyFilter(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	items := []string{"a", "b", "c"}
	results := cf.MExists(items)

	expected := []int{0, 0, 0}
	assert.Equal(t, expected, results)
}

func TestCuckooFilterDel(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add and delete
	cf.Add("item1")
	assert.Equal(t, 1, cf.Exists("item1"))

	result := cf.Del("item1")
	assert.Equal(t, 1, result, "should return 1 for successful deletion")
	assert.Equal(t, 0, cf.Exists("item1"), "item should not exist after deletion")
}

func TestCuckooFilterDelNonExistent(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Try to delete non-existent item
	result := cf.Del("nonexistent")
	assert.Equal(t, 0, result, "should return 0 for non-existent item")
}

func TestCuckooFilterDelMultiple(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add items
	items := []string{"a", "b", "c", "d", "e"}
	for _, item := range items {
		cf.Add(item)
	}

	// Delete some items
	assert.Equal(t, 1, cf.Del("b"))
	assert.Equal(t, 1, cf.Del("d"))

	// Verify state
	assert.Equal(t, 1, cf.Exists("a"))
	assert.Equal(t, 0, cf.Exists("b"))
	assert.Equal(t, 1, cf.Exists("c"))
	assert.Equal(t, 0, cf.Exists("d"))
	assert.Equal(t, 1, cf.Exists("e"))
}

func TestCuckooFilterDelAndReAdd(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add, delete, re-add
	cf.Add("item1")
	assert.Equal(t, 1, cf.Exists("item1"))

	cf.Del("item1")
	assert.Equal(t, 0, cf.Exists("item1"))

	cf.Add("item1")
	assert.Equal(t, 1, cf.Exists("item1"))
}

func TestCuckooFilterCount(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Empty filter
	assert.Equal(t, 0, cf.Count("item1"))

	// After adding once
	cf.Add("item1")
	assert.Equal(t, 1, cf.Count("item1"))

	// After adding twice (cuckoo filter allows duplicates with Add)
	cf.Add("item1")
	assert.Equal(t, 2, cf.Count("item1"))

	// After deleting once
	cf.Del("item1")
	assert.Equal(t, 1, cf.Count("item1"))

	// After deleting again
	cf.Del("item1")
	assert.Equal(t, 0, cf.Count("item1"))
}

func TestCuckooFilterCountMultipleItems(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	cf.Add("a")
	cf.Add("a")
	cf.Add("a")
	cf.Add("b")
	cf.Add("b")
	cf.Add("c")

	assert.Equal(t, 3, cf.Count("a"))
	assert.Equal(t, 2, cf.Count("b"))
	assert.Equal(t, 1, cf.Count("c"))
	assert.Equal(t, 0, cf.Count("d"))
}

func TestCuckooFilterInfo(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)
	cf.Add("item1")
	cf.Add("item2")
	cf.Del("item1")

	info := cf.Info()
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
	assert.Equal(t, uint64(4), info[11])
	assert.Equal(t, "Expansion rate", info[12])
	assert.Equal(t, 2, info[13])
	assert.Equal(t, "Max iterations", info[14])
	assert.Equal(t, uint64(500), info[15])
}

func TestCuckooFilterInfoSize(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	info := cf.Info()
	size := info[1].(uint64)
	assert.Greater(t, size, uint64(0))
}

func TestCuckooFilterInfoNumBuckets(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	info := cf.Info()
	numBuckets := info[3].(uint64)
	assert.Greater(t, numBuckets, uint64(0))
}

func TestCuckooFilterAutoScaling(t *testing.T) {
	// Small capacity to trigger scaling
	cf := NewCuckooFilter(10, 4, 500, 2)

	// Initially should have 1 filter
	info := cf.Info()
	assert.Equal(t, 1, info[5])

	// Add items to exceed first filter capacity
	// With capacity 10, bucketSize 4: numBuckets = ceil(10/4) = 3
	// Total slots = 3 * 4 = 12
	for i := 0; i < 20; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// Should have scaled to more filters
	info = cf.Info()
	numFilters := info[5].(int)
	assert.GreaterOrEqual(t, numFilters, 2, "should have at least 2 filters")

	// Verify all items still exist
	for i := 0; i < 20; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)), "item%d should exist", i)
	}
}

func TestCuckooFilterMultipleScaling(t *testing.T) {
	// Very small capacity to trigger multiple scalings
	cf := NewCuckooFilter(5, 4, 500, 2)

	// Add many items
	for i := 0; i < 50; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// Should have multiple filters
	info := cf.Info()
	numFilters := info[5].(int)
	assert.GreaterOrEqual(t, numFilters, 2, "should have at least 2 filters")

	// Verify all items exist
	for i := 0; i < 50; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)), "item%d should exist", i)
	}
}

func TestCuckooFilterExistsAcrossFilters(t *testing.T) {
	// Small capacity to force multiple filters
	cf := NewCuckooFilter(5, 4, 500, 2)

	// Add items that will span multiple filters
	items := make([]string, 30)
	for i := 0; i < 30; i++ {
		items[i] = fmt.Sprintf("item%d", i)
		cf.Add(items[i])
	}

	// All items should be found regardless of which filter they're in
	for _, item := range items {
		assert.Equal(t, 1, cf.Exists(item), "item should exist: %s", item)
	}
}

func TestCuckooFilterDelAcrossFilters(t *testing.T) {
	cf := NewCuckooFilter(5, 4, 500, 2)

	// Add items across multiple filters
	for i := 0; i < 30; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// Delete some items
	for i := 0; i < 30; i += 3 {
		result := cf.Del(fmt.Sprintf("item%d", i))
		assert.Equal(t, 1, result)
	}

	// Verify deletions
	for i := 0; i < 30; i++ {
		if i%3 == 0 {
			assert.Equal(t, 0, cf.Exists(fmt.Sprintf("item%d", i)), "item%d should be deleted", i)
		} else {
			assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)), "item%d should exist", i)
		}
	}
}

func TestCuckooFilterEmptyString(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Empty string should work
	result := cf.Add("")
	assert.Equal(t, 1, result)

	assert.Equal(t, 1, cf.Exists(""))
	assert.Equal(t, 1, cf.Count(""))

	// Delete
	result = cf.Del("")
	assert.Equal(t, 1, result)
	assert.Equal(t, 0, cf.Exists(""))
}

func TestCuckooFilterSpecialCharacters(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	specialItems := []string{
		"hello world",
		"tab\there",
		"newline\nhere",
		"unicode: 你好",
		"emoji: 🎉",
		"null\x00byte",
	}

	for _, item := range specialItems {
		result := cf.Add(item)
		assert.Equal(t, 1, result, "should add: %q", item)
	}

	for _, item := range specialItems {
		assert.Equal(t, 1, cf.Exists(item), "should exist: %q", item)
	}

	for _, item := range specialItems {
		result := cf.Del(item)
		assert.Equal(t, 1, result, "should delete: %q", item)
		assert.Equal(t, 0, cf.Exists(item), "should not exist after delete: %q", item)
	}
}

func TestCuckooFilterLongStrings(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Create a long string
	longString := ""
	for i := 0; i < 1000; i++ {
		longString += "a"
	}

	result := cf.Add(longString)
	assert.Equal(t, 1, result)

	assert.Equal(t, 1, cf.Exists(longString))

	// Similar but different long string
	differentLongString := longString + "b"
	assert.Equal(t, 0, cf.Exists(differentLongString))

	// Delete works
	result = cf.Del(longString)
	assert.Equal(t, 1, result)
	assert.Equal(t, 0, cf.Exists(longString))
}

func TestCuckooFilterSmallCapacity(t *testing.T) {
	// Minimum capacity
	cf := NewCuckooFilter(1, 4, 500, 2)
	require.NotNil(t, cf)

	// Should work with capacity 1
	cf.Add("item1")
	assert.Equal(t, 1, cf.Exists("item1"))

	// Adding more items should trigger scaling
	cf.Add("item2")
	assert.Equal(t, 1, cf.Exists("item2"))
}

func TestCuckooFilterLargeBucketSize(t *testing.T) {
	cf := NewCuckooFilter(100, 8, 500, 2)
	require.NotNil(t, cf)

	info := cf.Info()
	assert.Equal(t, uint64(8), info[11])

	// Should work normally
	for i := 0; i < 50; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	for i := 0; i < 50; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilterExpansionRateOne(t *testing.T) {
	// Expansion rate of 1 means each filter has same capacity
	// Use larger capacity to ensure items fit
	cf := NewCuckooFilter(100, 4, 500, 1)

	info := cf.Info()
	assert.Equal(t, 1, info[13])

	// Add items - with expansion rate 1, each new filter has same capacity
	for i := 0; i < 50; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// All items should still exist
	for i := 0; i < 50; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilterHighExpansionRate(t *testing.T) {
	// Use larger initial capacity for stability
	cf := NewCuckooFilter(200, 4, 500, 4)

	info := cf.Info()
	assert.Equal(t, 4, info[13])

	// Add items
	for i := 0; i < 100; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// All items should exist
	for i := 0; i < 100; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilterItemCountTracking(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Initially 0
	info := cf.Info()
	assert.Equal(t, uint64(0), info[7])  // Items inserted
	assert.Equal(t, uint64(0), info[9])  // Items deleted

	// Add items
	cf.Add("a")
	cf.Add("b")
	cf.Add("c")

	info = cf.Info()
	assert.Equal(t, uint64(3), info[7])

	// Delete one
	cf.Del("b")

	info = cf.Info()
	assert.Equal(t, uint64(2), info[7])  // 3 - 1 = 2
	assert.Equal(t, uint64(1), info[9])  // 1 deletion
}

func TestBucketInsertToBucket(t *testing.T) {
	b := bucket{fingerprints: make([]uint16, 4)}

	// Insert into empty bucket
	assert.True(t, b.insertToBucket(100))
	assert.True(t, b.insertToBucket(200))
	assert.True(t, b.insertToBucket(300))
	assert.True(t, b.insertToBucket(400))

	// Bucket is now full
	assert.False(t, b.insertToBucket(500))
}

func TestBucketDeleteFromBucket(t *testing.T) {
	b := bucket{fingerprints: make([]uint16, 4)}

	b.insertToBucket(100)
	b.insertToBucket(200)

	// Delete existing
	assert.True(t, b.deleteFromBucket(100))
	// Delete again should fail
	assert.False(t, b.deleteFromBucket(100))

	// Delete other existing
	assert.True(t, b.deleteFromBucket(200))

	// Delete non-existing
	assert.False(t, b.deleteFromBucket(300))
}

func TestBucketContainsFingerprint(t *testing.T) {
	b := bucket{fingerprints: make([]uint16, 4)}

	assert.False(t, b.containsFingerprint(100))

	b.insertToBucket(100)
	assert.True(t, b.containsFingerprint(100))
	assert.False(t, b.containsFingerprint(200))

	b.deleteFromBucket(100)
	assert.False(t, b.containsFingerprint(100))
}

func TestBucketCountFingerprint(t *testing.T) {
	b := bucket{fingerprints: make([]uint16, 4)}

	assert.Equal(t, 0, b.countFingerprint(100))

	b.fingerprints[0] = 100
	assert.Equal(t, 1, b.countFingerprint(100))

	b.fingerprints[2] = 100
	assert.Equal(t, 2, b.countFingerprint(100))

	b.fingerprints[1] = 200
	assert.Equal(t, 2, b.countFingerprint(100))
	assert.Equal(t, 1, b.countFingerprint(200))
}

func TestGetFingerprint(t *testing.T) {
	// Should return non-zero fingerprint
	fp := getFingerprint("test")
	assert.NotEqual(t, uint16(0), fp)

	// Same item should produce same fingerprint
	fp2 := getFingerprint("test")
	assert.Equal(t, fp, fp2)

	// Different items should (likely) produce different fingerprints
	fp3 := getFingerprint("different")
	// Note: Could collide, but extremely unlikely
	assert.NotEqual(t, fp, fp3)
}

func TestSubCuckooFilterGetBucketIndex(t *testing.T) {
	f := &subCuckooFilter{
		buckets:    make([]bucket, 10),
		numBuckets: 10,
		bucketSize: 4,
	}

	idx := f.getBucketIndex("test")
	assert.Less(t, idx, uint64(10))

	// Same item should produce same index
	idx2 := f.getBucketIndex("test")
	assert.Equal(t, idx, idx2)
}

func TestSubCuckooFilterGetAltBucketIndex(t *testing.T) {
	// Use a power-of-2 number of buckets for cleaner XOR behavior
	f := &subCuckooFilter{
		buckets:    make([]bucket, 1024),
		numBuckets: 1024,
		bucketSize: 4,
	}

	fp := uint16(12345)
	idx := uint64(3)

	altIdx := f.getAltBucketIndex(idx, fp)
	assert.Less(t, altIdx, uint64(1024))

	// XOR property: altIndex of altIndex should give back original
	// Note: This property holds when numBuckets is a power of 2
	originalIdx := f.getAltBucketIndex(altIdx, fp)
	assert.Equal(t, idx, originalIdx)
}

func TestSubCuckooFilterInsert(t *testing.T) {
	f := &subCuckooFilter{
		buckets:    make([]bucket, 10),
		numBuckets: 10,
		bucketSize: 4,
		maxKicks:   500,
	}
	for i := range f.buckets {
		f.buckets[i] = bucket{fingerprints: make([]uint16, 4)}
	}

	// Should insert successfully
	assert.True(t, f.insert(0, 100))
	assert.True(t, f.insert(0, 200))
}

func TestSubCuckooFilterExistsInSubFilter(t *testing.T) {
	f := &subCuckooFilter{
		buckets:    make([]bucket, 100),
		numBuckets: 100,
		bucketSize: 4,
		maxKicks:   500,
	}
	for i := range f.buckets {
		f.buckets[i] = bucket{fingerprints: make([]uint16, 4)}
	}

	// Not in filter
	assert.False(t, f.existsInSubFilter("test"))

	// Add and check
	fp := getFingerprint("test")
	bucketIdx := f.getBucketIndex("test")
	f.insert(bucketIdx, fp)

	assert.True(t, f.existsInSubFilter("test"))
}

func TestCuckooFilterFalsePositiveRate(t *testing.T) {
	// Create a reasonably sized filter
	cf := NewCuckooFilter(10000, 4, 500, 2)

	// Add items
	for i := 0; i < 5000; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	// Check for false positives with items that were never added
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		// Use a different prefix to ensure these weren't added
		if cf.Exists(fmt.Sprintf("nonexistent%d", i)) == 1 {
			falsePositives++
		}
	}

	// With 16-bit fingerprints and bucket size 4, theoretical FPR ≈ 8/65536 ≈ 0.012%
	// Allow generous margin for statistical variation
	actualRate := float64(falsePositives) / float64(testCount)
	assert.Less(t, actualRate, 0.05, "false positive rate too high: %f", actualRate)
}

func TestCuckooFilterNoFalseNegatives(t *testing.T) {
	cf := NewCuckooFilter(1000, 4, 500, 2)

	// Add items
	items := make([]string, 500)
	for i := 0; i < 500; i++ {
		items[i] = fmt.Sprintf("item%d", i)
		cf.Add(items[i])
	}

	// All added items must be found (no false negatives)
	for _, item := range items {
		assert.Equal(t, 1, cf.Exists(item), "false negative for: %s", item)
	}
}

func TestCuckooFilterDeleteDoesNotAffectOthers(t *testing.T) {
	cf := NewCuckooFilter(100, 4, 500, 2)

	// Add multiple items
	cf.Add("item1")
	cf.Add("item2")
	cf.Add("item3")

	// Delete one
	cf.Del("item2")

	// Others should still exist
	assert.Equal(t, 1, cf.Exists("item1"))
	assert.Equal(t, 0, cf.Exists("item2"))
	assert.Equal(t, 1, cf.Exists("item3"))
}

func TestCuckooFilterManyFilters(t *testing.T) {
	// Create many filters by using small capacity
	cf := NewCuckooFilter(2, 4, 500, 2)

	// Add enough items to create multiple filters
	for i := 0; i < 200; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	info := cf.Info()
	numFilters := info[5].(int)
	assert.GreaterOrEqual(t, numFilters, 3)

	// All items should still exist
	for i := 0; i < 200; i++ {
		assert.Equal(t, 1, cf.Exists(fmt.Sprintf("item%d", i)))
	}
}

func TestCuckooFilterSizeGrowsWithItems(t *testing.T) {
	cf := NewCuckooFilter(10, 4, 500, 2)

	initialInfo := cf.Info()
	initialSize := initialInfo[1].(uint64)

	// Add many items to trigger scaling
	for i := 0; i < 100; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	newInfo := cf.Info()
	newSize := newInfo[1].(uint64)

	assert.Greater(t, newSize, initialSize)
}

func TestCuckooFilterBucketsGrowWithScaling(t *testing.T) {
	cf := NewCuckooFilter(10, 4, 500, 2)

	initialInfo := cf.Info()
	initialBuckets := initialInfo[3].(uint64)

	// Add many items to trigger scaling
	for i := 0; i < 100; i++ {
		cf.Add(fmt.Sprintf("item%d", i))
	}

	newInfo := cf.Info()
	newBuckets := newInfo[3].(uint64)

	assert.Greater(t, newBuckets, initialBuckets)
}
