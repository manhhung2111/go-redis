package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPFAdd_NewKey(t *testing.T) {
	s := NewStore().(*store)

	result, err := s.PFAdd("hll", []string{"item1"})
	assert.Equal(t, 1, result, "should return 1 for new key")
	assert.NoError(t, err)

	// Verify HyperLogLog was created
	rObj, exists := s.data.Get("hll")
	require.True(t, exists)
	assert.Equal(t, ObjHyperLogLog, rObj.Type)
	assert.Equal(t, EncHyperLogLog, rObj.Encoding)
}

func TestPFAdd_NewKeyNoItems(t *testing.T) {
	s := NewStore().(*store)

	// Creating empty HLL should return 1 (key was created)
	result, err := s.PFAdd("hll", []string{})
	assert.Equal(t, 1, result, "should return 1 when creating new key even with no items")
	assert.NoError(t, err)

	// Verify HyperLogLog was created
	rObj, exists := s.data.Get("hll")
	require.True(t, exists)
	assert.Equal(t, ObjHyperLogLog, rObj.Type)
}

func TestPFAdd_ExistingKeyNoItems(t *testing.T) {
	s := NewStore().(*store)

	// Create HLL first
	s.PFAdd("hll", []string{"item1"})

	// Adding no items to existing key should return 0
	result, err := s.PFAdd("hll", []string{})
	assert.Equal(t, 0, result, "should return 0 when adding no items to existing key")
	assert.NoError(t, err)
}

func TestPFAdd_ExistingKeyNewItem(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})

	// Adding new item should return 1 (register updated)
	result, err := s.PFAdd("hll", []string{"item2"})
	assert.Equal(t, 1, result, "should return 1 for new item")
	assert.NoError(t, err)
}

func TestPFAdd_ExistingKeyDuplicateItem(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})

	// Adding same item again should return 0 (no register updated)
	result, err := s.PFAdd("hll", []string{"item1"})
	assert.Equal(t, 0, result, "should return 0 for duplicate item")
	assert.NoError(t, err)
}

func TestPFAdd_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	result, err := s.PFAdd("hll", items)
	assert.Equal(t, 1, result, "should return 1 when registers updated")
	assert.NoError(t, err)

	// Verify count is approximately correct
	count, err := s.PFCount([]string{"hll"})
	require.NoError(t, err)
	assert.InDelta(t, len(items), count, 1)
}

func TestPFAdd_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	_, err := s.PFAdd("mykey", []string{"item"})
	assert.Error(t, err)
}

func TestPFAdd_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	// Create HLL and set it as expired
	s.PFAdd("hll", []string{"old_item"})
	s.expires.Set("hll", 1) // expired timestamp

	// Add new item - should create new HLL since old one expired
	result, err := s.PFAdd("hll", []string{"new_item"})
	assert.Equal(t, 1, result)
	assert.NoError(t, err)

	// Verify new HLL only has new item
	count, err := s.PFCount([]string{"hll"})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestPFCount_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	count, err := s.PFCount([]string{"nonexistent"})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestPFCount_EmptyKeys(t *testing.T) {
	s := NewStore().(*store)

	count, err := s.PFCount([]string{})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestPFCount_SingleKey(t *testing.T) {
	s := NewStore().(*store)

	// Add 100 unique items
	for i := 0; i < 100; i++ {
		s.PFAdd("hll", []string{fmt.Sprintf("item%d", i)})
	}

	count, err := s.PFCount([]string{"hll"})
	require.NoError(t, err)
	assert.InDelta(t, 100, count, 10, "count should be approximately 100")
}

func TestPFCount_MultipleKeys(t *testing.T) {
	s := NewStore().(*store)

	// Add different items to each HLL
	for i := 0; i < 50; i++ {
		s.PFAdd("hll1", []string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		s.PFAdd("hll2", []string{fmt.Sprintf("b%d", i)})
	}
	for i := 0; i < 50; i++ {
		s.PFAdd("hll3", []string{fmt.Sprintf("c%d", i)})
	}

	// Count union of all three
	count, err := s.PFCount([]string{"hll1", "hll2", "hll3"})
	require.NoError(t, err)
	assert.InDelta(t, 150, count, 15, "union count should be approximately 150")
}

func TestPFCount_MultipleKeysWithOverlap(t *testing.T) {
	s := NewStore().(*store)

	// Add overlapping items
	for i := 0; i < 100; i++ {
		s.PFAdd("hll1", []string{fmt.Sprintf("item%d", i)})
	}
	for i := 50; i < 150; i++ {
		s.PFAdd("hll2", []string{fmt.Sprintf("item%d", i)})
	}

	// Union should be ~150 (items 0-149)
	count, err := s.PFCount([]string{"hll1", "hll2"})
	require.NoError(t, err)
	assert.InDelta(t, 150, count, 15)
}

func TestPFCount_MixedExistingAndNonExisting(t *testing.T) {
	s := NewStore().(*store)

	for i := 0; i < 50; i++ {
		s.PFAdd("hll1", []string{fmt.Sprintf("item%d", i)})
	}

	// Include non-existing key
	count, err := s.PFCount([]string{"hll1", "nonexistent", "alsononexistent"})
	require.NoError(t, err)
	assert.InDelta(t, 50, count, 5)
}

func TestPFCount_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	_, err := s.PFCount([]string{"mykey"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}

func TestPFCount_OneWrongType(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})
	s.Set("mykey", "value")

	// Should fail because one key is wrong type
	_, err := s.PFCount([]string{"hll", "mykey"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}

func TestPFCount_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})
	s.expires.Set("hll", 1) // expired

	count, err := s.PFCount([]string{"hll"})
	require.NoError(t, err)
	assert.Equal(t, 0, count, "expired key should return 0")
}

func TestPFCount_DoesNotModifyHLLs(t *testing.T) {
	s := NewStore().(*store)

	for i := 0; i < 50; i++ {
		s.PFAdd("hll1", []string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		s.PFAdd("hll2", []string{fmt.Sprintf("b%d", i)})
	}

	count1Before, _ := s.PFCount([]string{"hll1"})
	count2Before, _ := s.PFCount([]string{"hll2"})

	// Get union count
	s.PFCount([]string{"hll1", "hll2"})

	// Individual counts should be unchanged
	count1After, _ := s.PFCount([]string{"hll1"})
	count2After, _ := s.PFCount([]string{"hll2"})

	assert.Equal(t, count1Before, count1After)
	assert.Equal(t, count2Before, count2After)
}

func TestPFMerge_NewDestKey(t *testing.T) {
	s := NewStore().(*store)

	for i := 0; i < 50; i++ {
		s.PFAdd("src1", []string{fmt.Sprintf("a%d", i)})
	}
	for i := 0; i < 50; i++ {
		s.PFAdd("src2", []string{fmt.Sprintf("b%d", i)})
	}

	err := s.PFMerge("dest", []string{"src1", "src2"})
	require.NoError(t, err)

	// Verify dest was created
	rObj, exists := s.data.Get("dest")
	require.True(t, exists)
	assert.Equal(t, ObjHyperLogLog, rObj.Type)

	// Verify count
	count, _ := s.PFCount([]string{"dest"})
	assert.InDelta(t, 100, count, 10)
}

func TestPFMerge_ExistingDestKey(t *testing.T) {
	s := NewStore().(*store)

	// Create dest with some items
	for i := 0; i < 30; i++ {
		s.PFAdd("dest", []string{fmt.Sprintf("d%d", i)})
	}

	// Create source
	for i := 0; i < 50; i++ {
		s.PFAdd("src", []string{fmt.Sprintf("s%d", i)})
	}

	err := s.PFMerge("dest", []string{"src"})
	require.NoError(t, err)

	// Dest should now contain union
	count, _ := s.PFCount([]string{"dest"})
	assert.InDelta(t, 80, count, 10)
}

func TestPFMerge_EmptySourceKeys(t *testing.T) {
	s := NewStore().(*store)

	// Merge with no sources - should just create empty dest
	err := s.PFMerge("dest", []string{})
	require.NoError(t, err)

	// Dest should exist but be empty
	rObj, exists := s.data.Get("dest")
	require.True(t, exists)
	assert.Equal(t, ObjHyperLogLog, rObj.Type)

	count, _ := s.PFCount([]string{"dest"})
	assert.Equal(t, 0, count)
}

func TestPFMerge_NonExistingSources(t *testing.T) {
	s := NewStore().(*store)

	// Create one source
	s.PFAdd("src1", []string{"item1", "item2"})

	// Merge including non-existing sources
	err := s.PFMerge("dest", []string{"src1", "nonexistent1", "nonexistent2"})
	require.NoError(t, err)

	// Should only have data from src1
	count, _ := s.PFCount([]string{"dest"})
	assert.InDelta(t, 2, count, 1)
}

func TestPFMerge_DestIsAlsoSource(t *testing.T) {
	s := NewStore().(*store)

	// Create dest with items
	for i := 0; i < 50; i++ {
		s.PFAdd("hll", []string{fmt.Sprintf("item%d", i)})
	}

	// Create another source
	for i := 50; i < 100; i++ {
		s.PFAdd("src", []string{fmt.Sprintf("item%d", i)})
	}

	// Merge into itself + another source
	err := s.PFMerge("hll", []string{"hll", "src"})
	require.NoError(t, err)

	// Should have union of both
	count, _ := s.PFCount([]string{"hll"})
	assert.InDelta(t, 100, count, 10)
}

func TestPFMerge_WrongTypeInDest(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	err := s.PFMerge("mykey", []string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}

func TestPFMerge_WrongTypeInSource(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})
	s.Set("mykey", "value")

	err := s.PFMerge("dest", []string{"hll", "mykey"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}

func TestPFMerge_DoesNotModifySources(t *testing.T) {
	s := NewStore().(*store)

	for i := 0; i < 50; i++ {
		s.PFAdd("src", []string{fmt.Sprintf("item%d", i)})
	}

	countBefore, _ := s.PFCount([]string{"src"})

	err := s.PFMerge("dest", []string{"src"})
	require.NoError(t, err)

	countAfter, _ := s.PFCount([]string{"src"})
	assert.Equal(t, countBefore, countAfter, "source should not be modified")
}

func TestPFMerge_ExpiredDestKey(t *testing.T) {
	s := NewStore().(*store)

	// Create dest and expire it
	s.PFAdd("dest", []string{"old_item"})
	s.expires.Set("dest", 1) // expired

	// Create source
	s.PFAdd("src", []string{"new_item"})

	err := s.PFMerge("dest", []string{"src"})
	require.NoError(t, err)

	// Dest should be recreated with only source data
	count, _ := s.PFCount([]string{"dest"})
	assert.Equal(t, 1, count)
}

func TestPFMerge_ExpiredSourceKey(t *testing.T) {
	s := NewStore().(*store)

	// Create and expire source
	s.PFAdd("src", []string{"item1"})
	s.expires.Set("src", 1) // expired

	err := s.PFMerge("dest", []string{"src"})
	require.NoError(t, err)

	// Dest should be empty (expired source ignored)
	count, _ := s.PFCount([]string{"dest"})
	assert.Equal(t, 0, count)
}

func TestGetHyperLogLog_NonExisting(t *testing.T) {
	s := NewStore().(*store)

	hll, err := s.getHyperLogLog("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, hll)
}

func TestGetHyperLogLog_Existing(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})

	hll, err := s.getHyperLogLog("hll")
	require.NoError(t, err)
	require.NotNil(t, hll)
}

func TestGetHyperLogLog_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	hll, err := s.getHyperLogLog("mykey")
	assert.Error(t, err)
	assert.Nil(t, hll)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}

func TestGetHyperLogLog_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.PFAdd("hll", []string{"item1"})
	s.expires.Set("hll", 1) // expired

	hll, err := s.getHyperLogLog("hll")
	require.NoError(t, err)
	assert.Nil(t, hll, "expired key should return nil")
}

func TestHyperLogLog_FullWorkflow(t *testing.T) {
	s := NewStore().(*store)

	// Add items to multiple HLLs
	users := []string{"user:1", "user:2", "user:3", "user:4", "user:5"}
	for _, user := range users {
		s.PFAdd("page:home", []string{user})
	}

	moreUsers := []string{"user:3", "user:4", "user:5", "user:6", "user:7"}
	for _, user := range moreUsers {
		s.PFAdd("page:about", []string{user})
	}

	// Count individual pages
	homeCount, _ := s.PFCount([]string{"page:home"})
	aboutCount, _ := s.PFCount([]string{"page:about"})
	assert.InDelta(t, 5, homeCount, 1)
	assert.InDelta(t, 5, aboutCount, 1)

	// Count union (unique visitors across both pages)
	totalCount, _ := s.PFCount([]string{"page:home", "page:about"})
	assert.InDelta(t, 7, totalCount, 2) // users 1-7

	// Merge into total
	err := s.PFMerge("page:total", []string{"page:home", "page:about"})
	require.NoError(t, err)

	mergedCount, _ := s.PFCount([]string{"page:total"})
	assert.InDelta(t, 7, mergedCount, 2)
}

func TestHyperLogLog_LargeDataset(t *testing.T) {
	s := NewStore().(*store)

	// Add 10000 unique items
	for i := 0; i < 10000; i++ {
		s.PFAdd("hll", []string{fmt.Sprintf("item%d", i)})
	}

	count, err := s.PFCount([]string{"hll"})
	require.NoError(t, err)

	// HLL has ~0.81% standard error, allow 3% tolerance
	errorRate := float64(count-10000) / 10000.0
	if errorRate < 0 {
		errorRate = -errorRate
	}
	assert.Less(t, errorRate, 0.03, "error rate should be less than 3%%")
}

func TestHyperLogLog_MultipleMerges(t *testing.T) {
	s := NewStore().(*store)

	// Create 5 HLLs with 20 items each
	for batch := 0; batch < 5; batch++ {
		key := fmt.Sprintf("hll%d", batch)
		for i := 0; i < 20; i++ {
			s.PFAdd(key, []string{fmt.Sprintf("batch%d_item%d", batch, i)})
		}
	}

	// Merge all into one
	err := s.PFMerge("merged", []string{"hll0", "hll1", "hll2", "hll3", "hll4"})
	require.NoError(t, err)

	count, _ := s.PFCount([]string{"merged"})
	assert.InDelta(t, 100, count, 10)
}

func TestHyperLogLog_Deterministic(t *testing.T) {
	s1 := NewStore().(*store)
	s2 := NewStore().(*store)

	items := []string{"a", "b", "c", "d", "e"}

	s1.PFAdd("hll", items)
	s2.PFAdd("hll", items)

	count1, _ := s1.PFCount([]string{"hll"})
	count2, _ := s2.PFCount([]string{"hll"})

	assert.Equal(t, count1, count2, "same items should produce same count")
}
