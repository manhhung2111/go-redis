package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCMSInitByDim_NewKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Verify CMS was created
	rObj, exists := s.data["cms"]
	require.True(t, exists)
	assert.Equal(t, ObjCountMinSketch, rObj.Type)
	assert.Equal(t, EncCountMinSketch, rObj.Encoding)

	// Verify dimensions via Info
	info := s.CMSInfo("cms")
	assert.Equal(t, "width", info[0])
	assert.Equal(t, 100, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 5, info[3])
}

func TestCMSInitByDim_KeyExists(t *testing.T) {
	s := NewStore().(*store)

	// Create CMS
	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Try to create again - should fail
	err = s.CMSInitByDim("cms", 200, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key already exists")

	// Original dimensions should be preserved
	info := s.CMSInfo("cms")
	assert.Equal(t, 100, info[1])
	assert.Equal(t, 5, info[3])
}

func TestCMSInitByDim_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	// Create CMS and expire it
	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires["cms"] = 1 // expired timestamp

	// Should succeed since key expired
	err = s.CMSInitByDim("cms", 200, 10)
	require.NoError(t, err)

	// New dimensions should be set
	info := s.CMSInfo("cms")
	assert.Equal(t, 200, info[1])
	assert.Equal(t, 10, info[3])
}

func TestCMSInitByProb_NewKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)

	// Verify CMS was created
	rObj, exists := s.data["cms"]
	require.True(t, exists)
	assert.Equal(t, ObjCountMinSketch, rObj.Type)
	assert.Equal(t, EncCountMinSketch, rObj.Encoding)

	// Verify dimensions via Info
	// errorRate = 0.01 -> width = ceil(e / 0.01) = 272
	// probability = 0.01 -> depth = ceil(ln(100)) = 5
	info := s.CMSInfo("cms")
	assert.Equal(t, 272, info[1])
	assert.Equal(t, 5, info[3])
}

func TestCMSInitByProb_KeyExists(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)

	// Try to create again - should fail
	err = s.CMSInitByProb("cms", 0.1, 0.1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key already exists")
}

func TestCMSInitByProb_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)
	s.expires["cms"] = 1 // expired

	// Should succeed since key expired
	err = s.CMSInitByProb("cms", 0.1, 0.1)
	require.NoError(t, err)
}

func TestCMSIncrBy_SingleItem(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result := s.CMSIncrBy("cms", map[string]uint64{"item1": 1})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(1), result[0])
}

func TestCMSIncrBy_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	items := map[string]uint64{
		"apple":  5,
		"banana": 10,
		"cherry": 15,
	}

	result := s.CMSIncrBy("cms", items)
	require.Len(t, result, 3)

	// All results should be at least the increment value
	for _, count := range result {
		assert.GreaterOrEqual(t, count, uint64(5))
	}
}

func TestCMSIncrBy_IncrementSameItem(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Add item with increment of 5
	result := s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	assert.Equal(t, uint64(5), result[0])

	// Add more to the same item
	result = s.CMSIncrBy("cms", map[string]uint64{"item1": 3})
	assert.Equal(t, uint64(8), result[0])

	// Verify via query
	queryResult := s.CMSQuery("cms", []string{"item1"})
	assert.Equal(t, uint64(8), queryResult[0])
}

func TestCMSIncrBy_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CMSIncrBy("nonexistent", map[string]uint64{"item1": 1})
	assert.Nil(t, result)
}

func TestCMSIncrBy_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a string key
	s.Set("mykey", "value")

	// Should panic when trying to use wrong type
	assert.Panics(t, func() {
		s.CMSIncrBy("mykey", map[string]uint64{"item1": 1})
	})
}

func TestCMSIncrBy_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})
	s.expires["cms"] = 1 // expired

	// Should return nil since key expired
	result := s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	assert.Nil(t, result)
}

func TestCMSIncrBy_EmptyMap(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result := s.CMSIncrBy("cms", map[string]uint64{})
	assert.Len(t, result, 0)
}

func TestCMSIncrBy_UpdatesTotalCount(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	s.CMSIncrBy("cms", map[string]uint64{"item2": 10})
	s.CMSIncrBy("cms", map[string]uint64{"item1": 3})

	info := s.CMSInfo("cms")
	assert.Equal(t, uint64(18), info[5]) // 5 + 10 + 3 = 18
}

func TestCMSQuery_SingleItem(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})

	result := s.CMSQuery("cms", []string{"item1"})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(10), result[0])
}

func TestCMSQuery_MultipleItems(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"apple": 5})
	s.CMSIncrBy("cms", map[string]uint64{"banana": 10})
	s.CMSIncrBy("cms", map[string]uint64{"cherry": 15})

	// Query all items
	appleCount := s.CMSQuery("cms", []string{"apple"})[0]
	bananaCount := s.CMSQuery("cms", []string{"banana"})[0]
	cherryCount := s.CMSQuery("cms", []string{"cherry"})[0]

	assert.Equal(t, uint64(5), appleCount)
	assert.Equal(t, uint64(10), bananaCount)
	assert.Equal(t, uint64(15), cherryCount)
}

func TestCMSQuery_NonExistentItem(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})

	// Query for item that was never added
	result := s.CMSQuery("cms", []string{"nonexistent"})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(0), result[0])
}

func TestCMSQuery_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.CMSQuery("nonexistent", []string{"item1", "item2", "item3"})
	require.Len(t, result, 3)

	// All should be zero
	for _, count := range result {
		assert.Equal(t, uint64(0), count)
	}
}

func TestCMSQuery_WrongType(t *testing.T) {
	s := NewStore().(*store)

	// Create a list key
	s.LPush("mylist", "value")

	assert.Panics(t, func() {
		s.CMSQuery("mylist", []string{"item1"})
	})
}

func TestCMSQuery_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})
	s.expires["cms"] = 1 // expired

	// Should return zeros since key expired
	result := s.CMSQuery("cms", []string{"item1", "item2"})
	require.Len(t, result, 2)
	assert.Equal(t, uint64(0), result[0])
	assert.Equal(t, uint64(0), result[1])
}

func TestCMSQuery_EmptySlice(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result := s.CMSQuery("cms", []string{})
	assert.Len(t, result, 0)
}

func TestCMSInfo_ValidKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 200, 10)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 100})

	info := s.CMSInfo("cms")
	require.Len(t, info, 6)

	assert.Equal(t, "width", info[0])
	assert.Equal(t, 200, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 10, info[3])
	assert.Equal(t, "count", info[4])
	assert.Equal(t, uint64(100), info[5])
}

func TestCMSInfo_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	info := s.CMSInfo("nonexistent")
	assert.Nil(t, info)
}

func TestCMSInfo_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.CMSInfo("mykey")
	})
}

func TestCMSInfo_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires["cms"] = 1 // expired

	info := s.CMSInfo("cms")
	assert.Nil(t, info)
}

func TestCMS_Workflow(t *testing.T) {
	s := NewStore().(*store)

	// Initialize CMS
	err := s.CMSInitByDim("pageviews", 1000, 5)
	require.NoError(t, err)

	// Track page views
	s.CMSIncrBy("pageviews", map[string]uint64{
		"/home":    100,
		"/about":   50,
		"/contact": 25,
	})

	// More views on home page
	s.CMSIncrBy("pageviews", map[string]uint64{"/home": 50})

	// Query counts
	homeViews := s.CMSQuery("pageviews", []string{"/home"})[0]
	aboutViews := s.CMSQuery("pageviews", []string{"/about"})[0]
	contactViews := s.CMSQuery("pageviews", []string{"/contact"})[0]

	assert.Equal(t, uint64(150), homeViews)
	assert.Equal(t, uint64(50), aboutViews)
	assert.Equal(t, uint64(25), contactViews)

	// Check info
	info := s.CMSInfo("pageviews")
	assert.Equal(t, uint64(225), info[5]) // total count
}

func TestCMS_MultipleKeys(t *testing.T) {
	s := NewStore().(*store)

	// Create multiple CMS instances
	err := s.CMSInitByDim("cms1", 100, 5)
	require.NoError(t, err)
	err = s.CMSInitByDim("cms2", 200, 10)
	require.NoError(t, err)

	// Add to each
	s.CMSIncrBy("cms1", map[string]uint64{"item": 10})
	s.CMSIncrBy("cms2", map[string]uint64{"item": 20})

	// Verify they are independent
	count1 := s.CMSQuery("cms1", []string{"item"})[0]
	count2 := s.CMSQuery("cms2", []string{"item"})[0]

	assert.Equal(t, uint64(10), count1)
	assert.Equal(t, uint64(20), count2)
}

func TestCMS_SpecialCharacters(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	specialItems := map[string]uint64{
		"hello world":   1,
		"tab\there":     2,
		"newline\nhere": 3,
		"unicode: 你好":   4,
		"emoji: 🎉":     5,
	}

	s.CMSIncrBy("cms", specialItems)

	for item, expectedCount := range specialItems {
		result := s.CMSQuery("cms", []string{item})
		assert.GreaterOrEqual(t, result[0], expectedCount,
			"count for special item should be at least %d", expectedCount)
	}
}

func TestCMS_LargeIncrement(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	largeCount := uint64(1 << 32) // 4 billion+
	s.CMSIncrBy("cms", map[string]uint64{"item1": largeCount})

	result := s.CMSQuery("cms", []string{"item1"})
	assert.Equal(t, largeCount, result[0])

	info := s.CMSInfo("cms")
	assert.Equal(t, largeCount, info[5])
}

func TestGetCountMinSketch_ValidKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	cms, exists := s.getCountMinSketch("cms")
	assert.True(t, exists)
	assert.NotNil(t, cms)
}

func TestGetCountMinSketch_NonExistingKey(t *testing.T) {
	s := NewStore().(*store)

	cms, exists := s.getCountMinSketch("nonexistent")
	assert.False(t, exists)
	assert.Nil(t, cms)
}

func TestGetCountMinSketch_WrongType(t *testing.T) {
	s := NewStore().(*store)

	s.Set("mykey", "value")

	assert.Panics(t, func() {
		s.getCountMinSketch("mykey")
	})
}

func TestGetCountMinSketch_ExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires["cms"] = 1 // expired

	cms, exists := s.getCountMinSketch("cms")
	assert.False(t, exists)
	assert.Nil(t, cms)
}
