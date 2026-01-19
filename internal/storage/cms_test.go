package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStoreCMS() Store {
	return NewStore(config.NewConfig())
}

func TestCMSInitByDim_NewKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Verify CMS was created
	rObj, exists := s.data.Get("cms")
	require.True(t, exists)
	assert.Equal(t, ObjCountMinSketch, rObj.objType)
	assert.Equal(t, EncCountMinSketch, rObj.encoding)

	// Verify dimensions via Info
	info, err := s.CMSInfo("cms")
	assert.Equal(t, "width", info[0])
	assert.Equal(t, 100, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 5, info[3])
	require.NoError(t, err)
}

func TestCMSInitByDim_KeyExists(t *testing.T) {
	s := newTestStoreCMS().(*store)

	// Create CMS
	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Try to create again - should fail
	err = s.CMSInitByDim("cms", 200, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key already exists")

	// Original dimensions should be preserved
	info, err := s.CMSInfo("cms")
	assert.Equal(t, 100, info[1])
	assert.Equal(t, 5, info[3])
	require.NoError(t, err)
}

func TestCMSInitByDim_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	// Create CMS and expire it
	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires.Set("cms", 1) // expired timestamp

	// Should succeed since key expired
	err = s.CMSInitByDim("cms", 200, 10)
	require.NoError(t, err)

	// New dimensions should be set
	info, err := s.CMSInfo("cms")
	assert.Equal(t, 200, info[1])
	assert.Equal(t, 10, info[3])
	require.NoError(t, err)
}

func TestCMSInitByProb_NewKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)

	// Verify CMS was created
	rObj, exists := s.data.Get("cms")
	require.True(t, exists)
	assert.Equal(t, ObjCountMinSketch, rObj.objType)
	assert.Equal(t, EncCountMinSketch, rObj.encoding)

	// Verify dimensions via Info
	// errorRate = 0.01 -> width = ceil(e / 0.01) = 272
	// probability = 0.01 -> depth = ceil(ln(100)) = 5
	info, err := s.CMSInfo("cms")
	assert.Equal(t, 272, info[1])
	assert.Equal(t, 5, info[3])
	require.NoError(t, err)
}

func TestCMSInitByProb_KeyExists(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)

	// Try to create again - should fail
	err = s.CMSInitByProb("cms", 0.1, 0.1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key already exists")
}

func TestCMSInitByProb_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByProb("cms", 0.01, 0.01)
	require.NoError(t, err)
	s.expires.Set("cms", 1) // expired

	// Should succeed since key expired
	err = s.CMSInitByProb("cms", 0.1, 0.1)
	require.NoError(t, err)
}

func TestCMSIncrBy_SingleItem(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result, err := s.CMSIncrBy("cms", map[string]uint64{"item1": 1})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(1), result[0])
	require.NoError(t, err)
}

func TestCMSIncrBy_MultipleItems(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	items := map[string]uint64{
		"apple":  5,
		"banana": 10,
		"cherry": 15,
	}

	result, err := s.CMSIncrBy("cms", items)
	require.Len(t, result, 3)
	require.NoError(t, err)

	// All results should be at least the increment value
	for _, count := range result {
		assert.GreaterOrEqual(t, count, uint64(5))
	}
}

func TestCMSIncrBy_IncrementSameItem(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	// Add item with increment of 5
	result, err := s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	assert.Equal(t, uint64(5), result[0])
	require.NoError(t, err)

	// Add more to the same item
	result, err = s.CMSIncrBy("cms", map[string]uint64{"item1": 3})
	assert.Equal(t, uint64(8), result[0])
	require.NoError(t, err)

	// Verify via query
	queryResult, err := s.CMSQuery("cms", []string{"item1"})
	assert.Equal(t, uint64(8), queryResult[0])
	require.NoError(t, err)
}

func TestCMSIncrBy_NonExistingKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	result, err := s.CMSIncrBy("nonexistent", map[string]uint64{"item1": 1})
	assert.Nil(t, result)
	require.Error(t, err)
}

func TestCMSIncrBy_WrongType(t *testing.T) {
	s := newTestStoreCMS().(*store)

	// Create a string key
	s.Set("mykey", "value")

	_, err := s.CMSIncrBy("mykey", map[string]uint64{"item1": 1})
	assert.Error(t, err)
}

func TestCMSIncrBy_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})
	s.expires.Set("cms", 1) // expired

	// Should return nil since key expired
	result, err := s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestCMSIncrBy_EmptyMap(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result, err := s.CMSIncrBy("cms", map[string]uint64{})
	assert.Len(t, result, 0)
	assert.NoError(t, err)
}

func TestCMSIncrBy_UpdatesTotalCount(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 5})
	s.CMSIncrBy("cms", map[string]uint64{"item2": 10})
	s.CMSIncrBy("cms", map[string]uint64{"item1": 3})

	info, err := s.CMSInfo("cms")
	assert.Equal(t, uint64(18), info[5]) // 5 + 10 + 3 = 18
	assert.NoError(t, err)
}

func TestCMSQuery_SingleItem(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})

	result, err := s.CMSQuery("cms", []string{"item1"})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(10), result[0])
	assert.NoError(t, err)
}

func TestCMSQuery_MultipleItems(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"apple": 5})
	s.CMSIncrBy("cms", map[string]uint64{"banana": 10})
	s.CMSIncrBy("cms", map[string]uint64{"cherry": 15})

	// Query all items
	appleCount, _ := s.CMSQuery("cms", []string{"apple"})
	bananaCount, _ := s.CMSQuery("cms", []string{"banana"})
	cherryCount, _ := s.CMSQuery("cms", []string{"cherry"})

	assert.Equal(t, uint64(5), appleCount[0])
	assert.Equal(t, uint64(10), bananaCount[0])
	assert.Equal(t, uint64(15), cherryCount[0])
}

func TestCMSQuery_NonExistentItem(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})

	// Query for item that was never added
	result, err := s.CMSQuery("cms", []string{"nonexistent"})
	require.Len(t, result, 1)
	assert.Equal(t, uint64(0), result[0])
	assert.NoError(t, err)
}

func TestCMSQuery_NonExistingKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	_, err := s.CMSQuery("nonexistent", []string{"item1", "item2", "item3"})
	assert.Error(t, err)
}

func TestCMSQuery_WrongType(t *testing.T) {
	s := newTestStoreCMS().(*store)

	// Create a list key
	s.LPush("mylist", "value")

	_, err := s.CMSQuery("mylist", []string{"item1"})
	assert.Error(t, err)
}

func TestCMSQuery_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.CMSIncrBy("cms", map[string]uint64{"item1": 10})
	s.expires.Set("cms", 1) // expired

	// Should return zeros since key expired
	_, err = s.CMSQuery("cms", []string{"item1", "item2"})
	assert.Error(t, err)
}

func TestCMSQuery_EmptySlice(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	result, err := s.CMSQuery("cms", []string{})
	assert.Len(t, result, 0)
	assert.NoError(t, err)
}

func TestCMSInfo_ValidKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 200, 10)
	require.NoError(t, err)

	s.CMSIncrBy("cms", map[string]uint64{"item1": 100})

	info, err := s.CMSInfo("cms")
	require.Len(t, info, 6)
	assert.NoError(t, err)

	assert.Equal(t, "width", info[0])
	assert.Equal(t, 200, info[1])
	assert.Equal(t, "depth", info[2])
	assert.Equal(t, 10, info[3])
	assert.Equal(t, "count", info[4])
	assert.Equal(t, uint64(100), info[5])
}

func TestCMSInfo_NonExistingKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	info, err := s.CMSInfo("nonexistent")
	assert.Nil(t, info)
	assert.Error(t, err)
}

func TestCMSInfo_WrongType(t *testing.T) {
	s := newTestStoreCMS().(*store)

	s.Set("mykey", "value")

	_, err := s.CMSInfo("mykey")
	assert.Error(t, err)
}

func TestCMSInfo_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires.Set("cms", 1) // expired

	info, err := s.CMSInfo("cms")
	assert.Nil(t, info)
	assert.Error(t, err)
}

func TestCMS_Workflow(t *testing.T) {
	s := newTestStoreCMS().(*store)

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
	homeViews, _ := s.CMSQuery("pageviews", []string{"/home"})
	aboutViews, _ := s.CMSQuery("pageviews", []string{"/about"})
	contactViews, _ := s.CMSQuery("pageviews", []string{"/contact"})

	assert.Equal(t, uint64(150), homeViews[0])
	assert.Equal(t, uint64(50), aboutViews[0])
	assert.Equal(t, uint64(25), contactViews[0])

	// Check info
	info, err := s.CMSInfo("pageviews")
	assert.Equal(t, uint64(225), info[5]) // total count
	assert.NoError(t, err)
}

func TestCMS_MultipleKeys(t *testing.T) {
	s := newTestStoreCMS().(*store)

	// Create multiple CMS instances
	err := s.CMSInitByDim("cms1", 100, 5)
	require.NoError(t, err)
	err = s.CMSInitByDim("cms2", 200, 10)
	require.NoError(t, err)

	// Add to each
	s.CMSIncrBy("cms1", map[string]uint64{"item": 10})
	s.CMSIncrBy("cms2", map[string]uint64{"item": 20})

	// Verify they are independent
	count1, _ := s.CMSQuery("cms1", []string{"item"})
	count2, _ := s.CMSQuery("cms2", []string{"item"})

	assert.Equal(t, uint64(10), count1[0])
	assert.Equal(t, uint64(20), count2[0])
}

func TestCMS_SpecialCharacters(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	specialItems := map[string]uint64{
		"hello world":   1,
		"tab\there":     2,
		"newline\nhere": 3,
		"unicode: 你好":   4,
		"emoji: 🎉":      5,
	}

	s.CMSIncrBy("cms", specialItems)

	for item, expectedCount := range specialItems {
		result, err := s.CMSQuery("cms", []string{item})
		assert.GreaterOrEqual(t, result[0], expectedCount,
			"count for special item should be at least %d", expectedCount)
		assert.NoError(t, err)
	}
}

func TestCMS_LargeIncrement(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	largeCount := uint64(1 << 32) // 4 billion+
	s.CMSIncrBy("cms", map[string]uint64{"item1": largeCount})

	result, _ := s.CMSQuery("cms", []string{"item1"})
	assert.Equal(t, largeCount, result[0])

	info, _ := s.CMSInfo("cms")
	assert.Equal(t, largeCount, info[5])
}

func TestGetCountMinSketch_ValidKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)

	cms, err := s.getCountMinSketch("cms", false)
	assert.NoError(t, err)
	assert.NotNil(t, cms)
}

func TestGetCountMinSketch_NonExistingKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	cms, err := s.getCountMinSketch("nonexistent", false)
	assert.Error(t, err)
	assert.Nil(t, cms)
}

func TestGetCountMinSketch_WrongType(t *testing.T) {
	s := newTestStoreCMS().(*store)

	s.Set("mykey", "value")

	_, err := s.getCountMinSketch("mykey", false)
	assert.Error(t, err)
}

func TestGetCountMinSketch_ExpiredKey(t *testing.T) {
	s := newTestStoreCMS().(*store)

	err := s.CMSInitByDim("cms", 100, 5)
	require.NoError(t, err)
	s.expires.Set("cms", 1) // expired

	cms, err := s.getCountMinSketch("cms", false)
	assert.Error(t, err)
	assert.Nil(t, cms)
}
