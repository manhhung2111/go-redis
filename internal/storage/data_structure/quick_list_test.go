package data_structure

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQuickList(t *testing.T) {
	ql := NewQuickList()
	require.NotNil(t, ql)

	q := ql.(*quickList)
	assert.Equal(t, uint32(0), q.size)
	require.NotNil(t, q.head)
	require.NotNil(t, q.tail)
	assert.Equal(t, q.tail, q.head.next, "Head should point to tail in empty list")
	assert.Equal(t, q.head, q.tail.prev, "Tail should point to head in empty list")
}

func TestLPushSingleElement(t *testing.T) {
	ql := NewQuickList()

	size := ql.LPush([]string{"first"})
	assert.Equal(t, uint32(1), size)

	result := ql.LRange(0, 0)
	assert.Equal(t, []string{"first"}, result)
}

func TestLPushMultipleElements(t *testing.T) {
	ql := NewQuickList()

	ql.LPush([]string{"third", "second", "first"})

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"first", "second", "third"}, result)
}

func TestRPushSingleElement(t *testing.T) {
	ql := NewQuickList()

	size := ql.RPush([]string{"first"})
	assert.Equal(t, uint32(1), size)

	result := ql.LRange(0, 0)
	assert.Equal(t, []string{"first"}, result)
}

func TestRPushMultipleElements(t *testing.T) {
	ql := NewQuickList()

	ql.RPush([]string{"first", "second", "third"})

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"first", "second", "third"}, result)
}

func TestLPopSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.LPush([]string{"only"})

	result := ql.LPop(1)
	assert.Equal(t, []string{"only"}, result)

	// List should be empty now
	remaining := ql.LRange(0, -1)
	assert.Empty(t, remaining)
}

func TestLPopMultipleElements(t *testing.T) {
	ql := NewQuickList()
	ql.LPush([]string{"5", "4", "3", "2", "1"})

	result := ql.LPop(3)
	assert.Equal(t, []string{"1", "2", "3"}, result)

	remaining := ql.LRange(0, -1)
	assert.Equal(t, []string{"4", "5"}, remaining)
}

func TestRPopSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"only"})

	result := ql.RPop(1)
	assert.Equal(t, []string{"only"}, result)

	remaining := ql.LRange(0, -1)
	assert.Empty(t, remaining)
}

func TestRPopMultipleElements(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3", "4", "5"})

	result := ql.RPop(3)
	assert.Equal(t, []string{"5", "4", "3"}, result)

	remaining := ql.LRange(0, -1)
	assert.Equal(t, []string{"1", "2"}, remaining)
}

func TestLRangeBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"0", "1", "2", "3", "4"})

	tests := []struct {
		name     string
		start    int32
		end      int32
		expected []string
	}{
		{"Full range", 0, -1, []string{"0", "1", "2", "3", "4"}},
		{"Partial range", 1, 3, []string{"1", "2", "3"}},
		{"Single element", 2, 2, []string{"2"}},
		{"From start", 0, 2, []string{"0", "1", "2"}},
		{"To end", 2, -1, []string{"2", "3", "4"}},
		{"Negative indices", -3, -1, []string{"2", "3", "4"}},
		{"Mixed indices", -4, 3, []string{"1", "2", "3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ql.LRange(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLRangeEdgeCases(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"0", "1", "2"})

	tests := []struct {
		name     string
		start    int32
		end      int32
		expected []string
	}{
		{"Empty - start > end", 2, 1, []string{}},
		{"Empty - start >= size", 5, 10, []string{}},
		{"End beyond size", 0, 100, []string{"0", "1", "2"}},
		{"Negative start beyond size", -10, -1, []string{"0", "1", "2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ql.LRange(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLRangeEmptyList(t *testing.T) {
	ql := NewQuickList()

	result := ql.LRange(0, -1)
	assert.Empty(t, result)
}

func TestPopFromEmptyList(t *testing.T) {
	ql := NewQuickList()

	lpopResult := ql.LPop(5)
	assert.NotNil(t, lpopResult)
	assert.Empty(t, lpopResult)

	rpopResult := ql.RPop(5)
	assert.NotNil(t, rpopResult)
	assert.Empty(t, rpopResult)
}

func TestPopZeroCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3"})

	lpopResult := ql.LPop(0)
	assert.NotNil(t, lpopResult)
	assert.Empty(t, lpopResult)

	rpopResult := ql.RPop(0)
	assert.NotNil(t, rpopResult)
	assert.Empty(t, rpopResult)

	// Verify list unchanged
	result := ql.LRange(0, -1)
	assert.Len(t, result, 3)
}

func TestPopMoreThanSize(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3"})

	result := ql.LPop(10)
	assert.Equal(t, []string{"1", "2", "3"}, result)

	// List should be empty
	remaining := ql.LRange(0, -1)
	assert.Empty(t, remaining)
}

func TestPushEmptySlice(t *testing.T) {
	ql := NewQuickList()

	sizeL := ql.LPush([]string{})
	assert.Equal(t, uint32(0), sizeL)

	sizeR := ql.RPush([]string{})
	assert.Equal(t, uint32(0), sizeR)
}

func TestMixedPushOperations(t *testing.T) {
	ql := NewQuickList()

	ql.LPush([]string{"2", "1"}) // [1, 2]
	ql.RPush([]string{"3", "4"}) // [1, 2, 3, 4]
	ql.LPush([]string{"0"})      // [0, 1, 2, 3, 4]
	ql.RPush([]string{"5"})      // [0, 1, 2, 3, 4, 5]

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"0", "1", "2", "3", "4", "5"}, result)
}

func TestMixedPopOperations(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3", "4", "5", "6"})

	lpop := ql.LPop(2) // [3, 4, 5, 6]
	assert.Equal(t, []string{"1", "2"}, lpop)

	rpop := ql.RPop(2) // [3, 4]
	assert.Equal(t, []string{"6", "5"}, rpop)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"3", "4"}, result)
}

func TestAlternatingPushPop(t *testing.T) {
	ql := NewQuickList()

	ql.LPush([]string{"1"}) // [1]
	ql.RPush([]string{"2"}) // [1, 2]
	ql.LPop(1)              // [2]
	ql.RPush([]string{"3"}) // [2, 3]
	ql.LPush([]string{"4"}) // [4, 2, 3]
	ql.RPop(1)              // [4, 2]

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"4", "2"}, result)
}

func TestLargeDataLPush(t *testing.T) {
	ql := NewQuickList()

	// Create enough data to span multiple nodes
	elements := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		elements[i] = fmt.Sprintf("elem-%d", i)
	}

	ql.LPush(elements)

	// Verify first and last elements
	result := ql.LRange(0, 0)
	assert.Equal(t, []string{"elem-999"}, result)

	result = ql.LRange(-1, -1)
	assert.Equal(t, []string{"elem-0"}, result)

	// Verify total size
	allElements := ql.LRange(0, -1)
	assert.Len(t, allElements, 1000)
}

func TestLargeDataRPush(t *testing.T) {
	ql := NewQuickList()

	elements := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		elements[i] = fmt.Sprintf("elem-%d", i)
	}

	ql.RPush(elements)

	// Verify order is preserved
	result := ql.LRange(0, 4)
	assert.Equal(t, []string{"elem-0", "elem-1", "elem-2", "elem-3", "elem-4"}, result)
}

func TestLargeDataPopOperations(t *testing.T) {
	ql := NewQuickList()

	// Push 500 elements
	elements := make([]string, 500)
	for i := 0; i < 500; i++ {
		elements[i] = strconv.Itoa(i)
	}
	ql.RPush(elements)

	// Pop 200 from left
	lpopResult := ql.LPop(200)
	assert.Len(t, lpopResult, 200)
	assert.Equal(t, "0", lpopResult[0])
	assert.Equal(t, "199", lpopResult[199])

	// Pop 150 from right
	rpopResult := ql.RPop(150)
	assert.Len(t, rpopResult, 150)
	assert.Equal(t, "499", rpopResult[0])
	assert.Equal(t, "350", rpopResult[149])

	// Verify remaining elements
	remaining := ql.LRange(0, -1)
	assert.Len(t, remaining, 150)
	assert.Equal(t, "200", remaining[0])
	assert.Equal(t, "349", remaining[149])
}

func TestLRangeAcrossMultipleNodes(t *testing.T) {
	ql := NewQuickList()

	// Create data that will span multiple nodes
	elements := make([]string, 500)
	for i := 0; i < 500; i++ {
		elements[i] = fmt.Sprintf("item-%03d", i)
	}
	ql.RPush(elements)

	// Test range that likely spans multiple nodes
	result := ql.LRange(100, 200)
	assert.Len(t, result, 101)
	assert.Equal(t, "item-100", result[0])
	assert.Equal(t, "item-200", result[100])
}

func TestLIndexBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	tests := []struct {
		index    int32
		expected string
		exists   bool
	}{
		{0, "a", true},
		{1, "b", true},
		{4, "e", true},
		{-1, "e", true},
		{-2, "d", true},
		{-5, "a", true},
		{10, "", false},
		{-10, "", false},
	}

	for _, tt := range tests {
		val, ok := ql.LIndex(tt.index)
		assert.Equal(t, tt.exists, ok, "LIndex(%d) exists", tt.index)
		if ok {
			assert.Equal(t, tt.expected, val, "LIndex(%d) value", tt.index)
		}
	}
}

func TestLIndexEmptyList(t *testing.T) {
	ql := NewQuickList()

	val, ok := ql.LIndex(0)
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestLRemPositiveCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a", "d", "a"})

	// Remove first 2 occurrences of "a"
	removed := ql.LRem(2, "a")
	assert.Equal(t, uint32(2), removed)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "a", "d", "a"}, result)
}

func TestLRemNegativeCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a", "d", "a"})

	// Remove last 2 occurrences of "a"
	removed := ql.LRem(-2, "a")
	assert.Equal(t, uint32(2), removed)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "a", "c", "d"}, result)
}

func TestLRemZeroCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a"})

	// Remove all occurrences of "a"
	removed := ql.LRem(0, "a")
	assert.Equal(t, uint32(3), removed)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"b", "c"}, result)
}

func TestLRemNoMatch(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	removed := ql.LRem(5, "x")
	assert.Equal(t, uint32(0), removed)

	// List should be unchanged
	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestLRemEmptyList(t *testing.T) {
	ql := NewQuickList()

	removed := ql.LRem(5, "a")
	assert.Equal(t, uint32(0), removed)
}

func TestLRemAllElements(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "a", "a"})

	removed := ql.LRem(0, "a")
	assert.Equal(t, uint32(3), removed)

	// List should be empty
	assert.Equal(t, uint32(0), ql.Size())
}

func TestLSetBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	// Set first element
	err := ql.LSet(0, "x")
	assert.NoError(t, err)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"x", "b", "c"}, result)
}

func TestLSetNegativeIndex(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	// Set last element
	err := ql.LSet(-1, "z")
	assert.NoError(t, err)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "z"}, result)
}

func TestLSetOutOfBounds(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	tests := []int32{10, -10, 3, -4}

	for _, index := range tests {
		err := ql.LSet(index, "x")
		assert.Error(t, err, "LSet(%d) should return error", index)
	}
}

func TestLSetMiddle(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	err := ql.LSet(2, "X")
	assert.NoError(t, err)

	val, _ := ql.LIndex(2)
	assert.Equal(t, "X", val)
}

func TestLTrimBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	// Keep elements from index 1 to 3
	ql.LTrim(1, 3)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "d"}, result)
}

func TestLTrimKeepAll(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	// Keep all elements
	ql.LTrim(0, -1)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestLTrimNegativeIndices(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	// Keep last 3 elements
	ql.LTrim(-3, -1)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"c", "d", "e"}, result)
}

func TestLTrimClearList(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	// Invalid range clears the list
	ql.LTrim(10, 20)

	assert.Equal(t, uint32(0), ql.Size())
}

func TestLTrimEmptyList(t *testing.T) {
	ql := NewQuickList()

	// Should not panic
	ql.LTrim(0, 5)

	assert.Equal(t, uint32(0), ql.Size())
}

func TestLTrimSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	// Keep only element at index 2
	ql.LTrim(2, 2)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"c"}, result)
}

func TestLTrimFromStart(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	// Keep first 3 elements
	ql.LTrim(0, 2)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestLTrimToEnd(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})

	// Remove first 2, keep the rest
	ql.LTrim(2, 10)

	result := ql.LRange(0, -1)
	assert.Equal(t, []string{"c", "d", "e"}, result)
}

func TestLTrimStartGreaterThanEnd(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})

	// Invalid range
	ql.LTrim(3, 1)

	// Should clear the list
	assert.Equal(t, uint32(0), ql.Size())
}