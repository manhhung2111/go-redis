package data_structure

import (
	"fmt"
	"strconv"
	"testing"
)

func TestNewQuickList(t *testing.T) {
	ql := NewQuickList()
	
	if ql == nil {
		t.Fatal("NewQuickList returned nil")
	}
	
	q := ql.(*quickList)
	if q.size != 0 {
		t.Errorf("Expected size 0, got %d", q.size)
	}
	
	if q.head == nil || q.tail == nil {
		t.Fatal("Sentinel nodes not initialized")
	}
	
	if q.head.next != q.tail {
		t.Error("Head should point to tail in empty list")
	}
	
	if q.tail.prev != q.head {
		t.Error("Tail should point to head in empty list")
	}
}

func TestLPushSingleElement(t *testing.T) {
	ql := NewQuickList()
	
	size := ql.LPush([]string{"first"})
	
	if size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}
	
	result := ql.LRange(0, 0)
	if len(result) != 1 || result[0] != "first" {
		t.Errorf("Expected [first], got %v", result)
	}
}

func TestLPushMultipleElements(t *testing.T) {
	ql := NewQuickList()
	
	ql.LPush([]string{"third", "second", "first"})
	
	result := ql.LRange(0, -1)
	expected := []string{"first", "second", "third"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRPushSingleElement(t *testing.T) {
	ql := NewQuickList()
	
	size := ql.RPush([]string{"first"})
	
	if size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}
	
	result := ql.LRange(0, 0)
	if len(result) != 1 || result[0] != "first" {
		t.Errorf("Expected [first], got %v", result)
	}
}

func TestRPushMultipleElements(t *testing.T) {
	ql := NewQuickList()
	
	ql.RPush([]string{"first", "second", "third"})
	
	result := ql.LRange(0, -1)
	expected := []string{"first", "second", "third"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLPopSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.LPush([]string{"only"})
	
	result := ql.LPop(1)
	
	if len(result) != 1 || result[0] != "only" {
		t.Errorf("Expected [only], got %v", result)
	}
	
	// List should be empty now
	remaining := ql.LRange(0, -1)
	if len(remaining) != 0 {
		t.Errorf("Expected empty list, got %v", remaining)
	}
}

func TestLPopMultipleElements(t *testing.T) {
	ql := NewQuickList()
	ql.LPush([]string{"5", "4", "3", "2", "1"})
	
	result := ql.LPop(3)
	expected := []string{"1", "2", "3"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	
	remaining := ql.LRange(0, -1)
	expectedRemaining := []string{"4", "5"}
	
	if !sliceEqual(remaining, expectedRemaining) {
		t.Errorf("Expected remaining %v, got %v", expectedRemaining, remaining)
	}
}

func TestRPopSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"only"})
	
	result := ql.RPop(1)
	
	if len(result) != 1 || result[0] != "only" {
		t.Errorf("Expected [only], got %v", result)
	}
	
	remaining := ql.LRange(0, -1)
	if len(remaining) != 0 {
		t.Errorf("Expected empty list, got %v", remaining)
	}
}

func TestRPopMultipleElements(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3", "4", "5"})
	
	result := ql.RPop(3)
	expected := []string{"5", "4", "3"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	
	remaining := ql.LRange(0, -1)
	expectedRemaining := []string{"1", "2"}
	
	if !sliceEqual(remaining, expectedRemaining) {
		t.Errorf("Expected remaining %v, got %v", expectedRemaining, remaining)
	}
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
			if !sliceEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
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
			if !sliceEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestLRangeEmptyList(t *testing.T) {
	ql := NewQuickList()
	
	result := ql.LRange(0, -1)
	if len(result) != 0 {
		t.Errorf("Expected empty slice, got %v", result)
	}
}

func TestPopFromEmptyList(t *testing.T) {
	ql := NewQuickList()
	
	lpopResult := ql.LPop(5)
	if lpopResult == nil || len(lpopResult) != 0 {
		t.Errorf("LPop on empty list should return empty list, got %v", lpopResult)
	}
	
	rpopResult := ql.RPop(5)
	if rpopResult == nil || len(rpopResult) != 0 {
		t.Errorf("RPop on empty list should return empty list, got %v", rpopResult)
	}
}

func TestPopZeroCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3"})
	
	lpopResult := ql.LPop(0)
	if lpopResult == nil || len(lpopResult) != 0 {
		t.Errorf("LPop(0) should return return empty list, got %v", lpopResult)
	}
	
	rpopResult := ql.RPop(0)
	if rpopResult == nil || len(rpopResult) != 0 {
		t.Errorf("RPop(0) should return return empty list, got %v", rpopResult)
	}
	
	// Verify list unchanged
	result := ql.LRange(0, -1)
	if len(result) != 3 {
		t.Errorf("List should still have 3 elements, got %d", len(result))
	}
}

func TestPopMoreThanSize(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3"})
	
	result := ql.LPop(10)
	expected := []string{"1", "2", "3"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	
	// List should be empty
	remaining := ql.LRange(0, -1)
	if len(remaining) != 0 {
		t.Errorf("Expected empty list, got %v", remaining)
	}
}

func TestPushEmptySlice(t *testing.T) {
	ql := NewQuickList()
	
	sizeL := ql.LPush([]string{})
	if sizeL != 0 {
		t.Errorf("LPush empty slice should return 0, got %d", sizeL)
	}
	
	sizeR := ql.RPush([]string{})
	if sizeR != 0 {
		t.Errorf("RPush empty slice should return 0, got %d", sizeR)
	}
}

func TestMixedPushOperations(t *testing.T) {
	ql := NewQuickList()
	
	ql.LPush([]string{"2", "1"})     // [1, 2]
	ql.RPush([]string{"3", "4"})     // [1, 2, 3, 4]
	ql.LPush([]string{"0"})          // [0, 1, 2, 3, 4]
	ql.RPush([]string{"5"})          // [0, 1, 2, 3, 4, 5]
	
	result := ql.LRange(0, -1)
	expected := []string{"0", "1", "2", "3", "4", "5"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestMixedPopOperations(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"1", "2", "3", "4", "5", "6"})
	
	lpop := ql.LPop(2)  // [3, 4, 5, 6]
	if !sliceEqual(lpop, []string{"1", "2"}) {
		t.Errorf("LPop failed: expected [1, 2], got %v", lpop)
	}
	
	rpop := ql.RPop(2)  // [3, 4]
	if !sliceEqual(rpop, []string{"6", "5"}) {
		t.Errorf("RPop failed: expected [6, 5], got %v", rpop)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"3", "4"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestAlternatingPushPop(t *testing.T) {
	ql := NewQuickList()
	
	ql.LPush([]string{"1"}) // [1]
	ql.RPush([]string{"2"}) // [1, 2]
	ql.LPop(1) // [2]
	ql.RPush([]string{"3"}) // [2, 3]
	ql.LPush([]string{"4"}) // [4, 2, 3]
	ql.RPop(1) // [4, 2]
	
	result := ql.LRange(0, -1)
	expected := []string{"4", "2"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
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
	if len(result) != 1 || result[0] != "elem-999" {
		t.Errorf("First element should be elem-999, got %v", result)
	}
	
	result = ql.LRange(-1, -1)
	if len(result) != 1 || result[0] != "elem-0" {
		t.Errorf("Last element should be elem-0, got %v", result)
	}
	
	// Verify total size
	allElements := ql.LRange(0, -1)
	if len(allElements) != 1000 {
		t.Errorf("Expected 1000 elements, got %d", len(allElements))
	}
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
	expected := []string{"elem-0", "elem-1", "elem-2", "elem-3", "elem-4"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
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
	if len(lpopResult) != 200 {
		t.Errorf("Expected to pop 200 elements, got %d", len(lpopResult))
	}
	if lpopResult[0] != "0" || lpopResult[199] != "199" {
		t.Errorf("LPop returned wrong elements: first=%s, last=%s", 
			lpopResult[0], lpopResult[199])
	}
	
	// Pop 150 from right
	rpopResult := ql.RPop(150)
	if len(rpopResult) != 150 {
		t.Errorf("Expected to pop 150 elements, got %d", len(rpopResult))
	}
	if rpopResult[0] != "499" || rpopResult[149] != "350" {
		t.Errorf("RPop returned wrong elements: first=%s, last=%s",
			rpopResult[0], rpopResult[149])
	}
	
	// Verify remaining elements
	remaining := ql.LRange(0, -1)
	if len(remaining) != 150 {
		t.Errorf("Expected 150 remaining elements, got %d", len(remaining))
	}
	if remaining[0] != "200" || remaining[149] != "349" {
		t.Errorf("Remaining elements wrong: first=%s, last=%s",
			remaining[0], remaining[149])
	}
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
	if len(result) != 101 {
		t.Errorf("Expected 101 elements, got %d", len(result))
	}
	
	if result[0] != "item-100" {
		t.Errorf("First element should be item-100, got %s", result[0])
	}
	
	if result[100] != "item-200" {
		t.Errorf("Last element should be item-200, got %s", result[100])
	}
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
		if ok != tt.exists {
			t.Errorf("LIndex(%d): expected exists=%v, got %v", tt.index, tt.exists, ok)
		}
		if ok && val != tt.expected {
			t.Errorf("LIndex(%d): expected %s, got %s", tt.index, tt.expected, val)
		}
	}
}

func TestLIndexEmptyList(t *testing.T) {
	ql := NewQuickList()
	
	val, ok := ql.LIndex(0)
	if ok {
		t.Error("LIndex on empty list should return false")
	}
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestLRemPositiveCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a", "d", "a"})
	
	// Remove first 2 occurrences of "a"
	removed := ql.LRem(2, "a")
	
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"b", "c", "a", "d", "a"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLRemNegativeCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a", "d", "a"})
	
	// Remove last 2 occurrences of "a"
	removed := ql.LRem(-2, "a")
	
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"a", "b", "a", "c", "d"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLRemZeroCount(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "a", "c", "a"})
	
	// Remove all occurrences of "a"
	removed := ql.LRem(0, "a")
	
	if removed != 3 {
		t.Errorf("Expected 3 removed, got %d", removed)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"b", "c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLRemNoMatch(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	removed := ql.LRem(5, "x")
	
	if removed != 0 {
		t.Errorf("Expected 0 removed, got %d", removed)
	}
	
	// List should be unchanged
	result := ql.LRange(0, -1)
	expected := []string{"a", "b", "c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("List should be unchanged: expected %v, got %v", expected, result)
	}
}

func TestLRemEmptyList(t *testing.T) {
	ql := NewQuickList()
	
	removed := ql.LRem(5, "a")
	
	if removed != 0 {
		t.Errorf("Expected 0 removed, got %d", removed)
	}
}

func TestLRemAllElements(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "a", "a"})
	
	removed := ql.LRem(0, "a")
	
	if removed != 3 {
		t.Errorf("Expected 3 removed, got %d", removed)
	}
	
	// List should be empty
	if ql.Size() != 0 {
		t.Errorf("List should be empty, size is %d", ql.Size())
	}
}

func TestLSetBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	// Set first element
	err := ql.LSet(0, "x")
	if err != nil {
		t.Errorf("LSet failed: %v", err)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"x", "b", "c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLSetNegativeIndex(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	// Set last element
	err := ql.LSet(-1, "z")
	if err != nil {
		t.Errorf("LSet failed: %v", err)
	}
	
	result := ql.LRange(0, -1)
	expected := []string{"a", "b", "z"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLSetOutOfBounds(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	tests := []int32{10, -10, 3, -4}
	
	for _, index := range tests {
		err := ql.LSet(index, "x")
		if err == nil {
			t.Errorf("LSet(%d) should return error for out of bounds", index)
		}
	}
}

func TestLSetMiddle(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	err := ql.LSet(2, "X")
	if err != nil {
		t.Errorf("LSet failed: %v", err)
	}
	
	val, _ := ql.LIndex(2)
	if val != "X" {
		t.Errorf("Expected X at index 2, got %s", val)
	}
}

func TestLTrimBasic(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	// Keep elements from index 1 to 3
	ql.LTrim(1, 3)
	
	result := ql.LRange(0, -1)
	expected := []string{"b", "c", "d"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimKeepAll(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	// Keep all elements
	ql.LTrim(0, -1)
	
	result := ql.LRange(0, -1)
	expected := []string{"a", "b", "c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimNegativeIndices(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	// Keep last 3 elements
	ql.LTrim(-3, -1)
	
	result := ql.LRange(0, -1)
	expected := []string{"c", "d", "e"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimClearList(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	// Invalid range clears the list
	ql.LTrim(10, 20)
	
	if ql.Size() != 0 {
		t.Errorf("List should be empty, size is %d", ql.Size())
	}
}

func TestLTrimEmptyList(t *testing.T) {
	ql := NewQuickList()
	
	// Should not panic
	ql.LTrim(0, 5)
	
	if ql.Size() != 0 {
		t.Error("Empty list should remain empty")
	}
}

func TestLTrimSingleElement(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	// Keep only element at index 2
	ql.LTrim(2, 2)
	
	result := ql.LRange(0, -1)
	expected := []string{"c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimFromStart(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	// Keep first 3 elements
	ql.LTrim(0, 2)
	
	result := ql.LRange(0, -1)
	expected := []string{"a", "b", "c"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimToEnd(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c", "d", "e"})
	
	// Remove first 2, keep the rest
	ql.LTrim(2, 10)
	
	result := ql.LRange(0, -1)
	expected := []string{"c", "d", "e"}
	
	if !sliceEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestLTrimStartGreaterThanEnd(t *testing.T) {
	ql := NewQuickList()
	ql.RPush([]string{"a", "b", "c"})
	
	// Invalid range
	ql.LTrim(3, 1)
	
	// Should clear the list
	if ql.Size() != 0 {
		t.Errorf("List should be empty, size is %d", ql.Size())
	}
}


func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}