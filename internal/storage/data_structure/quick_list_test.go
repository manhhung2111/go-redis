package quicklist

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