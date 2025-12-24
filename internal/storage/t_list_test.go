package storage

import (
	"testing"
)

func TestLPush(t *testing.T) {
	t.Run("push to new key", func(t *testing.T) {
		s := NewStore()
		count := s.LPush("mylist", "world", "hello")
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}

		// Verify order (should be hello, world due to LPUSH)
		result := s.LRange("mylist", 0, -1)
		if len(result) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(result))
		}
		if result[0] != "hello" || result[1] != "world" {
			t.Errorf("expected [hello, world], got %v", result)
		}
	})

	t.Run("push to existing key", func(t *testing.T) {
		s := NewStore()
		s.LPush("mylist", "world")
		count := s.LPush("mylist", "hello")
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}

		result := s.LRange("mylist", 0, -1)
		if result[0] != "hello" || result[1] != "world" {
			t.Errorf("expected [hello, world], got %v", result)
		}
	})

	t.Run("push multiple elements", func(t *testing.T) {
		s := NewStore()
		count := s.LPush("mylist", "three", "two", "one")
		if count != 3 {
			t.Errorf("expected 3, got %d", count)
		}

		result := s.LRange("mylist", 0, -1)
		expected := []string{"one", "two", "three"}
		for i, v := range expected {
			if result[i] != v {
				t.Errorf("at index %d: expected %s, got %s", i, v, result[i])
			}
		}
	})

	t.Run("push to wrong type returns 0", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		count := s.LPush("mykey", "value")
		if count != 0 {
			t.Errorf("expected 0 for wrong type, got %d", count)
		}
	})
}

func TestRPush(t *testing.T) {
	t.Run("push to new key", func(t *testing.T) {
		s := NewStore()
		count := s.RPush("mylist", "hello", "world")
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}

		result := s.LRange("mylist", 0, -1)
		if len(result) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(result))
		}
		if result[0] != "hello" || result[1] != "world" {
			t.Errorf("expected [hello, world], got %v", result)
		}
	})

	t.Run("push to existing key", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "hello")
		count := s.RPush("mylist", "world")
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}

		result := s.LRange("mylist", 0, -1)
		if result[0] != "hello" || result[1] != "world" {
			t.Errorf("expected [hello, world], got %v", result)
		}
	})

	t.Run("push to wrong type returns 0", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		count := s.RPush("mykey", "value")
		if count != 0 {
			t.Errorf("expected 0 for wrong type, got %d", count)
		}
	})
}

func TestLPop(t *testing.T) {
	t.Run("pop from list with elements", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		result := s.LPop("mylist", 1)
		if len(result) != 1 || result[0] != "one" {
			t.Errorf("expected [one], got %v", result)
		}

		remaining := s.LRange("mylist", 0, -1)
		if len(remaining) != 2 {
			t.Errorf("expected 2 remaining elements, got %d", len(remaining))
		}
	})

	t.Run("pop multiple elements", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four")
		
		result := s.LPop("mylist", 2)
		if len(result) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(result))
		}
		if result[0] != "one" || result[1] != "two" {
			t.Errorf("expected [one, two], got %v", result)
		}
	})

	t.Run("pop all elements deletes key", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two")
		
		s.LPop("mylist", 2)
		_, exists := s.Get("mylist")
		if exists {
			t.Error("expected key to be deleted when list is empty")
		}
	})

	t.Run("pop from non-existent key", func(t *testing.T) {
		s := NewStore()
		result := s.LPop("nonexistent", 1)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("pop from wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		result := s.LPop("mykey", 1)
		if result != nil {
			t.Errorf("expected nil for wrong type, got %v", result)
		}
	})
}

func TestRPop(t *testing.T) {
	t.Run("pop from list with elements", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		result := s.RPop("mylist", 1)
		if len(result) != 1 || result[0] != "three" {
			t.Errorf("expected [three], got %v", result)
		}

		remaining := s.LRange("mylist", 0, -1)
		if len(remaining) != 2 {
			t.Errorf("expected 2 remaining elements, got %d", len(remaining))
		}
	})

	t.Run("pop multiple elements", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four")
		
		result := s.RPop("mylist", 2)
		if len(result) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(result))
		}
		if result[0] != "four" || result[1] != "three" {
			t.Errorf("expected [four, three], got %v", result)
		}
	})

	t.Run("pop all elements deletes key", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two")
		
		s.RPop("mylist", 2)
		_, exists := s.Get("mylist")
		if exists {
			t.Error("expected key to be deleted when list is empty")
		}
	})

	t.Run("pop from non-existent key", func(t *testing.T) {
		s := NewStore()
		result := s.RPop("nonexistent", 1)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

func TestLRange(t *testing.T) {
	t.Run("range with positive indices", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four", "five")
		
		result := s.LRange("mylist", 1, 3)
		expected := []string{"two", "three", "four"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("range with negative indices", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four", "five")
		
		result := s.LRange("mylist", 0, -1)
		if len(result) != 5 {
			t.Errorf("expected 5 elements, got %d", len(result))
		}
	})

	t.Run("range from non-existent key", func(t *testing.T) {
		s := NewStore()
		result := s.LRange("nonexistent", 0, -1)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("range from wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		result := s.LRange("mykey", 0, -1)
		if result != nil {
			t.Errorf("expected nil for wrong type, got %v", result)
		}
	})
}

func TestLIndex(t *testing.T) {
	t.Run("get element at positive index", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		val, ok := s.LIndex("mylist", 0)
		if !ok || val != "one" {
			t.Errorf("expected (one, true), got (%s, %v)", val, ok)
		}

		val, ok = s.LIndex("mylist", 2)
		if !ok || val != "three" {
			t.Errorf("expected (three, true), got (%s, %v)", val, ok)
		}
	})

	t.Run("get element at negative index", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		val, ok := s.LIndex("mylist", -1)
		if !ok || val != "three" {
			t.Errorf("expected (three, true), got (%s, %v)", val, ok)
		}

		val, ok = s.LIndex("mylist", -3)
		if !ok || val != "one" {
			t.Errorf("expected (one, true), got (%s, %v)", val, ok)
		}
	})

	t.Run("index from non-existent key", func(t *testing.T) {
		s := NewStore()
		_, ok := s.LIndex("nonexistent", 0)
		if ok {
			t.Error("expected false for non-existent key")
		}
	})

	t.Run("index from wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		_, ok := s.LIndex("mykey", 0)
		if ok {
			t.Error("expected false for wrong type")
		}
	})
}

func TestLLen(t *testing.T) {
	t.Run("length of list with elements", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		length := s.LLen("mylist")
		if length != 3 {
			t.Errorf("expected 3, got %d", length)
		}
	})

	t.Run("length of non-existent key", func(t *testing.T) {
		s := NewStore()
		length := s.LLen("nonexistent")
		if length != 0 {
			t.Errorf("expected 0, got %d", length)
		}
	})

	t.Run("length of wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		length := s.LLen("mykey")
		if length != 0 {
			t.Errorf("expected 0 for wrong type, got %d", length)
		}
	})

	t.Run("length after operations", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four")
		s.LPop("mylist", 1)
		
		length := s.LLen("mylist")
		if length != 3 {
			t.Errorf("expected 3 after pop, got %d", length)
		}
	})
}

func TestLRem(t *testing.T) {
	t.Run("remove all occurrences with count 0", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "a", "b", "a", "c", "a")
		
		removed := s.LRem("mylist", 0, "a")
		if removed != 3 {
			t.Errorf("expected 3 removed, got %d", removed)
		}

		result := s.LRange("mylist", 0, -1)
		expected := []string{"b", "c"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("remove from head with positive count", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "a", "b", "a", "c", "a")
		
		removed := s.LRem("mylist", 2, "a")
		if removed != 2 {
			t.Errorf("expected 2 removed, got %d", removed)
		}

		result := s.LRange("mylist", 0, -1)
		expected := []string{"b", "c", "a"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("remove from tail with negative count", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "a", "b", "a", "c", "a")
		
		removed := s.LRem("mylist", -2, "a")
		if removed != 2 {
			t.Errorf("expected 2 removed, got %d", removed)
		}

		result := s.LRange("mylist", 0, -1)
		expected := []string{"a", "b", "c"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("remove all elements deletes key", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "a", "a")
		
		s.LRem("mylist", 0, "a")
		_, exists := s.Get("mylist")
		if exists {
			t.Error("expected key to be deleted when list is empty")
		}
	})

	t.Run("remove from non-existent key", func(t *testing.T) {
		s := NewStore()
		removed := s.LRem("nonexistent", 0, "a")
		if removed != 0 {
			t.Errorf("expected 0, got %d", removed)
		}
	})

	t.Run("remove from wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		removed := s.LRem("mykey", 0, "a")
		if removed != 0 {
			t.Errorf("expected 0 for wrong type, got %d", removed)
		}
	})
}

func TestLSet(t *testing.T) {
	t.Run("set element at positive index", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		err := s.LSet("mylist", 1, "new")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		result := s.LRange("mylist", 0, -1)
		expected := []string{"one", "new", "three"}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("set element at negative index", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		err := s.LSet("mylist", -1, "new")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		result := s.LRange("mylist", 0, -1)
		if result[2] != "new" {
			t.Errorf("expected last element to be 'new', got %s", result[2])
		}
	})

	t.Run("set on non-existent key", func(t *testing.T) {
		s := NewStore()
		err := s.LSet("nonexistent", 0, "value")
		if err == nil {
			t.Error("expected error for non-existent key")
		}
	})

	t.Run("set on wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		err := s.LSet("mykey", 0, "value")
		if err == nil {
			t.Error("expected error for wrong type")
		}
	})
}

func TestLTrim(t *testing.T) {
	t.Run("trim with positive indices", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four", "five")
		
		s.LTrim("mylist", 1, 3)
		result := s.LRange("mylist", 0, -1)
		expected := []string{"two", "three", "four"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("trim with negative indices", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three", "four", "five")
		
		s.LTrim("mylist", -3, -1)
		result := s.LRange("mylist", 0, -1)
		expected := []string{"three", "four", "five"}
		if len(result) != len(expected) {
			t.Fatalf("expected %d elements, got %d", len(expected), len(result))
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
			}
		}
	})

	t.Run("trim to empty list deletes key", func(t *testing.T) {
		s := NewStore()
		s.RPush("mylist", "one", "two", "three")
		
		s.LTrim("mylist", 5, 10)
		_, exists := s.Get("mylist")
		if exists {
			t.Error("expected key to be deleted when list is empty after trim")
		}
	})

	t.Run("trim on non-existent key", func(t *testing.T) {
		s := NewStore()
		s.LTrim("nonexistent", 0, 1)
		// Should not panic
	})

	t.Run("trim on wrong type", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "string_value")
		s.LTrim("mykey", 0, 1)
		// Should not panic
	})
}

// Integration test combining multiple operations
func TestListOperationsIntegration(t *testing.T) {
	s := NewStore()

	// Build a list
	s.RPush("mylist", "a", "b", "c")
	s.LPush("mylist", "z")
	
	if s.LLen("mylist") != 4 {
		t.Errorf("expected length 4, got %d", s.LLen("mylist"))
	}

	// Check content
	result := s.LRange("mylist", 0, -1)
	expected := []string{"z", "a", "b", "c"}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expected[i], result[i])
		}
	}

	// Pop from both ends
	s.LPop("mylist", 1)
	s.RPop("mylist", 1)
	
	if s.LLen("mylist") != 2 {
		t.Errorf("expected length 2 after pops, got %d", s.LLen("mylist"))
	}

	// Set and verify
	s.LSet("mylist", 0, "new")
	val, _ := s.LIndex("mylist", 0)
	if val != "new" {
		t.Errorf("expected 'new', got %s", val)
	}
}