package data_structure

import (
	"testing"
)

func TestSimpleSetAdd(t *testing.T) {
	s := NewSimpleSet()
	
	// Add single member
	added := s.Add("a")
	if added != 1 {
		t.Errorf("Expected 1 added, got %d", added)
	}
	
	// Add duplicate member
	added = s.Add("a")
	if added != 0 {
		t.Errorf("Expected 0 added for duplicate, got %d", added)
	}
	
	// Add multiple members
	added = s.Add("b", "c", "d")
	if added != 3 {
		t.Errorf("Expected 3 added, got %d", added)
	}
	
	// Add mix of new and duplicate
	added = s.Add("d", "e", "f")
	if added != 2 {
		t.Errorf("Expected 2 added (d is duplicate), got %d", added)
	}
}

func TestSimpleSetAddEmpty(t *testing.T) {
	s := NewSimpleSet()
	
	added := s.Add()
	if added != 0 {
		t.Errorf("Expected 0 added for empty call, got %d", added)
	}
}

func TestSimpleSetSize(t *testing.T) {
	s := NewSimpleSet()
	
	// Empty set
	if s.Size() != 0 {
		t.Errorf("Expected size 0, got %d", s.Size())
	}
	
	// After adding
	s.Add("a", "b", "c")
	if s.Size() != 3 {
		t.Errorf("Expected size 3, got %d", s.Size())
	}
	
	// After adding duplicates
	s.Add("a", "b")
	if s.Size() != 3 {
		t.Errorf("Expected size 3 (no change), got %d", s.Size())
	}
}

func TestSimpleSetIsMember(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c")
	
	// Existing members
	if !s.IsMember("a") {
		t.Error("Expected 'a' to be a member")
	}
	if !s.IsMember("b") {
		t.Error("Expected 'b' to be a member")
	}
	if !s.IsMember("c") {
		t.Error("Expected 'c' to be a member")
	}
	
	// Non-existing members
	if s.IsMember("d") {
		t.Error("Expected 'd' to not be a member")
	}
	if s.IsMember("") {
		t.Error("Expected empty string to not be a member")
	}
}

func TestSimpleSetIsMemberEmpty(t *testing.T) {
	s := NewSimpleSet()
	
	if s.IsMember("a") {
		t.Error("Expected no members in empty set")
	}
}

func TestSimpleSetMIsMember(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c")
	
	result := s.MIsMember("a", "d", "b", "e", "c")
	expected := []bool{true, false, true, false, true}
	
	if len(result) != len(expected) {
		t.Fatalf("Expected length %d, got %d", len(expected), len(result))
	}
	
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Index %d: expected %v, got %v", i, expected[i], result[i])
		}
	}
}

func TestSimpleSetMIsMemberEmpty(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b")
	
	result := s.MIsMember()
	if len(result) != 0 {
		t.Errorf("Expected empty result, got length %d", len(result))
	}
}

func TestSimpleSetMIsMemberEmptySet(t *testing.T) {
	s := NewSimpleSet()
	
	result := s.MIsMember("a", "b", "c")
	expected := []bool{false, false, false}
	
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Index %d: expected %v, got %v", i, expected[i], result[i])
		}
	}
}

func TestSimpleSetMembers(t *testing.T) {
	s := NewSimpleSet()
	
	// Empty set
	members := s.Members()
	if len(members) != 0 {
		t.Errorf("Expected empty members, got %d", len(members))
	}
	
	// After adding
	s.Add("a", "b", "c")
	members = s.Members()
	
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}
	
	// Check all members are present (order doesn't matter for maps)
	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	
	if !memberMap["a"] || !memberMap["b"] || !memberMap["c"] {
		t.Errorf("Expected members [a, b, c], got %v", members)
	}
}

func TestSimpleSetDelete(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c", "d")
	
	// Delete existing member
	removed := s.Delete("a")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}
	if s.IsMember("a") {
		t.Error("'a' should have been removed")
	}
	
	// Delete non-existing member
	removed = s.Delete("z")
	if removed != 0 {
		t.Errorf("Expected 0 removed, got %d", removed)
	}
	
	// Delete multiple members
	removed = s.Delete("b", "c")
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}
	
	// Delete mix of existing and non-existing
	removed = s.Delete("d", "x", "y")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}
}

func TestSimpleSetDeleteEmpty(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a")
	
	removed := s.Delete()
	if removed != 0 {
		t.Errorf("Expected 0 removed for empty call, got %d", removed)
	}
}

func TestSimpleSetDeleteFromEmpty(t *testing.T) {
	s := NewSimpleSet()
	
	removed := s.Delete("a", "b")
	if removed != 0 {
		t.Errorf("Expected 0 removed from empty set, got %d", removed)
	}
}

func TestIntSetAdd(t *testing.T) {
	s := NewIntSet()
	
	// Add single member
	added := s.Add("1")
	if added != 1 {
		t.Errorf("Expected 1 added, got %d", added)
	}
	
	// Add duplicate
	added = s.Add("1")
	if added != 0 {
		t.Errorf("Expected 0 added for duplicate, got %d", added)
	}
	
	// Add multiple members
	added = s.Add("5", "3", "7")
	if added != 3 {
		t.Errorf("Expected 3 added, got %d", added)
	}
	
	// Verify sorted order
	members := s.Members()
	expected := []string{"1", "3", "5", "7"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetAddNegativeNumbers(t *testing.T) {
	s := NewIntSet()
	
	added := s.Add("-5", "-1", "0", "3")
	if added != 4 {
		t.Errorf("Expected 4 added, got %d", added)
	}
	
	members := s.Members()
	expected := []string{"-5", "-1", "0", "3"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetAddMaintainsSortedOrder(t *testing.T) {
	s := NewIntSet()
	
	// Add in random order
	s.Add("10")
	s.Add("5")
	s.Add("15")
	s.Add("3")
	s.Add("20")
	
	members := s.Members()
	expected := []string{"3", "5", "10", "15", "20"}
	
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetSize(t *testing.T) {
	s := NewIntSet()
	
	// Empty set
	if s.Size() != 0 {
		t.Errorf("Expected size 0, got %d", s.Size())
	}
	
	// After adding
	s.Add("1", "2", "3")
	if s.Size() != 3 {
		t.Errorf("Expected size 3, got %d", s.Size())
	}
	
	// After adding duplicates
	s.Add("1", "2")
	if s.Size() != 3 {
		t.Errorf("Expected size 3 (no change), got %d", s.Size())
	}
}

func TestIntSetIsMember(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "5", "10")
	
	// Existing members
	if !s.IsMember("1") {
		t.Error("Expected '1' to be a member")
	}
	if !s.IsMember("5") {
		t.Error("Expected '5' to be a member")
	}
	if !s.IsMember("10") {
		t.Error("Expected '10' to be a member")
	}
	
	// Non-existing members
	if s.IsMember("2") {
		t.Error("Expected '2' to not be a member")
	}
	if s.IsMember("100") {
		t.Error("Expected '100' to not be a member")
	}
}

func TestIntSetIsMemberEmpty(t *testing.T) {
	s := NewIntSet()
	
	if s.IsMember("1") {
		t.Error("Expected no members in empty set")
	}
}

func TestIntSetMIsMember(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "5", "10")
	
	result := s.MIsMember("1", "3", "5", "7", "10")
	expected := []bool{true, false, true, false, true}
	
	if len(result) != len(expected) {
		t.Fatalf("Expected length %d, got %d", len(expected), len(result))
	}
	
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Index %d: expected %v, got %v", i, expected[i], result[i])
		}
	}
}

func TestIntSetMIsMemberDuplicates(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")
	
	// Query with duplicates
	result := s.MIsMember("1", "1", "2", "2", "5")
	expected := []bool{true, true, true, true, false}
	
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Index %d: expected %v, got %v", i, expected[i], result[i])
		}
	}
}

func TestIntSetMIsMemberEmpty(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2")
	
	result := s.MIsMember()
	if len(result) != 0 {
		t.Errorf("Expected empty result, got length %d", len(result))
	}
}

func TestIntSetMIsMemberEmptySet(t *testing.T) {
	s := NewIntSet()
	
	result := s.MIsMember("1", "2", "3")
	expected := []bool{false, false, false}
	
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Index %d: expected %v, got %v", i, expected[i], result[i])
		}
	}
}

func TestIntSetMembers(t *testing.T) {
	s := NewIntSet()
	
	// Empty set
	members := s.Members()
	if len(members) != 0 {
		t.Errorf("Expected empty members, got %d", len(members))
	}
	
	// After adding
	s.Add("5", "1", "10", "3")
	members = s.Members()
	
	if len(members) != 4 {
		t.Errorf("Expected 4 members, got %d", len(members))
	}
	
	// Verify sorted order
	expected := []string{"1", "3", "5", "10"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetDelete(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3", "4", "5")
	
	// Delete existing member
	removed := s.Delete("3")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}
	if s.IsMember("3") {
		t.Error("'3' should have been removed")
	}
	
	// Verify order maintained
	members := s.Members()
	expected := []string{"1", "2", "4", "5"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetDeleteNonExisting(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")
	
	removed := s.Delete("10")
	if removed != 0 {
		t.Errorf("Expected 0 removed, got %d", removed)
	}
	
	// Size should remain unchanged
	if s.Size() != 3 {
		t.Errorf("Expected size 3, got %d", s.Size())
	}
}

func TestIntSetDeleteMultiple(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3", "4", "5")
	
	removed := s.Delete("2", "4")
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}
	
	members := s.Members()
	expected := []string{"1", "3", "5"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetDeleteMixed(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")
	
	// Mix of existing and non-existing
	removed := s.Delete("1", "10", "2", "20")
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}
	
	if s.Size() != 1 {
		t.Errorf("Expected size 1, got %d", s.Size())
	}
}

func TestIntSetDeleteEmpty(t *testing.T) {
	s := NewIntSet()
	s.Add("1")
	
	removed := s.Delete()
	if removed != 0 {
		t.Errorf("Expected 0 removed for empty call, got %d", removed)
	}
}

func TestIntSetDeleteFromEmpty(t *testing.T) {
	s := NewIntSet()
	
	removed := s.Delete("1", "2")
	if removed != 0 {
		t.Errorf("Expected 0 removed from empty set, got %d", removed)
	}
}

func TestIntSetDeleteFirst(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")
	
	removed := s.Delete("1")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}
	
	members := s.Members()
	expected := []string{"2", "3"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}

func TestIntSetDeleteLast(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")
	
	removed := s.Delete("3")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}
	
	members := s.Members()
	expected := []string{"1", "2"}
	for i := range expected {
		if members[i] != expected[i] {
			t.Errorf("Index %d: expected %s, got %s", i, expected[i], members[i])
		}
	}
}