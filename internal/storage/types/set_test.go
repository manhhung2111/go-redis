package types

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleSetAdd(t *testing.T) {
	s := NewSimpleSet()

	// Add single member
	added, ok, _ := s.Add("a")
	assert.True(t, ok)
	assert.Equal(t, int64(1), added)

	// Add duplicate member
	added, ok, _ = s.Add("a")
	assert.True(t, ok)
	assert.Equal(t, int64(0), added)

	// Add multiple members
	added, ok, _ = s.Add("b", "c", "d")
	assert.True(t, ok)
	assert.Equal(t, int64(3), added)

	// Add mix of new and duplicate
	added, ok, _ = s.Add("d", "e", "f")
	assert.True(t, ok)
	assert.Equal(t, int64(2), added, "d is duplicate")
}

func TestSimpleSetAddEmpty(t *testing.T) {
	s := NewSimpleSet()

	added, ok, _ := s.Add()
	assert.True(t, ok)
	assert.Equal(t, int64(0), added)
}

func TestSimpleSetSize(t *testing.T) {
	s := NewSimpleSet()

	// Empty set
	assert.Equal(t, int64(0), s.Size())

	// After adding
	s.Add("a", "b", "c")
	assert.Equal(t, int64(3), s.Size())

	// After adding duplicates
	s.Add("a", "b")
	assert.Equal(t, int64(3), s.Size(), "no change expected")
}

func TestSimpleSetIsMember(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c")

	// Existing members
	assert.True(t, s.IsMember("a"))
	assert.True(t, s.IsMember("b"))
	assert.True(t, s.IsMember("c"))

	// Non-existing members
	assert.False(t, s.IsMember("d"))
	assert.False(t, s.IsMember(""))
}

func TestSimpleSetIsMemberEmpty(t *testing.T) {
	s := NewSimpleSet()
	assert.False(t, s.IsMember("a"))
}

func TestSimpleSetMIsMember(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c")

	result := s.MIsMember("a", "d", "b", "e", "c")
	expected := []bool{true, false, true, false, true}

	assert.Equal(t, expected, result)
}

func TestSimpleSetMIsMemberEmpty(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b")

	result := s.MIsMember()
	assert.Empty(t, result)
}

func TestSimpleSetMIsMemberEmptySet(t *testing.T) {
	s := NewSimpleSet()

	result := s.MIsMember("a", "b", "c")
	expected := []bool{false, false, false}

	assert.Equal(t, expected, result)
}

func TestSimpleSetMembers(t *testing.T) {
	s := NewSimpleSet()

	// Empty set
	members := s.Members()
	assert.Empty(t, members)

	// After adding
	s.Add("a", "b", "c")
	members = s.Members()

	assert.Len(t, members, 3)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, members)
}

func TestSimpleSetDelete(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a", "b", "c", "d")

	// Delete existing member
	removed, _ := s.Delete("a")
	assert.Equal(t, int64(1), removed)
	assert.False(t, s.IsMember("a"))

	// Delete non-existing member
	removed, _ = s.Delete("z")
	assert.Equal(t, int64(0), removed)

	// Delete multiple members
	removed, _ = s.Delete("b", "c")
	assert.Equal(t, int64(2), removed)

	// Delete mix of existing and non-existing
	removed, _ = s.Delete("d", "x", "y")
	assert.Equal(t, int64(1), removed)
}

func TestSimpleSetDeleteEmpty(t *testing.T) {
	s := NewSimpleSet()
	s.Add("a")

	removed, _ := s.Delete()
	assert.Equal(t, int64(0), removed)
}

func TestSimpleSetDeleteFromEmpty(t *testing.T) {
	s := NewSimpleSet()

	removed, _ := s.Delete("a", "b")
	assert.Equal(t, int64(0), removed)
}

func TestIntSetAdd(t *testing.T) {
	s := NewIntSet()

	// Add single member
	added, ok, _ := s.Add("1")
	assert.True(t, ok)
	assert.Equal(t, int64(1), added)

	// Add duplicate
	added, ok, _ = s.Add("1")
	assert.True(t, ok)
	assert.Equal(t, int64(0), added)

	// Add multiple members
	added, ok, _ = s.Add("5", "3", "7")
	assert.True(t, ok)
	assert.Equal(t, int64(3), added)

	// Verify sorted order
	members := s.Members()
	assert.Equal(t, []string{"1", "3", "5", "7"}, members)
}

func TestIntSetAddNegativeNumbers(t *testing.T) {
	s := NewIntSet()

	added, ok, _ := s.Add("-5", "-1", "0", "3")
	assert.True(t, ok)
	assert.Equal(t, int64(4), added)

	members := s.Members()
	assert.Equal(t, []string{"-5", "-1", "0", "3"}, members)
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
	assert.Equal(t, []string{"3", "5", "10", "15", "20"}, members)
}

func TestIntSetSize(t *testing.T) {
	s := NewIntSet()

	// Empty set
	assert.Equal(t, int64(0), s.Size())

	// After adding
	s.Add("1", "2", "3")
	assert.Equal(t, int64(3), s.Size())

	// After adding duplicates
	s.Add("1", "2")
	assert.Equal(t, int64(3), s.Size(), "no change expected")
}

func TestIntSetIsMember(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "5", "10")

	// Existing members
	assert.True(t, s.IsMember("1"))
	assert.True(t, s.IsMember("5"))
	assert.True(t, s.IsMember("10"))

	// Non-existing members
	assert.False(t, s.IsMember("2"))
	assert.False(t, s.IsMember("100"))
}

func TestIntSetIsMemberEmpty(t *testing.T) {
	s := NewIntSet()
	assert.False(t, s.IsMember("1"))
}

func TestIntSetMIsMember(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "5", "10")

	result := s.MIsMember("1", "3", "5", "7", "10")
	expected := []bool{true, false, true, false, true}

	assert.Equal(t, expected, result)
}

func TestIntSetMIsMemberDuplicates(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	// Query with duplicates
	result := s.MIsMember("1", "1", "2", "2", "5")
	expected := []bool{true, true, true, true, false}

	assert.Equal(t, expected, result)
}

func TestIntSetMIsMemberEmpty(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2")

	result := s.MIsMember()
	assert.Empty(t, result)
}

func TestIntSetMIsMemberEmptySet(t *testing.T) {
	s := NewIntSet()

	result := s.MIsMember("1", "2", "3")
	expected := []bool{false, false, false}

	assert.Equal(t, expected, result)
}

func TestIntSetMembers(t *testing.T) {
	s := NewIntSet()

	// Empty set
	members := s.Members()
	assert.Empty(t, members)

	// After adding
	s.Add("5", "1", "10", "3")
	members = s.Members()

	assert.Len(t, members, 4)
	assert.Equal(t, []string{"1", "3", "5", "10"}, members)
}

func TestIntSetDelete(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3", "4", "5")

	// Delete existing member
	removed, _ := s.Delete("3")
	assert.Equal(t, int64(1), removed)
	assert.False(t, s.IsMember("3"))

	// Verify order maintained
	members := s.Members()
	assert.Equal(t, []string{"1", "2", "4", "5"}, members)
}

func TestIntSetDeleteNonExisting(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	removed, _ := s.Delete("10")
	assert.Equal(t, int64(0), removed)
	assert.Equal(t, int64(3), s.Size())
}

func TestIntSetDeleteMultiple(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3", "4", "5")

	removed, _ := s.Delete("2", "4")
	assert.Equal(t, int64(2), removed)

	members := s.Members()
	assert.Equal(t, []string{"1", "3", "5"}, members)
}

func TestIntSetDeleteMixed(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	// Mix of existing and non-existing
	removed, _ := s.Delete("1", "10", "2", "20")
	assert.Equal(t, int64(2), removed)
	assert.Equal(t, int64(1), s.Size())
}

func TestIntSetDeleteEmpty(t *testing.T) {
	s := NewIntSet()
	s.Add("1")

	removed, _ := s.Delete()
	assert.Equal(t, int64(0), removed)
}

func TestIntSetDeleteFromEmpty(t *testing.T) {
	s := NewIntSet()

	removed, _ := s.Delete("1", "2")
	assert.Equal(t, int64(0), removed)
}

func TestIntSetDeleteFirst(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	removed, _ := s.Delete("1")
	assert.Equal(t, int64(1), removed)

	members := s.Members()
	assert.Equal(t, []string{"2", "3"}, members)
}

func TestIntSetDeleteLast(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	removed, _ := s.Delete("3")
	assert.Equal(t, int64(1), removed)

	members := s.Members()
	assert.Equal(t, []string{"1", "2"}, members)
}

func TestIntSetAddInvalidString(t *testing.T) {
	s := NewIntSet()

	// Try to add invalid integer string
	added, ok, _ := s.Add("abc")
	assert.False(t, ok)
	assert.Equal(t, int64(0), added)
	assert.Equal(t, int64(0), s.Size())
}

func TestIntSetAddPartialFailure(t *testing.T) {
	s := NewIntSet()

	// Add mix of valid and invalid
	_, ok, _ := s.Add("1", "abc", "2")
	assert.False(t, ok)

	// Should not add any members on partial failure
	assert.Equal(t, int64(0), s.Size())
}

func TestIntSetAddCapacityExceeded(t *testing.T) {
	s := NewIntSet().(*intSet)

	// Fill to capacity
	capacity := cap(s.contents)
	for i := 0; i < capacity; i++ {
		added, ok, _ := s.Add(strconv.Itoa(i))
		require.True(t, ok, "Failed to add element %d", i)
		require.Equal(t, int64(1), added, "Failed to add element %d", i)
	}

	// Try to add one more (should fail)
	added, ok, _ := s.Add(strconv.Itoa(capacity))
	assert.False(t, ok)
	assert.Equal(t, int64(0), added)
	assert.Equal(t, int64(capacity), s.Size())
}

func TestIntSetIsMemberInvalidString(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	// Check with invalid string (should return false, not panic)
	assert.False(t, s.IsMember("abc"))
}

func TestIntSetDeleteInvalidString(t *testing.T) {
	s := NewIntSet()
	s.Add("1", "2", "3")

	// Try to delete invalid string (should be skipped)
	removed, _ := s.Delete("abc", "2", "xyz")
	assert.Equal(t, int64(1), removed, "only '2' should be removed")

	// Verify '2' was deleted but others remain
	assert.False(t, s.IsMember("2"))
	assert.True(t, s.IsMember("1"))
	assert.True(t, s.IsMember("3"))
}

func TestSimpleSetAlwaysReturnsTrue(t *testing.T) {
	s := NewSimpleSet()

	// SimpleSet should always return true for ok
	_, ok, _ := s.Add("a", "b", "c")
	assert.True(t, ok, "SimpleSet Add should always return true")

	_, ok, _ = s.Add()
	assert.True(t, ok, "SimpleSet Add should always return true, even for empty")
}
