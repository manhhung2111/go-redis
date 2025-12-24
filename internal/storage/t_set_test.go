package storage

import (
	"strconv"
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
)

func TestSAdd_IntSet_Creation(t *testing.T) {
	s := NewStore().(*store)
	added := s.SAdd("myset", "1", "2", "3")

	assertEqual(t, added, int64(3), "members added")
	assertEncoding(t, s, "myset", EncIntSet)
	assertEqual(t, s.SCard("myset"), int64(3), "set size")
}

func TestSAdd_SimpleSet_NonInteger(t *testing.T) {
	s := NewStore().(*store)
	added := s.SAdd("myset", "hello", "world")

	assertEqual(t, added, int64(2), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
}

func TestSAdd_SimpleSet_MixedValues(t *testing.T) {
	s := NewStore().(*store)
	added := s.SAdd("myset", "1", "hello", "2")

	assertEqual(t, added, int64(3), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
}

func TestSAdd_UpgradeToSimpleSet_NonInteger(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")
	assertEncoding(t, s, "myset", EncIntSet)

	added := s.SAdd("myset", "hello")
	assertEqual(t, added, int64(1), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
	assertEqual(t, s.SCard("myset"), int64(4), "set size after upgrade")

	// Verify all members preserved
	assertTrue(t, s.SIsMember("myset", "1"), "member 1")
	assertTrue(t, s.SIsMember("myset", "2"), "member 2")
	assertTrue(t, s.SIsMember("myset", "3"), "member 3")
	assertTrue(t, s.SIsMember("myset", "hello"), "member hello")
}

func TestSAdd_UpgradeToSimpleSet_CapacityExceeded(t *testing.T) {
	s := NewStore().(*store)

	// Fill to capacity
	members := make([]string, config.SET_MAX_INTSET_ENTRIES)
	for i := 0; i < config.SET_MAX_INTSET_ENTRIES; i++ {
		members[i] = strconv.Itoa(i)
	}
	s.SAdd("myset", members...)
	assertEncoding(t, s, "myset", EncIntSet)

	// Exceed capacity
	added := s.SAdd("myset", strconv.Itoa(config.SET_MAX_INTSET_ENTRIES))
	assertEqual(t, added, int64(1), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
	assertEqual(t, s.SCard("myset"), int64(config.SET_MAX_INTSET_ENTRIES+1), "size after upgrade")
}

func TestSAdd_SimpleSet_InitialBatchExceedsCapacity(t *testing.T) {
	s := NewStore().(*store)

	members := make([]string, config.SET_MAX_INTSET_ENTRIES+5)
	for i := 0; i < len(members); i++ {
		members[i] = strconv.Itoa(i)
	}

	s.SAdd("myset", members...)
	assertEncoding(t, s, "myset", EncHashTable)
}

func TestSAdd_NoDuplicates_IntSet(t *testing.T) {
	s := NewStore().(*store)
	added1 := s.SAdd("myset", "1", "2", "3")
	added2 := s.SAdd("myset", "2", "3", "4")

	assertEqual(t, added1, int64(3), "first add")
	assertEqual(t, added2, int64(1), "second add")
	assertEqual(t, s.SCard("myset"), int64(4), "total size")
}

func TestSAdd_NoDuplicates_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	added1 := s.SAdd("myset", "a", "b", "c")
	added2 := s.SAdd("myset", "b", "c", "d")

	assertEqual(t, added1, int64(3), "first add")
	assertEqual(t, added2, int64(1), "second add")
	assertEqual(t, s.SCard("myset"), int64(4), "total size")
}

func TestSAdd_NegativeIntegers(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "-5", "-1", "0", "1", "5")

	assertEncoding(t, s, "myset", EncIntSet)
	members := s.SMembers("myset")
	expected := []string{"-5", "-1", "0", "1", "5"}
	assertSliceEqual(t, members, expected, "sorted members")
}

func TestSCard_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	assertEqual(t, s.SCard("nonexistent"), int64(0), "size")
}

func TestSCard_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")
	assertEqual(t, s.SCard("myset"), int64(3), "size")
}

func TestSCard_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c", "d")
	assertEqual(t, s.SCard("myset"), int64(4), "size")
}

func TestSIsMember_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "5", "10")

	assertTrue(t, s.SIsMember("myset", "1"), "1")
	assertTrue(t, s.SIsMember("myset", "5"), "5")
	assertFalse(t, s.SIsMember("myset", "4"), "4")
	assertFalse(t, s.SIsMember("myset", "hello"), "hello")
}

func TestSIsMember_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "hello", "world", "foo")

	assertTrue(t, s.SIsMember("myset", "hello"), "hello")
	assertTrue(t, s.SIsMember("myset", "world"), "world")
	assertFalse(t, s.SIsMember("myset", "bar"), "bar")
}

func TestSIsMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	assertFalse(t, s.SIsMember("nonexistent", "anything"), "nonexistent key")
}

func TestSMIsMember_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "3", "5", "7", "9")

	results := s.SMIsMember("myset", "1", "2", "3", "4", "5")
	expected := []bool{true, false, true, false, true}
	assertBoolSliceEqual(t, results, expected, "membership results")
}

func TestSMIsMember_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "apple", "banana", "cherry")

	results := s.SMIsMember("myset", "apple", "orange", "banana")
	expected := []bool{true, false, true}
	assertBoolSliceEqual(t, results, expected, "membership results")
}

func TestSMIsMember_Empty(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2")

	results := s.SMIsMember("myset")
	assertEqual(t, len(results), 0, "empty query")
}

func TestSMIsMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	results := s.SMIsMember("nonexistent", "a", "b", "c")
	expected := []bool{false, false, false}
	assertBoolSliceEqual(t, results, expected, "nonexistent key")
}

func TestSMIsMember_Duplicates(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c")

	results := s.SMIsMember("myset", "a", "a", "b", "d", "a")
	expected := []bool{true, true, true, false, true}
	assertBoolSliceEqual(t, results, expected, "with duplicates")
}

func TestSMembers_IntSet_Sorted(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "9", "3", "7", "1", "5")

	members := s.SMembers("myset")
	expected := []string{"1", "3", "5", "7", "9"}
	assertSliceEqual(t, members, expected, "sorted members")
}

func TestSMembers_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "apple", "banana", "cherry")

	members := s.SMembers("myset")
	assertEqual(t, len(members), 3, "member count")

	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	assertTrue(t, memberMap["apple"] && memberMap["banana"] && memberMap["cherry"], "all members present")
}

func TestSMembers_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	members := s.SMembers("nonexistent")
	assertEqual(t, len(members), 0, "empty result")
}

func TestSRem_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	removed := s.SRem("myset", "2", "4")
	assertEqual(t, removed, int64(2), "removed count")
	assertEqual(t, s.SCard("myset"), int64(3), "remaining size")
	assertFalse(t, s.SIsMember("myset", "2"), "2 removed")
	assertFalse(t, s.SIsMember("myset", "4"), "4 removed")
	assertTrue(t, s.SIsMember("myset", "1"), "1 remains")
}

func TestSRem_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c", "d")

	removed := s.SRem("myset", "b", "d")
	assertEqual(t, removed, int64(2), "removed count")
	assertEqual(t, s.SCard("myset"), int64(2), "remaining size")
}

func TestSRem_NonexistentMembers(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	removed := s.SRem("myset", "4", "5")
	assertEqual(t, removed, int64(0), "removed count")
	assertEqual(t, s.SCard("myset"), int64(3), "size unchanged")
}

func TestSRem_MixedExistingNonexisting(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	removed := s.SRem("myset", "2", "4", "3")
	assertEqual(t, removed, int64(2), "removed count")
	assertEqual(t, s.SCard("myset"), int64(1), "remaining size")
}

func TestSRem_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	removed := s.SRem("nonexistent", "1", "2")
	assertEqual(t, removed, int64(0), "removed count")
}

func TestSPop_Basic(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	popped := s.SPop("myset", 2)
	assertEqual(t, len(popped), 2, "popped count")
	assertEqual(t, s.SCard("myset"), int64(3), "remaining size")

	for _, member := range popped {
		assertFalse(t, s.SIsMember("myset", member), "member removed")
	}
}

func TestSPop_EntireSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	popped := s.SPop("myset", 10)
	assertEqual(t, len(popped), 3, "popped count")

	_, exists := s.data["myset"]
	assertFalse(t, exists, "set deleted")
}

func TestSPop_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	popped := s.SPop("nonexistent", 5)
	assertEqual(t, len(popped), 0, "popped count")
}

func TestSPop_CountZero(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	popped := s.SPop("myset", 0)
	assertEqual(t, len(popped), 0, "popped count")
	assertEqual(t, s.SCard("myset"), int64(3), "size unchanged")
}

func TestSRandMember_PositiveCount(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	members := s.SRandMember("myset", 3)
	assertEqual(t, len(members), 3, "member count")

	// Verify uniqueness
	seen := make(map[string]bool)
	for _, m := range members {
		assertFalse(t, seen[m], "no duplicates")
		seen[m] = true
		assertTrue(t, s.SIsMember("myset", m), "valid member")
	}

	assertEqual(t, s.SCard("myset"), int64(5), "size unchanged")
}

func TestSRandMember_CountExceedsSize(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members := s.SRandMember("myset", 10)
	assertEqual(t, len(members), 3, "returns all members")
}

func TestSRandMember_NegativeCount(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members := s.SRandMember("myset", -10)
	assertEqual(t, len(members), 10, "member count")

	for _, m := range members {
		assertTrue(t, s.SIsMember("myset", m), "valid member")
	}
}

func TestSRandMember_CountZero(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members := s.SRandMember("myset", 0)
	assertEqual(t, len(members), 0, "empty result")
}

func TestSRandMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	members := s.SRandMember("nonexistent", 5)
	assertEqual(t, len(members), 0, "empty result")
}

func TestIntegration_UpgradePreservesAllOperations(t *testing.T) {
	s := NewStore().(*store)

	// Build IntSet
	s.SAdd("myset", "1", "2", "3", "4", "5")
	assertTrue(t, s.SIsMember("myset", "3"), "before upgrade")

	// Upgrade
	s.SAdd("myset", "hello")
	assertEncoding(t, s, "myset", EncHashTable)

	// All operations work
	assertTrue(t, s.SIsMember("myset", "3"), "after upgrade")
	assertTrue(t, s.SIsMember("myset", "hello"), "new member")

	removed := s.SRem("myset", "3", "hello")
	assertEqual(t, removed, int64(2), "removal")
	assertEqual(t, s.SCard("myset"), int64(4), "final size")
}

func TestIntegration_MultipleOperations(t *testing.T) {
	s := NewStore().(*store)

	s.SAdd("myset", "10", "20", "30")
	s.SRem("myset", "20")
	s.SAdd("myset", "40", "50")

	assertEqual(t, s.SCard("myset"), int64(4), "size")

	results := s.SMIsMember("myset", "10", "20", "30", "40", "50")
	expected := []bool{true, false, true, true, true}
	assertBoolSliceEqual(t, results, expected, "membership")
}

func assertEqual(t *testing.T, got, want interface{}, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}

func assertTrue(t *testing.T, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Errorf("%s: expected true, got false", msg)
	}
}

func assertFalse(t *testing.T, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Errorf("%s: expected false, got true", msg)
	}
}

func assertEncoding(t *testing.T, s *store, key string, expected ObjectEncoding) {
	t.Helper()
	rObj, exists := s.data[key]
	if !exists {
		t.Fatalf("key %s does not exist", key)
	}
	if rObj.Encoding != ObjectEncoding(expected) {
		t.Errorf("encoding: got %v, want %v", rObj.Encoding, expected)
	}
}

func assertSliceEqual(t *testing.T, got, want []string, msg string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length mismatch: got %d, want %d", msg, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s[%d]: got %s, want %s", msg, i, got[i], want[i])
		}
	}
}

func assertBoolSliceEqual(t *testing.T, got, want []bool, msg string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length mismatch: got %d, want %d", msg, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s[%d]: got %v, want %v", msg, i, got[i], want[i])
		}
	}
}
