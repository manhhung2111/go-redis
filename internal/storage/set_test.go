package storage

import (
	"strconv"
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSAdd_IntSet_Creation(t *testing.T) {
	s := NewStore().(*store)

	added, err := s.SAdd("myset", "1", "2", "3")

	assert.Equal(t, int64(3), added)
	assert.NoError(t, err)
	assertEncoding(t, s, "myset", EncIntSet)

	size, err := s.SCard("myset")
	assert.Equal(t, int64(3), size)
	assert.NoError(t, err)
}

func TestSAdd_SimpleSet_NonInteger(t *testing.T) {
	s := NewStore().(*store)
	added, err := s.SAdd("myset", "hello", "world")

	assert.NoError(t, err)
	assert.Equal(t, added, int64(2), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
}

func TestSAdd_SimpleSet_MixedValues(t *testing.T) {
	s := NewStore().(*store)
	added, err := s.SAdd("myset", "1", "hello", "2")

	assert.NoError(t, err)
	assert.Equal(t, added, int64(3), "members added")
	assertEncoding(t, s, "myset", EncHashTable)
}

func TestSAdd_UpgradeToSimpleSet_NonInteger(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")
	assertEncoding(t, s, "myset", EncIntSet)

	added, err := s.SAdd("myset", "hello")
	assert.NoError(t, err)
	assert.Equal(t, added, int64(1), "members added")
	assertEncoding(t, s, "myset", EncHashTable)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(4), "set size after upgrade")
	assert.NoError(t, err)

	// Verify all members preserved
	isMember, err := s.SIsMember("myset", "1")
	assert.True(t, isMember, "member 1")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "2")
	assert.True(t, isMember, "member 2")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "3")
	assert.True(t, isMember, "member 3")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "hello")
	assert.True(t, isMember, "member hello")
	assert.NoError(t, err)
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
	added, err := s.SAdd("myset", strconv.Itoa(config.SET_MAX_INTSET_ENTRIES))
	assert.Equal(t, added, int64(1), "members added")
	assert.NoError(t, err)

	assertEncoding(t, s, "myset", EncHashTable)
	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(config.SET_MAX_INTSET_ENTRIES+1), "size after upgrade")
	assert.NoError(t, err)
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
	added1, err1 := s.SAdd("myset", "1", "2", "3")
	added2, err2 := s.SAdd("myset", "2", "3", "4")

	assertEncoding(t, s, "myset", EncIntSet)

	assert.Equal(t, added1, int64(3), "first add")
	assert.NoError(t, err1)
	assert.Equal(t, added2, int64(1), "second add")
	assert.NoError(t, err2)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(4), "total size")
	assert.NoError(t, err)
}

func TestSAdd_NoDuplicates_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	added1, err1 := s.SAdd("myset", "a", "b", "c")
	added2, err2 := s.SAdd("myset", "b", "c", "d")

	assert.Equal(t, added1, int64(3), "first add")
	assert.NoError(t, err1)
	assert.Equal(t, added2, int64(1), "second add")
	assert.NoError(t, err2)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(4), "total size")
	assert.NoError(t, err)
}

func TestSAdd_NegativeIntegers(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "-5", "-1", "0", "1", "5")

	assertEncoding(t, s, "myset", EncIntSet)
	members, err := s.SMembers("myset")
	expected := []string{"-5", "-1", "0", "1", "5"}
	assert.Equal(t, members, expected, "sorted members")
	assert.NoError(t, err)
}

func TestSCard_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	size, err := s.SCard("nonexistent")
	assert.Equal(t, size, int64(0), "size")
	assert.NoError(t, err)
}

func TestSCard_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")
	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(3), "size")
	assert.NoError(t, err)
}

func TestSCard_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c", "d")
	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(4), "size")
	assert.NoError(t, err)
}

func TestSIsMember_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "5", "10")

	isMember, err := s.SIsMember("myset", "1")
	assert.True(t, isMember, "1")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "5")
	assert.True(t, isMember, "5")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "4")
	assert.False(t, isMember, "4")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "hello")
	assert.False(t, isMember, "hello")
	assert.NoError(t, err)
}

func TestSIsMember_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "hello", "world", "foo")

	isMember, err := s.SIsMember("myset", "hello")
	assert.True(t, isMember, "hello")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "world")
	assert.True(t, isMember, "world")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "bar")
	assert.False(t, isMember, "bar")
	assert.NoError(t, err)
}

func TestSIsMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)

	isMember, err := s.SIsMember("nonexistent", "anything")
	assert.False(t, isMember, "nonexistent key")
	assert.NoError(t, err)
}

func TestSMIsMember_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "3", "5", "7", "9")

	results, err := s.SMIsMember("myset", "1", "2", "3", "4", "5")
	expected := []bool{true, false, true, false, true}
	assert.Equal(t, results, expected, "membership results")
	assert.NoError(t, err)
}

func TestSMIsMember_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "apple", "banana", "cherry")

	results, err := s.SMIsMember("myset", "apple", "orange", "banana")
	expected := []bool{true, false, true}
	assert.Equal(t, results, expected, "membership results")
	assert.NoError(t, err)
}

func TestSMIsMember_Empty(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2")

	results, err := s.SMIsMember("myset")
	assert.Equal(t, len(results), 0, "empty query")
	assert.NoError(t, err)
}

func TestSMIsMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	results, err := s.SMIsMember("nonexistent", "a", "b", "c")
	expected := []bool{false, false, false}
	assert.Equal(t, results, expected, "nonexistent key")
	assert.NoError(t, err)
}

func TestSMIsMember_Duplicates(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c")

	results, err := s.SMIsMember("myset", "a", "a", "b", "d", "a")
	expected := []bool{true, true, true, false, true}
	assert.Equal(t, results, expected, "with duplicates")
	assert.NoError(t, err)
}

func TestSMembers_IntSet_Sorted(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "9", "3", "7", "1", "5")

	members, err := s.SMembers("myset")
	expected := []string{"1", "3", "5", "7", "9"}
	assert.Equal(t, members, expected, "sorted members")
	assert.NoError(t, err)
}

func TestSMembers_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "apple", "banana", "cherry")

	members, err := s.SMembers("myset")
	assert.Equal(t, len(members), 3, "member count")

	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	assert.True(t, memberMap["apple"] && memberMap["banana"] && memberMap["cherry"], "all members present")
	assert.NoError(t, err)
}

func TestSMembers_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	members, err := s.SMembers("nonexistent")
	assert.Equal(t, len(members), 0, "empty result")
	assert.NoError(t, err)
}

func TestSRem_IntSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	removed, err := s.SRem("myset", "2", "4")

	assert.Equal(t, removed, int64(2), "removed count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(3), "remaining size")
	assert.NoError(t, err)

	isMember, err := s.SIsMember("myset", "2")
	assert.False(t, isMember, "2 removed")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "4")
	assert.False(t, isMember, "4 removed")
	assert.NoError(t, err)

	isMember, err = s.SIsMember("myset", "1")
	assert.True(t, isMember, "1 remains")
	assert.NoError(t, err)
}

func TestSRem_SimpleSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "a", "b", "c", "d")

	removed, err := s.SRem("myset", "b", "d")
	assert.Equal(t, removed, int64(2), "removed count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(2), "remaining size")
	assert.NoError(t, err)
}

func TestSRem_NonexistentMembers(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	removed, err := s.SRem("myset", "4", "5")
	assert.Equal(t, removed, int64(0), "removed count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(3), "size unchanged")
	assert.NoError(t, err)
}

func TestSRem_MixedExistingNonexisting(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	removed, err := s.SRem("myset", "2", "4", "3")
	assert.Equal(t, removed, int64(2), "removed count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(1), "remaining size")
	assert.NoError(t, err)
}

func TestSRem_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	removed, err := s.SRem("nonexistent", "1", "2")
	assert.Equal(t, removed, int64(0), "removed count")
	assert.NoError(t, err)
}

func TestSPop_Basic(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	popped, err := s.SPop("myset", 2)
	assert.Equal(t, len(popped), 2, "popped count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(3), "remaining size")
	assert.NoError(t, err)

	for _, member := range popped {
		isMember, err := s.SIsMember("myset", member)
		assert.False(t, isMember, "member removed")
		assert.NoError(t, err)
	}
}

func TestSPop_EntireSet(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	popped, err := s.SPop("myset", 10)
	assert.Equal(t, len(popped), 3, "popped count")
	assert.NoError(t, err)

	_, exists := s.data["myset"]
	assert.False(t, exists, "set deleted")
}

func TestSPop_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	popped, err := s.SPop("nonexistent", 5)
	assert.Equal(t, len(popped), 0, "popped count")
	assert.NoError(t, err)
}

func TestSPop_CountZero(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	popped, err := s.SPop("myset", 0)
	assert.Equal(t, len(popped), 0, "popped count")
	assert.NoError(t, err)

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(3), "size unchanged")
	assert.NoError(t, err)
}

func TestSRandMember_PositiveCount(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3", "4", "5")

	members, err := s.SRandMember("myset", 3)
	assert.Equal(t, len(members), 3, "member count")
	assert.NoError(t, err)

	// Verify uniqueness
	seen := make(map[string]bool)
	for _, m := range members {
		assert.False(t, seen[m], "no duplicates")
		seen[m] = true

		isMember, err := s.SIsMember("myset", m)
		assert.True(t, isMember, "valid member")
		assert.NoError(t, err)
	}

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(5), "size unchanged")
	assert.NoError(t, err)
}

func TestSRandMember_CountExceedsSize(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members, err := s.SRandMember("myset", 10)
	assert.Equal(t, len(members), 3, "returns all members")
	assert.NoError(t, err)
}

func TestSRandMember_NegativeCount(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members, err := s.SRandMember("myset", -10)
	assert.Equal(t, len(members), 10, "member count")
	assert.NoError(t, err)

	for _, m := range members {
		isMember, err := s.SIsMember("myset", m)
		assert.True(t, isMember, "valid member")
		assert.NoError(t, err)
	}
}

func TestSRandMember_CountZero(t *testing.T) {
	s := NewStore().(*store)
	s.SAdd("myset", "1", "2", "3")

	members, err := s.SRandMember("myset", 0)
	assert.Equal(t, len(members), 0, "empty result")
	assert.NoError(t, err)
}

func TestSRandMember_NonexistentKey(t *testing.T) {
	s := NewStore().(*store)
	members, err := s.SRandMember("nonexistent", 5)
	assert.Equal(t, len(members), 0, "empty result")
	assert.NoError(t, err)
}

func TestIntegration_UpgradePreservesAllOperations(t *testing.T) {
	s := NewStore().(*store)

	// Build IntSet
	s.SAdd("myset", "1", "2", "3", "4", "5")

	isMember, err := s.SIsMember("myset", "3")
	assert.True(t, isMember, "before upgrade")
	assert.NoError(t, err)

	// Upgrade
	s.SAdd("myset", "hello")
	assertEncoding(t, s, "myset", EncHashTable)

	// All operations work
	isMember, _ = s.SIsMember("myset", "3")
	assert.True(t, isMember, "after upgrade")
	isMember, _ = s.SIsMember("myset", "hello")
	assert.True(t, isMember, "new member")

	removed, _ := s.SRem("myset", "3", "hello")
	assert.Equal(t, removed, int64(2), "removal")
	size, _ := s.SCard("myset")
	assert.Equal(t, size, int64(4), "final size")
}

func TestIntegration_MultipleOperations(t *testing.T) {
	s := NewStore().(*store)

	s.SAdd("myset", "10", "20", "30")
	s.SRem("myset", "20")
	s.SAdd("myset", "40", "50")

	size, err := s.SCard("myset")
	assert.Equal(t, size, int64(4), "size")
	assert.NoError(t, err)

	results, err := s.SMIsMember("myset", "10", "20", "30", "40", "50")
	expected := []bool{true, false, true, true, true}
	assert.Equal(t, results, expected, "membership")
	assert.NoError(t, err)
}

func assertEncoding(t *testing.T, s *store, key string, expected ObjectEncoding) {
	t.Helper()

	rObj, exists := s.data[key]
	require.True(t, exists, "key %s does not exist", key)

	assert.Equal(t, ObjectEncoding(expected), rObj.Encoding)
}
