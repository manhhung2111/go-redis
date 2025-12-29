package data_structure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildSkipList(pairs ...struct {
	v string
	s float64
}) *skipList {
	sl := newSkipList()
	for _, p := range pairs {
		sl.insert(p.v, p.s)
	}
	return sl
}

func assertOrder(t *testing.T, sl *skipList, expected ...string) {
	t.Helper()
	cur := sl.head.levels[0].forward
	for _, v := range expected {
		require.NotNil(t, cur)
		assert.Equal(t, v, cur.value)
		cur = cur.levels[0].forward
	}
}

func TestSkipList_NewAndSize(t *testing.T) {
	sl := newSkipList()
	assert.NotNil(t, sl.head)
	assert.Nil(t, sl.tail)
	assert.Equal(t, 0, sl.size())

	sl.insert("a", 1)
	sl.insert("b", 2)
	assert.Equal(t, 2, sl.size())

	sl.delete("a", 1)
	sl.delete("b", 2)
	assert.Equal(t, 0, sl.size())
}

func TestSkipList_InsertOrdering(t *testing.T) {
	tests := []struct {
		name     string
		input    []struct{ v string; s float64 }
		expected []string
	}{
		{
			"ordered",
			[]struct{ v string; s float64 }{{"a", 1}, {"b", 2}, {"c", 3}},
			[]string{"a", "b", "c"},
		},
		{
			"unordered",
			[]struct{ v string; s float64 }{{"c", 3}, {"a", 1}, {"b", 2}},
			[]string{"a", "b", "c"},
		},
		{
			"same score",
			[]struct{ v string; s float64 }{{"z", 1}, {"a", 1}, {"m", 1}},
			[]string{"a", "m", "z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := buildSkipList(tt.input...)
			assertOrder(t, sl, tt.expected...)
			assert.Equal(t, len(tt.expected), sl.size())
		})
	}
}

func TestSkipList_Delete(t *testing.T) {
	sl := buildSkipList(
		struct{ v string; s float64 }{"a", 1},
		struct{ v string; s float64 }{"b", 2},
		struct{ v string; s float64 }{"c", 3},
	)

	assert.True(t, sl.delete("b", 2))
	assertOrder(t, sl, "a", "c")

	assert.False(t, sl.delete("x", 9))
	assert.False(t, sl.delete("a", 9))

	sl.delete("a", 1)
	sl.delete("c", 3)
	assert.Equal(t, 0, sl.size())
	assert.Nil(t, sl.tail)
}

func TestSkipList_DeleteNode_MultiLevelSpans(t *testing.T) {
	sl := newSkipList()

	// Force multiple levels by inserting many nodes
	for i := 0; i < 50; i++ {
		sl.insert(string(rune('a'+i)), float64(i))
	}

	oldLevel := sl.level
	require.Greater(t, oldLevel, 1)

	ok := sl.delete("m", 12)
	require.True(t, ok)

	// length decremented
	assert.Equal(t, 49, sl.size())

	// tail still valid
	assert.NotNil(t, sl.tail)

	// level may shrink (exercise level reduction loop)
	assert.LessOrEqual(t, sl.level, oldLevel)
}

func TestSkipList_Update(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		oldScore float64
		newScore float64
		expected []string
	}{
		{"no move", "b", 2, 2.5, []string{"a", "b", "c"}},
		{"forward", "b", 2, 4, []string{"a", "c", "b"}},
		{"backward", "c", 3, 0.5, []string{"c", "a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := buildSkipList(
				struct{ v string; s float64 }{"a", 1},
				struct{ v string; s float64 }{"b", 2},
				struct{ v string; s float64 }{"c", 3},
			)

			node := sl.update(tt.value, tt.oldScore, tt.newScore)
			require.NotNil(t, node)
			assertOrder(t, sl, tt.expected...)
		})
	}
}

func TestSkipList_GetRank(t *testing.T) {
	sl := buildSkipList(
		struct{ v string; s float64 }{"a", 1},
		struct{ v string; s float64 }{"b", 2},
		struct{ v string; s float64 }{"c", 3},
	)

	assert.Equal(t, 0, sl.getRank("a", 1))
	assert.Equal(t, 2, sl.getRank("c", 3))
	assert.Equal(t, -1, sl.getRank("x", 1))

	sl.delete("b", 2)
	assert.Equal(t, 1, sl.getRank("c", 3))
}

func TestSkipList_GetRank_SpanTraversal(t *testing.T) {
	sl := newSkipList()

	for i := 0; i < 100; i++ {
		sl.insert(string(rune('a'+i)), float64(i))
	}

	rank := sl.getRank("x", 23)
	assert.Equal(t, 23, rank)
}

func TestSkipList_GetRank_SameScoreDifferentValue(t *testing.T) {
	sl := newSkipList()

	sl.insert("apple", 1)
	sl.insert("banana", 1)
	sl.insert("cherry", 1)

	assert.Equal(t, 0, sl.getRank("apple", 1))
	assert.Equal(t, 1, sl.getRank("banana", 1))
	assert.Equal(t, 2, sl.getRank("cherry", 1))
}

func TestSkipList_GetRangeByScore(t *testing.T) {
	sl := buildSkipList(
		struct{ v string; s float64 }{"a", 1},
		struct{ v string; s float64 }{"b", 2},
		struct{ v string; s float64 }{"c", 3},
		struct{ v string; s float64 }{"d", 4},
	)

	assertOrder(t, &skipList{head: &skipListNode{levels: []skipListLevel{{forward: sl.getRangeByScore(2, 3)[0]}}}},
		"b", "c")

	assert.Len(t, sl.getRangeByScore(10, 20), 0)
}

func TestSkipList_GetRangeByScore_SeekAndStop(t *testing.T) {
	sl := newSkipList()

	for i := 0; i < 10; i++ {
		sl.insert(string(rune('a'+i)), float64(i))
	}

	nodes := sl.getRangeByScore(3.5, 6.2)

	require.Len(t, nodes, 3)
	assert.Equal(t, "e", nodes[0].value)
	assert.Equal(t, "g", nodes[2].value)
}

func TestSkipList_GetRangeByLex(t *testing.T) {
	sl := newSkipList()

	sl.insert("apple", 1)
	sl.insert("banana", 2)
	sl.insert("cherry", 3)
	sl.insert("date", 4)

	nodes := sl.getRangeByLex("banana", "date")

	require.Len(t, nodes, 3)
	assert.Equal(t, "banana", nodes[0].value)
	assert.Equal(t, "cherry", nodes[1].value)
	assert.Equal(t, "date", nodes[2].value)
}

func TestSkipList_GetRangeByLex_EmptyAndBounds(t *testing.T) {
	sl := newSkipList()

	assert.Len(t, sl.getRangeByLex("a", "z"), 0)

	sl.insert("mango", 1)

	assert.Len(t, sl.getRangeByLex("z", "zz"), 0)
	assert.Len(t, sl.getRangeByLex("a", "a"), 0)
}

func TestSkipList_GetRangeByRank(t *testing.T) {
	sl := buildSkipList(
		struct{ v string; s float64 }{"a", 1},
		struct{ v string; s float64 }{"b", 2},
		struct{ v string; s float64 }{"c", 3},
		struct{ v string; s float64 }{"d", 4},
	)

	tests := []struct {
		start, end int
		expected   []string
	}{
		{0, 1, []string{"a", "b"}},
		{1, 2, []string{"b", "c"}},
		{-2, -1, []string{"c", "d"}},
		{-4, -1, []string{"a", "b", "c", "d"}},
		{3, 10, []string{"d"}},
		{5, 6, nil},
	}

	for _, tt := range tests {
		nodes := sl.getRangeByRank(tt.start, tt.end)
		require.Len(t, nodes, len(tt.expected))
		for i, v := range tt.expected {
			assert.Equal(t, v, nodes[i].value)
		}
	}
}

func TestSkipList_GetRangeByRank_SpanJump(t *testing.T) {
	sl := newSkipList()

	for i := 0; i < 200; i++ {
		sl.insert(string(rune('a'+i%26)), float64(i))
	}

	nodes := sl.getRangeByRank(150, 155)

	require.Len(t, nodes, 6)
	assert.Equal(t, float64(150), nodes[0].score)
	assert.Equal(t, float64(155), nodes[5].score)
}

func TestSkipList_BackwardPointers(t *testing.T) {
	sl := buildSkipList(
		struct{ v string; s float64 }{"a", 1},
		struct{ v string; s float64 }{"b", 2},
		struct{ v string; s float64 }{"c", 3},
	)

	assert.Equal(t, "c", sl.tail.value)
	assert.Equal(t, "b", sl.tail.backward.value)
	assert.Equal(t, "a", sl.tail.backward.backward.value)
}

func TestSkipList_LargeDataset(t *testing.T) {
	sl := newSkipList()
	for i := 0; i < 1000; i++ {
		sl.insert(string(rune('a'+i%26)), float64(i))
	}
	assert.Equal(t, 1000, sl.size())
	assert.Len(t, sl.getRangeByRank(500, 599), 100)
}

func TestSkipList_GetRevRangeByRank_Basic(t *testing.T) {
	sl := newSkipList()
	sl.insert("one", 1)
	sl.insert("two", 2)
	sl.insert("three", 3)

	nodes := sl.getRevRangeByRank(0, 0)

	require.Len(t, nodes, 1)
	assert.Equal(t, "three", nodes[0].value)
}

func TestSkipList_GetRevRangeByRank_FirstTwo(t *testing.T) {
	sl := newSkipList()
	sl.insert("one", 1)
	sl.insert("two", 2)
	sl.insert("three", 3)

	nodes := sl.getRevRangeByRank(0, 1)

	require.Len(t, nodes, 2)
	assert.Equal(t, "three", nodes[0].value)
	assert.Equal(t, "two", nodes[1].value)
}

func TestSkipList_GetRevRangeByRank_Middle(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)
	sl.insert("e", 5)

	nodes := sl.getRevRangeByRank(1, 3)

	require.Len(t, nodes, 3)
	assert.Equal(t, "d", nodes[0].value)
	assert.Equal(t, "c", nodes[1].value)
	assert.Equal(t, "b", nodes[2].value)
}

func TestSkipList_GetRevRangeByRank_NegativeIndices(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)

	nodes := sl.getRevRangeByRank(-2, -1)

	require.Len(t, nodes, 2)
	assert.Equal(t, "b", nodes[0].value)
	assert.Equal(t, "a", nodes[1].value)
}

func TestSkipList_GetRevRangeByRank_All(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)

	nodes := sl.getRevRangeByRank(0, 2)

	require.Len(t, nodes, 3)
	assert.Equal(t, "c", nodes[0].value)
	assert.Equal(t, "b", nodes[1].value)
	assert.Equal(t, "a", nodes[2].value)
}

func TestSkipList_GetRevRangeByRank_OutOfBounds(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)

	nodes := sl.getRevRangeByRank(0, 10)

	require.Len(t, nodes, 2)
	assert.Equal(t, "b", nodes[0].value)
	assert.Equal(t, "a", nodes[1].value)
}

func TestSkipList_GetRevRangeByRank_InvalidRanges(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)

	assert.Len(t, sl.getRevRangeByRank(2, 3), 0)
	assert.Len(t, sl.getRevRangeByRank(1, 0), 0)
	assert.Len(t, sl.getRevRangeByRank(-5, -4), 0)
}

func TestSkipList_GetRevRangeByRank_Empty(t *testing.T) {
	sl := newSkipList()
	nodes := sl.getRevRangeByRank(0, 0)
	assert.Len(t, nodes, 0)
}

func TestSkipList_GetRevRangeByRank_AfterDelete(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)

	sl.delete("c", 3)

	nodes := sl.getRevRangeByRank(0, 2)

	require.Len(t, nodes, 3)
	assert.Equal(t, "d", nodes[0].value)
	assert.Equal(t, "b", nodes[1].value)
	assert.Equal(t, "a", nodes[2].value)
}

func TestSkipList_RankByScore(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 3)
	sl.insert("e", 5)

	tests := []struct {
		score    float64
		expected int
	}{
		{0, 0},   // below all
		{1, 0},   // equal lowest
		{2, 1},   // one element < 2
		{3, 2},   // elements < 3 (a,b)
		{4, 4},   // elements < 4 (a,b,c,d)
		{5, 4},   // equal highest
		{6, 5},   // above all
	}

	for _, tt := range tests {
		assert.Equal(
			t,
			tt.expected,
			sl.rankByScore(tt.score),
			"rankByScore(%v)", tt.score,
		)
	}
}

func TestSkipList_CountByScore(t *testing.T) {
	sl := newSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 3)
	sl.insert("e", 5)

	tests := []struct {
		min, max float64
		expected int
	}{
		{0, 10, 5},   // all
		{1, 1, 1},    // exact
		{3, 3, 2},    // duplicate scores
		{2, 4, 3},    // b,c,d
		{4, 4, 0},    // no matches
		{5, 5, 1},    // highest only
		{6, 10, 0},   // above all
		{0, 0, 0},    // below all
		{4, 2, 0},    // invalid range
	}

	for _, tt := range tests {
		assert.Equal(
			t,
			tt.expected,
			sl.countByScore(tt.min, tt.max),
			"countByScore(%v, %v)", tt.min, tt.max,
		)
	}
}

func TestSkipList_CountByScore_Empty(t *testing.T) {
	sl := newSkipList()

	assert.Equal(t, 0, sl.rankByScore(1))
	assert.Equal(t, 0, sl.countByScore(0, 10))
}

func TestSkipList_CountByScore_Large(t *testing.T) {
	sl := newSkipList()

	for i := 0; i < 1000; i++ {
		sl.insert(string(rune('a'+i%26)), float64(i))
	}

	assert.Equal(t, 1000, sl.rankByScore(1000))
	assert.Equal(t, 500, sl.countByScore(250, 749))
}

func TestSkipList_rankByLex_Basic(t *testing.T) {
	sl := newSkipList()

	// All scores are the same (REQUIRED)
	sl.insert("apple", 0)
	sl.insert("banana", 0)
	sl.insert("cherry", 0)
	sl.insert("date", 0)

	tests := []struct {
		value    string
		expected int
	}{
		{"apple", 0},
		{"banana", 1},
		{"cherry", 2},
		{"date", 3},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, sl.rankByLex(tt.value), "rankByLex(%q)", tt.value)
	}
}

func TestSkipList_rankByLex_NonExisting(t *testing.T) {
	sl := newSkipList()

	sl.insert("apple", 0)
	sl.insert("banana", 0)
	sl.insert("cherry", 0)

	tests := []struct {
		value    string
		expected int
	}{
		{"a", 0},        // before all
		{"apricot", 1},  // between apple and banana
		{"blueberry", 2},
		{"zoo", 3},      // after all
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, sl.rankByLex(tt.value), "rankByLex(%q)", tt.value)
	}
}

func TestSkipList_rankByLex_Empty(t *testing.T) {
	sl := newSkipList()
	assert.Equal(t, 0, sl.rankByLex("anything"))
}

func TestSkipList_countByLex_Basic(t *testing.T) {
	sl := newSkipList()

	sl.insert("apple", 0)
	sl.insert("banana", 0)
	sl.insert("cherry", 0)
	sl.insert("date", 0)

	tests := []struct {
		min, max string
		expected int
	}{
		{"apple", "date", 4},
		{"banana", "date", 3},
		{"banana", "cherry", 2},
		{"apple", "apple", 1},
		{"date", "date", 1},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, sl.countByLex(tt.min, tt.max), "countByLex(%q, %q)", tt.min, tt.max)
	}
}

func TestSkipList_countByLex_EmptyAndInvalidRanges(t *testing.T) {
	sl := newSkipList()

	sl.insert("apple", 0)
	sl.insert("banana", 0)

	tests := []struct {
		min, max string
		expected int
	}{
		{"x", "z", 0},        // above all
		{"a", "a", 0},        // exact miss
		{"banana", "apple", 0}, // invalid range
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, sl.countByLex(tt.min, tt.max), "countByLex(%q, %q)", tt.min, tt.max)
	}
}

func TestSkipList_countByLex_EmptySet(t *testing.T) {
	sl := newSkipList()
	assert.Equal(t, 0, sl.countByLex("a", "z"))
}

func TestSkipList_countByLex_MatchesRangeByLex(t *testing.T) {
	sl := newSkipList()

	values := []string{
		"apple", "banana", "cherry", "date", "fig", "grape",
	}

	for _, v := range values {
		sl.insert(v, 0)
	}

	min, max := "banana", "fig"

	rangeNodes := sl.getRangeByLex(min, max)
	count := sl.countByLex(min, max)

	assert.Equal(t, len(rangeNodes), count)
}

func TestSkipList_PopMin_Basic(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)

	nodes := sl.popMin(1)

	require.Len(t, nodes, 1)
	assert.Equal(t, "a", nodes[0].value)
	assert.Equal(t, 2, sl.size())

	// Remaining order
	assertOrder(t, sl, "b", "c")
}

func TestSkipList_PopMin_Multiple(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)

	nodes := sl.popMin(2)

	require.Len(t, nodes, 2)
	assert.Equal(t, "a", nodes[0].value)
	assert.Equal(t, "b", nodes[1].value)

	assert.Equal(t, 2, sl.size())
	assertOrder(t, sl, "c", "d")
}

func TestSkipList_PopMin_CountGreaterThanSize(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)

	nodes := sl.popMin(10)

	require.Len(t, nodes, 2)
	assert.Equal(t, "a", nodes[0].value)
	assert.Equal(t, "b", nodes[1].value)

	assert.Equal(t, 0, sl.size())
	assert.Nil(t, sl.tail)
}

func TestSkipList_PopMin_EmptyAndInvalid(t *testing.T) {
	sl := newSkipList()

	assert.Nil(t, sl.popMin(1))
	assert.Nil(t, sl.popMin(0))
	assert.Nil(t, sl.popMin(-1))
}

func TestSkipList_PopMax_Basic(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)

	nodes := sl.popMax(1)

	require.Len(t, nodes, 1)
	assert.Equal(t, "c", nodes[0].value)
	assert.Equal(t, 2, sl.size())

	assertOrder(t, sl, "a", "b")
}

func TestSkipList_PopMax_Multiple(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)

	nodes := sl.popMax(3)

	require.Len(t, nodes, 3)
	assert.Equal(t, "d", nodes[0].value)
	assert.Equal(t, "c", nodes[1].value)
	assert.Equal(t, "b", nodes[2].value)

	assert.Equal(t, 1, sl.size())
	assertOrder(t, sl, "a")
}

func TestSkipList_PopMax_CountGreaterThanSize(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)

	nodes := sl.popMax(5)

	require.Len(t, nodes, 2)
	assert.Equal(t, "b", nodes[0].value)
	assert.Equal(t, "a", nodes[1].value)

	assert.Equal(t, 0, sl.size())
	assert.Nil(t, sl.tail)
}

func TestSkipList_PopMax_EmptyAndInvalid(t *testing.T) {
	sl := newSkipList()

	assert.Nil(t, sl.popMax(1))
	assert.Nil(t, sl.popMax(0))
	assert.Nil(t, sl.popMax(-1))
}

func TestSkipList_PopMin_SameScoreLexOrder(t *testing.T) {
	sl := newSkipList()

	sl.insert("z", 1)
	sl.insert("a", 1)
	sl.insert("m", 1)

	nodes := sl.popMin(2)

	require.Len(t, nodes, 2)
	assert.Equal(t, "a", nodes[0].value)
	assert.Equal(t, "m", nodes[1].value)

	assertOrder(t, sl, "z")
}

func TestSkipList_PopMax_SameScoreLexOrder(t *testing.T) {
	sl := newSkipList()

	sl.insert("z", 1)
	sl.insert("a", 1)
	sl.insert("m", 1)

	nodes := sl.popMax(2)

	require.Len(t, nodes, 2)
	assert.Equal(t, "z", nodes[0].value)
	assert.Equal(t, "m", nodes[1].value)

	assertOrder(t, sl, "a")
}

func TestSkipList_PopMinMax_BackwardPointers(t *testing.T) {
	sl := newSkipList()

	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)
	sl.insert("d", 4)

	sl.popMin(1) // removes "a"
	sl.popMax(1) // removes "d"

	assertOrder(t, sl, "b", "c")
	require.NotNil(t, sl.tail)
	assert.Equal(t, "c", sl.tail.value)
	assert.Equal(t, "b", sl.tail.backward.value)
}
