package data_structure

import (
	"math/rand"
	"time"
)

const (
	skipListMaxLevel      = 32
	skipListPromotionProb = 0.25
)

type skipListLevel struct {
	forward *skipListNode
	span    int // number of level-0 forward moves. node --(span = k)--> forward
}

type skipListNode struct {
	value    string
	score    float64
	backward *skipListNode   // pointer to previous node (for reverse traversal)
	levels   []skipListLevel // array of levels with forward pointers and spans
}

type skipList struct {
	head   *skipListNode
	tail   *skipListNode
	level  int
	length int
	rand   *rand.Rand
}

func newSkipList() *skipList {
	head := &skipListNode{
		levels:   make([]skipListLevel, skipListMaxLevel),
		backward: nil,
	}

	return &skipList{
		head:   head,
		tail:   nil,
		level:  1,
		length: 0,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Precondition: caller must check if the element exists in list or not
func (sl *skipList) insert(value string, score float64) *skipListNode {
	update := make([]*skipListNode, skipListMaxLevel)
	rank := make([]int, skipListMaxLevel)
	current := sl.head

	// Find the position to update
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for current.levels[i].forward != nil &&
			(current.levels[i].forward.score < score ||
				(current.levels[i].forward.score == score && current.levels[i].forward.value < value)) {
			rank[i] += current.levels[i].span
			current = current.levels[i].forward
		}

		update[i] = current
	}

	// Create new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].levels[i].span = sl.length
		}
		sl.level = newLevel
	}

	newNode := &skipListNode{
		value:  value,
		score:  score,
		levels: make([]skipListLevel, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = newNode

		newNode.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}

	for i := newLevel; i < sl.level; i++ {
		update[i].levels[i].span++
	}

	if update[0] == sl.head {
		newNode.backward = nil
	} else {
		newNode.backward = update[0]
	}

	if newNode.levels[0].forward != nil {
		newNode.levels[0].forward.backward = newNode
	} else {
		sl.tail = newNode
	}

	sl.length++
	return newNode
}

// Precondition: element must exist in list with (value, oldScore)
func (sl *skipList) update(value string, oldScore float64, newScore float64) *skipListNode {
	update := make([]*skipListNode, skipListMaxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			(current.levels[i].forward.score < oldScore ||
				current.levels[i].forward.score == oldScore && current.levels[i].forward.value < value) {
			current = current.levels[i].forward
		}

		update[i] = current
	}

	current = current.levels[0].forward
	if (current.backward == nil || current.backward.score < newScore) &&
		(current.levels[0].forward == nil || current.levels[0].forward.score > newScore) {
		current.score = newScore
		return current
	}

	sl.deleteNode(current, update)
	newNode := sl.insert(value, newScore)
	return newNode
}

func (sl *skipList) delete(value string, score float64) bool {
	update := make([]*skipListNode, skipListMaxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			(current.levels[i].forward.score < score ||
				current.levels[i].forward.score == score && current.levels[i].forward.value < value) {
			current = current.levels[i].forward
		}

		update[i] = current
	}

	current = current.levels[0].forward
	if current == nil || current.value != value || current.score != score {
		return false
	}

	sl.deleteNode(current, update)
	return true
}

/* Preconditions:
- node exists in the skiplist
- update[i] is the last node before node at level i
*/
func (sl *skipList) deleteNode(node *skipListNode, update []*skipListNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].forward == node {
			// Node exists at this level and we point directly to it
			update[i].levels[i].span += node.levels[i].span - 1
			update[i].levels[i].forward = node.levels[i].forward
		} else {
			// Node doesn't exist at this level OR we don't point to it
			// In either case, just decrement span by 1 (one less node in the list)
			update[i].levels[i].span--
		}
	}

	// Update backward pointer
	if node.levels[0].forward != nil {
		node.levels[0].forward.backward = node.backward
	} else {
		sl.tail = node.backward
	}

	// Update level if necessary
	for sl.level > 1 && sl.head.levels[sl.level-1].forward == nil {
		sl.level--
	}

	sl.length--
}

// Returns the rank (0-indexed) of a value
func (sl *skipList) getRank(value string, score float64) int {
	rank := 0
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			(current.levels[i].forward.score < score ||
				(current.levels[i].forward.score == score && current.levels[i].forward.value < value)) {
			rank += current.levels[i].span
			current = current.levels[i].forward
		}

		// Check if we found the target node
		if current.levels[i].forward != nil &&
			current.levels[i].forward.score == score &&
			current.levels[i].forward.value == value {
			return rank + current.levels[i].span - 1 // Return 0-indexed rank
		}
	}

	return -1
}

// Returns nodes within score range [minScore, maxScore]
func (sl *skipList) getRangeByScore(minScore, maxScore float64) []*skipListNode {
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil && current.levels[i].forward.score < minScore {
			current = current.levels[i].forward
		}
	}

	current = current.levels[0].forward
	result := make([]*skipListNode, 0)

	for current != nil && current.score <= maxScore {
		result = append(result, current)
		current = current.levels[0].forward
	}

	return result
}

// Returns nodes within value range [minValue, maxValue]
func (sl *skipList) getRangeByLex(minValue, maxValue string) []*skipListNode {
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil && current.levels[i].forward.value < minValue {
			current = current.levels[i].forward
		}
	}

	current = current.levels[0].forward
	result := make([]*skipListNode, 0)

	for current != nil && current.value <= maxValue {
		result = append(result, current)
		current = current.levels[0].forward
	}

	return result
}

/*
- Returns nodes by rank [start, end] (0-indexed)
- Supports negative indices: -1 is last element, -2 is second to last, etc.
*/
func (sl *skipList) getRangeByRank(start, end int) []*skipListNode {
	result := make([]*skipListNode, 0)

	// Handle empty list
	if sl.length == 0 {
		return result
	}

	// Convert negative indices to positive
	if start < 0 {
		start = max(sl.length+start, 0)
	}

	if end < 0 {
		end = sl.length + end
		// If still negative after conversion, invalid range
		if end < 0 {
			return result
		}
	}

	// Validate bounds after conversion
	if start >= sl.length || start > end {
		return result
	}

	// Clamp end to valid range
	if end >= sl.length {
		end = sl.length - 1
	}

	// Use spans to jump directly to start position
	traversed := 0
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil && (traversed+current.levels[i].span) <= start {
			traversed += current.levels[i].span
			current = current.levels[i].forward
		}
	}

	// Move to start position
	current = current.levels[0].forward
	traversed++

	// Collect nodes in range
	for traversed <= end+1 && current != nil {
		result = append(result, current)
		current = current.levels[0].forward
		traversed++
	}

	return result
}

// Returns nodes by rank [start, end] (0-indexed) in reverse order
// Supports negative indices: -1 is last element, -2 is second to last, etc.
// Note: start and end still represent the range, but results are reversed
func (sl *skipList) getRevRangeByRank(start, end int) []*skipListNode {
	result := make([]*skipListNode, 0)

	if sl.length == 0 {
		return result
	}

	// Handle negative indices (relative to REV order)
	if start < 0 {
		start = sl.length + start
	}
	if end < 0 {
		end = sl.length + end
	}

	if start < 0 || end < 0 || start > end || start >= sl.length {
		return result
	}

	if end >= sl.length {
		end = sl.length - 1
	}

	// Convert REV ranks → forward ranks
	forwardStart := sl.length - 1 - end
	forwardEnd   := sl.length - 1 - start

	// Jump to forwardEnd (same logic as getRangeByRank)
	traversed := 0
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			(traversed+current.levels[i].span) <= forwardEnd {
			traversed += current.levels[i].span
			current = current.levels[i].forward
		}
	}

	// Move exactly to forwardEnd
	current = current.levels[0].forward
	traversed++

	// Walk backwards collecting [forwardStart, forwardEnd]
	for current != nil && traversed >= forwardStart+1 {
		result = append(result, current)
		current = current.backward
		traversed--
	}

	return result
}

func (sl *skipList) size() int {
	return sl.length
}

func (sl *skipList) randomLevel() int {
	level := 1
	for level < skipListMaxLevel && sl.rand.Float64() < skipListPromotionProb {
		level++
	}

	return level
}

func (sl *skipList) rankByScore(score float64) int {
	rank := 0
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			current.levels[i].forward.score < score {
			rank += current.levels[i].span
			current = current.levels[i].forward
		}
	}

	return rank
}

func (sl *skipList) countByScore(minScore, maxScore float64) int {
	if sl.length == 0 || minScore > maxScore {
		return 0
	}

	left := sl.rankByScore(minScore)

	// rank of first element with score > maxScore
	rank := 0
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.levels[i].forward != nil &&
			current.levels[i].forward.score <= maxScore {
			rank += current.levels[i].span
			current = current.levels[i].forward
		}
	}

	return rank - left
}