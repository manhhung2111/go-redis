package data_structure

import (
	"errors"

	"github.com/DmitriyVTitov/size"
)

type QuickList interface {
	Size() uint32

	LPush(elements []string) (uint32, int64)
	LPop(count uint32) ([]string, int64)
	RPush(elements []string) (uint32, int64)
	RPop(count uint32) ([]string, int64)
	LRange(start, end int32) []string
	LIndex(index int32) (string, bool)
	LRem(count int32, element string) (uint32, int64)
	LSet(index int32, element string) (error, int64)
	LTrim(start, end int32) int64
	MemoryUsage() int64
}

type quickList struct {
	head *quickListNode // sentinel node
	tail *quickListNode // sentinel node
	size uint32
}

type quickListNode struct {
	next     *quickListNode
	prev     *quickListNode
	listPack *listPack
}

func NewQuickList() QuickList {
	head := &quickListNode{}
	tail := &quickListNode{}

	head.next = tail
	tail.prev = head

	return &quickList{
		head: head,
		tail: tail,
		size: 0,
	}
}

func newQuickListNode() *quickListNode {
	return &quickListNode{
		listPack: newListPack(),
	}
}

func (q *quickList) Size() uint32 {
	return q.size
}

func (q *quickList) LPush(elements []string) (uint32, int64) {
	if len(elements) == 0 {
		return q.size, 0
	}

	delta := int64(0)
	first := q.head.next

	// Create first node if list is empty
	if first == q.tail {
		first = newQuickListNode()
		first.prev = q.head
		first.next = q.tail
		q.head.next = first
		q.tail.prev = first
	}

	remaining := elements
	for len(remaining) > 0 {
		// Calculate how many elements can fit in current node
		canFit := 0
		projectedSize := first.listPack.approxSizeBytes()

		for canFit < len(remaining) {
			elemSize := stringHeaderSize + uint64(len(remaining[canFit]))
			if projectedSize+elemSize > listPackMaxSizeBytes {
				break
			}
			projectedSize += elemSize
			canFit++
		}

		if canFit > 0 {
			// Insert batch into current node
			for i := 0; i < canFit; i++ {
				delta += QuickListElementSize(remaining[i])
			}
			first.listPack.lPush(remaining[:canFit])
			q.size += uint32(canFit)
			remaining = remaining[canFit:]
		} else {
			// Current node is full and can't fit even one element
			// Create a new node and continue the loop
			newNode := newQuickListNode()
			newNode.prev = q.head
			newNode.next = first
			q.head.next = newNode
			first.prev = newNode
			first = newNode
		}
	}

	return q.size, delta
}

func (q *quickList) LPop(count uint32) ([]string, int64) {
	if count == 0 || q.size == 0 {
		return []string{}, 0
	}

	if count >= q.size {
		count = q.size
	}

	result := make([]string, 0, count)
	delta := int64(0)

	for count > 0 && q.size > 0 {
		first := q.head.next

		// Skip and remove empty nodes
		for first != q.tail && first.listPack.empty() {
			q.removeNode(first)
			first = q.head.next
		}

		if first == q.tail {
			break
		}

		// Batch pop from current node
		nodeSize := first.listPack.size()
		popCount := min(count, nodeSize)

		// Pop multiple elements at once
		for range popCount {
			elem := first.listPack.lPop()
			delta -= QuickListElementSize(elem)
			result = append(result, elem)
			q.size--
			count--
		}

		if first.listPack.empty() {
			q.removeNode(first)
		}
	}

	return result, delta
}

func (q *quickList) RPush(elements []string) (uint32, int64) {
	if len(elements) == 0 {
		return q.size, 0
	}

	delta := int64(0)
	last := q.tail.prev
	if last == q.head {
		last = newQuickListNode()
		last.prev = q.head
		last.next = q.tail
		q.head.next = last
		q.tail.prev = last
	}

	remaining := elements
	for len(remaining) > 0 {
		canFit := 0
		listPackSize := last.listPack.approxSizeBytes()

		for canFit < len(remaining) {
			elemSize := stringHeaderSize + uint64(len(remaining[canFit]))
			if listPackSize+elemSize > listPackMaxSizeBytes {
				break
			}
			listPackSize += elemSize
			canFit++
		}

		if canFit > 0 {
			for i := 0; i < canFit; i++ {
				delta += QuickListElementSize(remaining[i])
			}
			last.listPack.rPush(remaining[:canFit])
			q.size += uint32(canFit)
			remaining = remaining[canFit:]
		} else {
			newNode := newQuickListNode()
			newNode.next = q.tail
			q.tail.prev = newNode

			last.next = newNode
			newNode.prev = last

			last = newNode
		}
	}

	return q.size, delta
}

func (q *quickList) RPop(count uint32) ([]string, int64) {
	if count == 0 || q.size == 0 {
		return []string{}, 0
	}

	if count >= q.size {
		count = q.size
	}

	result := make([]string, 0, int(count))
	delta := int64(0)
	for count > 0 && q.size > 0 {
		last := q.tail.prev

		for last != q.head && last.listPack.empty() {
			q.removeNode(last)
			last = q.tail.prev
		}

		if last == q.head {
			break
		}

		nodeSize := last.listPack.size()
		popCount := min(count, nodeSize)

		for range popCount {
			elem := last.listPack.rPop()
			delta -= QuickListElementSize(elem)
			result = append(result, elem)
			count--
			q.size--
		}

		if last.listPack.empty() {
			q.removeNode(last)
		}
	}

	return result, delta
}

func (q *quickList) LRange(start, end int32) []string {
	qSize := int32(q.size)
	if qSize == 0 {
		return []string{}
	}

	if start < 0 {
		start = qSize + start
		if start < 0 {
			start = 0 // Clamp to 0
		}
	}

	if end < 0 {
		end = qSize + end
	}

	if end >= qSize {
		end = qSize - 1
	}

	if start >= qSize || start > end {
		return []string{}
	}

	node, index := q.findPosition(uint32(start))

	result := make([]string, 0, end-start+1)
	for i := int32(0); i < end-start+1 && node != q.tail; i++ {
		result = append(result, node.listPack.get(index))
		index++

		if index >= int32(node.listPack.size()) {
			index = 0
			node = node.next
		}
	}

	return result
}

func (q *quickList) LIndex(index int32) (string, bool) {
	qSize := int32(q.size)
	if qSize == 0 {
		return "", false
	}

	if index < 0 {
		index = qSize + index
	}

	if index < 0 || index >= int32(q.size) {
		return "", false
	}

	node, index := q.findPosition(uint32(index))
	return node.listPack.get(index), true
}

func (q *quickList) LRem(count int32, element string) (uint32, int64) {
	if q.size == 0 {
		return 0, 0
	}

	var removed uint32 = 0
	delta := int64(0)
	elemDelta := QuickListElementSize(element)
	absCount := count
	if absCount < 0 {
		absCount = -absCount
	}

	if count >= 0 {
		// Remove from head to tail
		node := q.head.next
		for node != q.tail {
			nextNode := node.next
			nodeRemoved := q.removeFromNode(node, element, absCount, removed, count == 0)
			removed += nodeRemoved
			delta -= int64(nodeRemoved) * elemDelta

			// Stop if we've removed enough (unless count is 0, meaning remove all)
			if count != 0 && removed >= uint32(absCount) {
				break
			}

			node = nextNode
		}
	} else {
		// Remove from tail to head (count < 0)
		node := q.tail.prev
		for node != q.head {
			prevNode := node.prev
			nodeRemoved := q.removeFromNodeReverse(node, element, absCount, removed)
			removed += nodeRemoved
			delta -= int64(nodeRemoved) * elemDelta

			if removed >= uint32(absCount) {
				break
			}

			node = prevNode
		}
	}

	return removed, delta
}

func (q *quickList) LSet(index int32, element string) (error, int64) {
	qSize := int32(q.size)
	if qSize == 0 {
		return nil, 0
	}

	if index < 0 {
		index = qSize + index
	}

	if index < 0 || index >= qSize {
		return errors.New("index out of range"), 0
	}

	node, localIndex := q.findPosition(uint32(index))
	oldElement := node.listPack.get(localIndex)
	node.listPack.set(localIndex, element)
	delta := QuickListElementSize(element) - QuickListElementSize(oldElement)
	return nil, delta
}

func (q *quickList) LTrim(start, end int32) int64 {
	qSize := int32(q.size)
	if qSize == 0 {
		return 0
	}

	if start < 0 {
		start = max(qSize+start, 0)
	}

	if end < 0 {
		end = qSize + end
	}

	if end >= qSize {
		end = qSize - 1
	}

	// If range is invalid, clear the entire list
	if start > end || start >= qSize {
		delta := q.clearWithDelta()
		return delta
	}

	delta := int64(0)

	// Remove elements before start
	if start > 0 {
		delta += q.removeFromHeadWithDelta(uint32(start))
	}

	// Remove elements after end
	newEnd := end - start
	newSize := int32(q.size)
	if newEnd < newSize-1 {
		toRemove := newSize - newEnd - 1
		delta += q.removeFromTailWithDelta(uint32(toRemove))
	}

	return delta
}

func (q *quickList) MemoryUsage() int64 {
	return int64(size.Of(q))
}

func (q *quickList) removeFromHead(count uint32) {
	for count > 0 && q.size > 0 {
		first := q.head.next
		if first == q.tail {
			break
		}

		nodeSize := first.listPack.size()
		removeCount := min(count, nodeSize)

		for i := uint32(0); i < removeCount; i++ {
			first.listPack.lPop()
			q.size--
			count--
		}

		// Remove node if empty
		if first.listPack.empty() {
			q.removeNode(first)
		}
	}
}

func (q *quickList) removeFromTail(count uint32) {
	for count > 0 && q.size > 0 {
		last := q.tail.prev
		if last == q.head {
			break
		}

		nodeSize := last.listPack.size()
		removeCount := min(count, nodeSize)

		for i := uint32(0); i < removeCount; i++ {
			last.listPack.rPop()
			q.size--
			count--
		}

		// Remove node if empty
		if last.listPack.empty() {
			q.removeNode(last)
		}
	}
}

func (q *quickList) clear() {
	node := q.head.next
	for node != q.tail {
		next := node.next
		node.prev = nil
		node.next = nil
		node = next
	}

	q.head.next = q.tail
	q.tail.prev = q.head
	q.size = 0
}

func (q *quickList) clearWithDelta() int64 {
	delta := int64(0)
	node := q.head.next
	for node != q.tail {
		// Calculate delta for all elements in this node
		for i := uint32(0); i < node.listPack.size(); i++ {
			delta -= QuickListElementSize(node.listPack.get(int32(i)))
		}
		next := node.next
		node.prev = nil
		node.next = nil
		node = next
	}

	q.head.next = q.tail
	q.tail.prev = q.head
	q.size = 0
	return delta
}

func (q *quickList) removeFromHeadWithDelta(count uint32) int64 {
	delta := int64(0)
	for count > 0 && q.size > 0 {
		first := q.head.next
		if first == q.tail {
			break
		}

		nodeSize := first.listPack.size()
		removeCount := min(count, nodeSize)

		for i := uint32(0); i < removeCount; i++ {
			elem := first.listPack.lPop()
			delta -= QuickListElementSize(elem)
			q.size--
			count--
		}

		// Remove node if empty
		if first.listPack.empty() {
			q.removeNode(first)
		}
	}
	return delta
}

func (q *quickList) removeFromTailWithDelta(count uint32) int64 {
	delta := int64(0)
	for count > 0 && q.size > 0 {
		last := q.tail.prev
		if last == q.head {
			break
		}

		nodeSize := last.listPack.size()
		removeCount := min(count, nodeSize)

		for i := uint32(0); i < removeCount; i++ {
			elem := last.listPack.rPop()
			delta -= QuickListElementSize(elem)
			q.size--
			count--
		}

		// Remove node if empty
		if last.listPack.empty() {
			q.removeNode(last)
		}
	}
	return delta
}

func (q *quickList) findPosition(index uint32) (*quickListNode, int32) {
	if index == 0 {
		return q.head.next, 0
	}

	if index == q.size-1 {
		last := q.tail.prev
		return last, int32(last.listPack.size() - 1)
	}

	leftNode := q.head.next
	rightNode := q.tail.prev
	leftAccum := uint32(0)
	rightAccum := q.size - 1

	for leftNode != q.tail {
		leftSize := leftNode.listPack.size()

		// Check if target is in current left node
		if index >= leftAccum && index < leftAccum+leftSize {
			return leftNode, int32(index - leftAccum)
		}

		// Check if target is in current right node
		if rightNode != leftNode {
			rightSize := rightNode.listPack.size()
			if index >= rightAccum-rightSize+1 && index <= rightAccum {
				localIdx := index - (rightAccum - rightSize + 1)
				return rightNode, int32(localIdx)
			}
			rightAccum -= rightSize
			rightNode = rightNode.prev
		}

		leftAccum += leftSize
		leftNode = leftNode.next

		// If pointers meet or cross, we've checked all nodes
		if leftNode == rightNode.next {
			break
		}
	}

	// This line should never be reached if index is valid
	panic("findPosition: index out of bounds")
}

func (q *quickList) removeNode(node *quickListNode) {
	if node == q.head || node == q.tail {
		return
	}

	prev := node.prev
	next := node.next

	prev.next = next
	next.prev = prev

	node.prev = nil
	node.next = nil
}

func (q *quickList) removeFromNode(node *quickListNode, element string, limit int32, alreadyRemoved uint32, removeAll bool) uint32 {
	removed := uint32(0)
	i := int32(0)

	for i < int32(node.listPack.size()) {
		if node.listPack.get(i) == element {
			node.listPack.removeAt(i)
			removed++
			q.size--

			if !removeAll && alreadyRemoved+removed >= uint32(limit) {
				break
			}
		} else {
			i++
		}
	}

	// Remove node if empty
	if node.listPack.empty() {
		q.removeNode(node)
	}

	return removed
}

func (q *quickList) removeFromNodeReverse(node *quickListNode, element string, limit int32, alreadyRemoved uint32) uint32 {
	removed := uint32(0)
	i := int32(node.listPack.size()) - 1

	for i >= 0 {
		if node.listPack.get(i) == element {
			node.listPack.removeAt(i)
			removed++
			q.size--

			if alreadyRemoved+removed >= uint32(limit) {
				break
			}
		}
		i--
	}

	// Remove node if empty
	if node.listPack.empty() {
		q.removeNode(node)
	}

	return removed
}
