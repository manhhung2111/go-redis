package data_structure

import (
	"math/bits"
	"strconv"

	"github.com/DmitriyVTitov/size"
	"github.com/manhhung2111/go-redis/internal/config"
)

type intSet struct {
	contents []int64
}

func NewIntSet() Set {
	return &intSet{
		contents: make([]int64, 0, config.SET_MAX_INTSET_ENTRIES),
	}
}

func (set *intSet) Add(members ...string) (int64, bool) {
	added := 0
	oldContents := make([]int64, len(set.contents))
	copy(oldContents, set.contents)

	for i := range members {
		value, err := strconv.ParseInt(members[i], 10, 64)
		if err != nil {
			set.contents = oldContents
			return 0, false
		}

		insertionIndex, exists := set.getInsertIndex(value)
		if !exists {
			if len(set.contents)+1 > cap(set.contents) {
				set.contents = oldContents
				return 0, false
			}

			set.contents = append(set.contents, 0)
			copy(set.contents[insertionIndex+1:], set.contents[insertionIndex:])
			set.contents[insertionIndex] = value
			added++
		}
	}

	return int64(added), true
}

func (set *intSet) Size() int64 {
	return int64(len(set.contents))
}

func (set *intSet) IsMember(member string) bool {
	value, err := strconv.ParseInt(member, 10, 64)
	if err != nil {
		return false
	}
	return set.binarySearch(value) != -1
}

func (set *intSet) MIsMember(members ...string) []bool {
	membersLen := len(members)
	result := make([]bool, membersLen)

	if membersLen == 0 {
		return result
	}

	if set.shouldBinarySearch(membersLen) {
		for i := 0; i < membersLen; i++ {
			result[i] = set.IsMember(members[i])
		}
		return result
	}

	indexMap := make(map[string][]int, membersLen)
	for i, v := range members {
		indexMap[v] = append(indexMap[v], i)
	}

	for _, v := range set.contents {
		if idxs, ok := indexMap[strconv.FormatInt(v, 10)]; ok {
			for _, idx := range idxs {
				result[idx] = true
			}
		}
	}

	return result
}

func (set *intSet) Members() []string {
	n := len(set.contents)
	result := make([]string, n)

	for i := 0; i < n; i++ {
		result[i] = strconv.FormatInt(set.contents[i], 10)
	}

	return result
}

func (set *intSet) Delete(members ...string) int64 {
	removed := 0
	for i := range members {
		value, err := strconv.ParseInt(members[i], 10, 64)
		if err != nil {
			continue
		}

		index := set.binarySearch(value)
		if index != -1 {
			copy(set.contents[index:], set.contents[index+1:])
			set.contents = set.contents[:len(set.contents)-1]
			removed++
		}
	}

	return int64(removed)
}

func (set *intSet) MemoryUsage() int64 {
	return int64(size.Of(set))
}

func (set *intSet) binarySearch(target int64) int32 {
	low, high := int32(0), int32(len(set.contents)-1)
	index := int32(-1)

	for low <= high {
		mid := low + (high-low)/2

		if set.contents[mid] == target {
			return mid
		}

		if set.contents[mid] > target {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return index
}

func (set *intSet) shouldBinarySearch(membersLen int) bool {
	n := len(set.contents)
	if n == 0 {
		return false
	}
	logN := bits.Len(uint(n)) // ≈ log2(n) + 1
	return membersLen*logN < n
}

func (set *intSet) getInsertIndex(target int64) (int32, bool) {
	low, high := int32(0), int32(len(set.contents)-1)
	index := int32(0)

	for low <= high {
		mid := low + (high-low)/2

		if set.contents[mid] == target {
			return mid, true
		}

		if set.contents[mid] > target {
			high = mid - 1
		} else {
			low = mid + 1
			index = mid + 1
		}
	}

	return index, false
}
