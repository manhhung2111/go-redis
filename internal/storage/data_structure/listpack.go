package data_structure

import (
	"github.com/manhhung2111/go-redis/internal/util"
)

const (
	sliceHeaderSize      uint64 = 24
	stringHeaderSize     uint64 = 16
	listPackMaxSizeBytes uint64 = 8 * 1024 // 8KiB default
)

type listPack struct {
	data []string
}

func newListPack() *listPack {
	return &listPack{
		data: make([]string, 0),
	}
}

func (lp *listPack) empty() bool {
	return len(lp.data) == 0
}

func (lp *listPack) get(index int32) string {
	if index < 0 || index >= int32(len(lp.data)) {
		panic("get called on invalid index of listPack")
	}

	return lp.data[index]
}

func (lp *listPack) size() uint32 {
	return uint32(len(lp.data))
}

func (lp *listPack) lPush(elements []string) {
	if len(elements) == 0 {
		return
	}

	oldLen := len(lp.data)
	newLen := oldLen + len(elements)

	if cap(lp.data) >= newLen {
		lp.data = lp.data[:newLen]
		// Only do the shift when reusing capacity
		if oldLen > 0 {
			copy(lp.data[len(elements):], lp.data[:oldLen])
		}
	} else {
		newData := make([]string, newLen)
		// Copy old data directly to final position
		if oldLen > 0 {
			copy(newData[len(elements):], lp.data)
		}
		lp.data = newData
	}

	util.ReverseSlice(elements)
	copy(lp.data, elements)
}

func (lp *listPack) lPop() string {
	if len(lp.data) == 0 {
		panic("lPop called on empty listPack")
	}

	val := lp.data[0]
	lp.data = lp.data[1:]
	return val
}

func (lp *listPack) rPush(elements []string) {
	if len(elements) == 0 {
		return
	}

	oldLen := len(lp.data)
	newLen := oldLen + len(elements)

	if cap(lp.data) >= newLen {
		lp.data = lp.data[:newLen]
	} else {
		newData := make([]string, newLen)
		copy(newData, lp.data)
		lp.data = newData
	}

	copy(lp.data[oldLen:], elements)
}

func (lp *listPack) rPop() string {
	if len(lp.data) == 0 {
		panic("rPop called on empty listPack")
	}

	last := lp.data[len(lp.data)-1]
	lp.data = lp.data[:len(lp.data)-1]
	return last
}

func (lp *listPack) approxSizeBytes() uint64 {
	var totalSize uint64 = sliceHeaderSize
	for _, s := range lp.data {
		totalSize += stringHeaderSize
		totalSize += uint64(len(s))
	}

	return totalSize
}

func (lp *listPack) removeAt(index int32) {
	if index < 0 || index >= int32(len(lp.data)) {
		panic("removeAt called on invalid index of listPack")
	}

	// Shift elements left to fill the gap
	copy(lp.data[index:], lp.data[index+1:])
	// Shrink the slice
	lp.data = lp.data[:len(lp.data)-1]
}

func (lp *listPack) set(index int32, value string) {
	if index < 0 || index >= int32(len(lp.data)) {
		panic("set called on invalid index of listPack")
	}

	lp.data[index] = value
}
