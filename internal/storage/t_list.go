package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) LPush(key string, elements ...string) uint32 {
	rObj, exists := s.data[key]
	if exists {
		quickList, ok := rObj.Value.(data_structure.QuickList)
		if !ok {
			return 0
		}

		return quickList.LPush(elements)
	}

	quicklist := data_structure.NewQuickList()
	res := quicklist.LPush(elements)

	s.data[key] = &RObj{
		Type:     ObjList,
		Encoding: EncQuickList,
		Value:    quicklist,
	}

	return res
}

func (s *store) LPop(key string, count uint32) []string {
	rObj, exists := s.data[key]
	if !exists {
		return nil
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return nil
	}

	poppedElements := quickList.LPop(count)
	if quickList.Size() == 0 {
		delete(s.data, key)
	}

	return poppedElements
}

func (s *store) RPush(key string, elements ...string) uint32 {
	rObj, exists := s.data[key]
	if exists {
		quickList, ok := rObj.Value.(data_structure.QuickList)
		if !ok {
			return 0
		}

		return quickList.RPush(elements)
	}

	quicklist := data_structure.NewQuickList()
	res := quicklist.RPush(elements)

	s.data[key] = &RObj{
		Type:     ObjList,
		Encoding: EncQuickList,
		Value:    quicklist,
	}

	return res
}

func (s *store) RPop(key string, count uint32) []string {
	rObj, exists := s.data[key]
	if !exists {
		return nil
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return nil
	}

	poppedElements := quickList.RPop(count)
	if quickList.Size() == 0 {
		delete(s.data, key)
	}

	return poppedElements
}

func (s *store) LRange(key string, start int32, end int32) []string {
	rObj, exists := s.data[key]
	if !exists {
		return nil
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return nil
	}

	return quickList.LRange(start, end)
}

func (s *store) LIndex(key string, index int32) (string, bool) {
	rObj, existing := s.Get(key)
	if !existing {
		return "", false
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return "", false
	}

	return quickList.LIndex(index)
}

func (s *store) LLen(key string) uint32 {
	rObj, existing := s.Get(key)
	if !existing {
		return 0
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return 0
	}

	return quickList.Size()
}

func (s *store) LRem(key string, count int32, element string) uint32 {
	rObj, existing := s.Get(key)
	if !existing {
		return 0
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return 0
	}

	removedElements := quickList.LRem(count, element)
	if quickList.Size() == 0 {
		delete(s.data, key)
	}

	return removedElements
}

func (s *store) LSet(key string, index int32, element string) error {
	rObj, existing := s.Get(key)
	if !existing {
		return errors.New("no such key")
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return errors.New("invalid rObj, can not cast to quicklist")
	}

	return quickList.LSet(index, element)
}

func (s *store) LTrim(key string, start, end int32) {
	rObj, existing := s.Get(key)
	if !existing {
		return
	}

	quickList, ok := rObj.Value.(data_structure.QuickList)
	if !ok {
		return
	}

	quickList.LTrim(start, end)
	if quickList.Size() == 0 {
		delete(s.data, key)
	}
}
