package storage

import quicklist "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) LPush(key string, elements ...string) uint32 {
	rObj, exists := s.data[key]
	if exists {
		quickList, ok := rObj.Value.(quicklist.QuickList)
		if !ok {
			return 0
		}

		return quickList.LPush(elements)
	}

	quicklist := quicklist.NewQuickList()
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

	quickList, ok := rObj.Value.(quicklist.QuickList)
	if !ok {
		return nil
	}

	shouldDeleteKey := count >= quickList.Size()
	poppedElements := quickList.LPop(count)
	if shouldDeleteKey {
		delete(s.data, key)
	}

	return poppedElements
}

func (s *store) RPush(key string, elements ...string) uint32 {
	rObj, exists := s.data[key]
	if exists {
		quickList, ok := rObj.Value.(quicklist.QuickList)
		if !ok {
			return 0
		}

		return quickList.RPush(elements)
	}

	quicklist := quicklist.NewQuickList()
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

	quickList, ok := rObj.Value.(quicklist.QuickList)
	if !ok {
		return nil
	}

	shouldDeleteKey := count >= quickList.Size()
	poppedElements := quickList.RPop(count)
	if shouldDeleteKey {
		delete(s.data, key)
	}

	return poppedElements
}

func (s *store) LRange(key string, start int32, end int32) []string {
	rObj, exists := s.data[key]
	if !exists {
		return nil
	}

	quickList, ok := rObj.Value.(quicklist.QuickList)
	if !ok {
		return nil
	}

	return quickList.LRange(start, end)
}
