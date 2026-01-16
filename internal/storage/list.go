package storage

import (
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) LPush(key string, elements ...string) (uint32, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.exists {
		quickList := result.object.value.(data_structure.QuickList)
		return quickList.LPush(elements), nil
	}

	quicklist := data_structure.NewQuickList()
	res := quicklist.LPush(elements)

	s.data.Set(key, &RObj{
		objType:     ObjList,
		encoding: EncQuickList,
		value:    quicklist,
	})

	return res, nil
}

func (s *store) LPop(key string, count uint32) ([]string, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	poppedElements := quickList.LPop(count)
	if quickList.Size() == 0 {
		s.delete(key)
	}

	return poppedElements, nil
}

func (s *store) RPush(key string, elements ...string) (uint32, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.exists {
		quickList := result.object.value.(data_structure.QuickList)
		return quickList.RPush(elements), nil
	}

	quicklist := data_structure.NewQuickList()
	res := quicklist.RPush(elements)

	s.data.Set(key, &RObj{
		objType:     ObjList,
		encoding: EncQuickList,
		value:    quicklist,
	})

	return res, nil
}

func (s *store) RPop(key string, count uint32) ([]string, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	poppedElements := quickList.RPop(count)
	if quickList.Size() == 0 {
		s.delete(key)
	}

	return poppedElements, nil
}

func (s *store) LRange(key string, start int32, end int32) ([]string, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return []string{}, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	return quickList.LRange(start, end), nil
}

func (s *store) LIndex(key string, index int32) (*string, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	val, succeeded := quickList.LIndex(index)
	if !succeeded {
		return nil, nil
	}

	return &val, nil
}

func (s *store) LLen(key string) (uint32, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.expired || !result.exists {
		return 0, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	return quickList.Size(), nil
}

func (s *store) LRem(key string, count int32, element string) (uint32, error) {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.expired || !result.exists {
		return 0, nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	removedElements := quickList.LRem(count, element)
	if quickList.Size() == 0 {
		s.delete(key)
	}

	return removedElements, nil
}

func (s *store) LSet(key string, index int32, element string) error {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return result.typeErr
	}

	if result.expired || !result.exists {
		return ErrKeyNotFoundError
	}

	quickList := result.object.value.(data_structure.QuickList)
	return quickList.LSet(index, element)
}

func (s *store) LTrim(key string, start, end int32) error {
	result := s.access(key, ObjList)
	if result.typeErr != nil {
		return result.typeErr
	}

	if result.expired || !result.exists {
		return nil
	}

	quickList := result.object.value.(data_structure.QuickList)
	quickList.LTrim(start, end)
	if quickList.Size() == 0 {
		s.delete(key)
	}

	return nil
}
