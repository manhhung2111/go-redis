package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) CMSIncrBy(key string, itemIncrement map[string]uint64) []uint64 {
	cms, exists := s.getCountMinSketch(key)
	if !exists {
		return nil
	}

	return cms.IncrBy(itemIncrement)
}

func (s *store) CMSInfo(key string) []any {
	cms, exists := s.getCountMinSketch(key)
	if !exists {
		return nil
	}

	return cms.Info()
}

func (s *store) CMSInitByDim(key string, width uint64, depth uint64) error {
	s.expireIfNeeded(key)

	if _, exists := s.data[key]; exists {
		return errors.New("CMS: key already exists")
	}

	cms := data_structure.NewCountMinSketchByDim(int(width), int(depth))
	s.data[key] = &RObj{
		Type:     ObjCountMinSketch,
		Encoding: EncCountMinSketch,
		Value:    cms,
	}

	return nil
}

func (s *store) CMSInitByProb(key string, errorRate float64, probability float64) error {
	s.expireIfNeeded(key)

	if _, exists := s.data[key]; exists {
		return errors.New("CMS: key already exists")
	}

	cms := data_structure.NewCountMinSketchByProb(errorRate, probability)
	s.data[key] = &RObj{
		Type:     ObjCountMinSketch,
		Encoding: EncCountMinSketch,
		Value:    cms,
	}

	return nil
}

func (s *store) CMSQuery(key string, items []string) []uint64 {
	cms, exists := s.getCountMinSketch(key)
	if !exists {
		return make([]uint64, len(items))
	}

	return cms.Query(items)
}

func (s *store) getCountMinSketch(key string) (data_structure.CountMinSketch, bool) {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	cms, ok := rObj.Value.(data_structure.CountMinSketch)
	if !ok {
		panic("Operation called on object not type CountMinSketch")
	}

	return cms, true
}
