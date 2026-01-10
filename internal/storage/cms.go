package storage

import (
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) CMSIncrBy(key string, itemIncrement map[string]uint64) ([]uint64, error) {
	cms, err := s.getCountMinSketch(key)
	if err != nil {
		return nil, err
	}

	return cms.IncrBy(itemIncrement), nil
}

func (s *store) CMSInfo(key string) ([]any, error) {
	cms, err := s.getCountMinSketch(key)
	if err != nil {
		return nil, err
	}

	return cms.Info(), nil
}

func (s *store) CMSInitByDim(key string, width uint64, depth uint64) error {
	result := s.access(key, ObjAny)

	if result.exists {
		return ErrCmSKeyAlreadyExistsError
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
	result := s.access(key, ObjAny)

	if result.exists {
		return ErrCmSKeyAlreadyExistsError
	}

	cms := data_structure.NewCountMinSketchByProb(errorRate, probability)
	s.data[key] = &RObj{
		Type:     ObjCountMinSketch,
		Encoding: EncCountMinSketch,
		Value:    cms,
	}

	return nil
}

func (s *store) CMSQuery(key string, items []string) ([]uint64, error) {
	cms, err := s.getCountMinSketch(key)
	if err != nil {
		return nil, err
	}

	return cms.Query(items), nil
}

func (s *store) getCountMinSketch(key string) (data_structure.CountMinSketch, error) {
	result := s.access(key, ObjCountMinSketch)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, ErrCmSKeyDoesNotExistError
	}

	cms := result.object.Value.(data_structure.CountMinSketch)
	return cms, nil
}
