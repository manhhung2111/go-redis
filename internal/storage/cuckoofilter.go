package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/storage/types"
)

func (s *store) CFAdd(key string, item string) (int, error) {
	scf, err := s.getOrCreateCuckooFilter(key)
	if err != nil {
		return 0, err
	}

	result, delta := scf.Add(item)
	s.usedMemory += delta
	return result, nil
}

func (s *store) CFAddNx(key string, item string) (int, error) {
	scf, err := s.getOrCreateCuckooFilter(key)
	if err != nil {
		return 0, err
	}

	result, delta := scf.AddNx(item)
	s.usedMemory += delta
	return result, nil
}

func (s *store) CFCount(key string, item string) (int, error) {
	scf, err := s.getCuckooFilter(key, false)
	if err != nil {
		return 0, err
	}

	if scf == nil {
		return 0, nil
	}

	return scf.Count(item), nil
}

func (s *store) CFDel(key string, item string) (int, error) {
	scf, err := s.getCuckooFilter(key, true)
	if err != nil {
		return 0, err
	}

	if scf == nil {
		return 0, ErrKeyNotFoundError
	}

	result, delta := scf.Del(item)
	s.usedMemory += delta
	return result, nil
}

func (s *store) CFExists(key string, item string) (int, error) {
	scf, err := s.getCuckooFilter(key, false)
	if err != nil {
		return 0, err
	}

	if scf == nil {
		return 0, nil
	}

	return scf.Exists(item), nil
}

func (s *store) CFInfo(key string) ([]any, error) {
	scf, err := s.getCuckooFilter(key, false)
	if err != nil {
		return nil, err
	}

	if scf == nil {
		return nil, ErrKeyNotFoundError
	}

	return scf.Info(), nil
}

func (s *store) CFMExists(key string, items []string) ([]int, error) {
	scf, err := s.getCuckooFilter(key, false)
	if err != nil {
		return nil, err
	}

	if scf == nil {
		return make([]int, len(items)), nil
	}

	return scf.MExists(items), nil
}

func (s *store) CFReserve(key string, capacity uint64, bucketSize uint64, maxIterations uint64, expansionRate int) error {
	result := s.access(key, ObjAny, true)

	// Check if key exists (any type) - CF.RESERVE should fail if key already exists
	if result.exists {
		return errors.New("item exists")
	}

	scf := types.NewCuckooFilter(capacity, bucketSize, maxIterations, expansionRate)

	delta := s.data.Set(key, &RObj{
		objType:  ObjCuckooFilter,
		encoding: EncCuckooFilter,
		value:    scf,
	})
	s.usedMemory += delta

	return nil
}

func (s *store) getCuckooFilter(key string, isWrite bool) (types.CuckooFilter, error) {
	result := s.access(key, ObjCuckooFilter, isWrite)
	if result.err != nil {
		return nil, result.err
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	scf := result.object.value.(types.CuckooFilter)
	return scf, nil
}

func (s *store) getOrCreateCuckooFilter(key string) (types.CuckooFilter, error) {
	result := s.access(key, ObjCuckooFilter, true)
	if result.err != nil {
		return nil, result.err
	}

	if result.exists {
		return result.object.value.(types.CuckooFilter), nil
	}

	// Create new cuckoo filter with default settings
	scf := types.NewCuckooFilter(
		uint64(s.config.CFDefaultInitialSize),
		uint64(s.config.CFDefaultBucketSize),
		uint64(s.config.CFDefaultMaxIterations),
		s.config.CFDefaultExpansionFactor,
	)

	delta := s.data.Set(key, &RObj{
		objType:  ObjCuckooFilter,
		encoding: EncCuckooFilter,
		value:    scf,
	})
	s.usedMemory += delta

	return scf, nil
}
