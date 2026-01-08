package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) CFAdd(key string, item string) int {
	scf := s.getOrCreateCuckooFilter(key)
	return scf.Add(item)
}

func (s *store) CFAddNx(key string, item string) int {
	scf := s.getOrCreateCuckooFilter(key)
	return scf.AddNx(item)
}

func (s *store) CFCount(key string, item string) int {
	scf, exists := s.getCuckooFilter(key)
	if !exists {
		return 0
	}

	return scf.Count(item)
}

func (s *store) CFDel(key string, item string) int {
	scf, exists := s.getCuckooFilter(key)
	if !exists {
		return 0
	}

	return scf.Del(item)
}

func (s *store) CFExists(key string, item string) int {
	scf, exists := s.getCuckooFilter(key)
	if !exists {
		return 0
	}

	return scf.Exists(item)
}

func (s *store) CFInfo(key string) []any {
	scf, exists := s.getCuckooFilter(key)
	if !exists {
		return nil
	}

	return scf.Info()
}

func (s *store) CFMExists(key string, items []string) []int {
	scf, exists := s.getCuckooFilter(key)
	if !exists {
		return make([]int, len(items))
	}

	return scf.MExists(items)
}

func (s *store) CFReserve(key string, capacity uint64, bucketSize uint64, maxIterations uint64, expansionRate int) error {
	s.expireIfNeeded(key)

	// Check if key exists (any type) - CF.RESERVE should fail if key already exists
	if _, exists := s.data[key]; exists {
		return errors.New("item exists")
	}

	scf := data_structure.NewCuckooFilter(capacity, bucketSize, maxIterations, expansionRate)

	s.data[key] = &RObj{
		Type:     ObjCuckooFilter,
		Encoding: EncCuckooFilter,
		Value:    scf,
	}

	return nil
}

func (s *store) getCuckooFilter(key string) (data_structure.CuckooFilter, bool) {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	scf, ok := rObj.Value.(data_structure.CuckooFilter)
	if !ok {
		panic("Operation called on object not type CuckooFilter")
	}

	return scf, true
}

func (s *store) getOrCreateCuckooFilter(key string) data_structure.CuckooFilter {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if exists {
		scf, ok := rObj.Value.(data_structure.CuckooFilter)
		if !ok {
			panic("Operation called on object not type CuckooFilter")
		}
		return scf
	}

	// Create new cuckoo filter with default settings
	scf := data_structure.NewCuckooFilter(
		uint64(config.CF_DEFAULT_INITIAL_SIZE),
		uint64(config.CF_DEFAULT_BUCKET_SIZE),
		uint64(config.CF_DEFAULT_MAX_ITERATIONS),
		config.CF_DEFAULT_EXPANSION_FACTOR,
	)

	s.data[key] = &RObj{
		Type:     ObjCuckooFilter,
		Encoding: EncCuckooFilter,
		Value:    scf,
	}

	return scf
}
