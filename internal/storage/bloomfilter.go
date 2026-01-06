package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) BFAdd(key string, item string) int {
	sbf := s.getOrCreateBloomFilter(key)
	return sbf.Add(item)
}

func (s *store) BFCard(key string) int {
	sbf, exists := s.getBloomFilter(key)
	if !exists {
		return 0
	}

	return sbf.Card()
}

func (s *store) BFExists(key string, item string) int {
	sbf, exists := s.getBloomFilter(key)
	if !exists {
		return 0
	}

	return sbf.Exists(item)
}

func (s *store) BFInfo(key string, option int) []any {
	sbf, exists := s.getBloomFilter(key)
	if !exists {
		panic("BFInfo called on non-existing key")
	}

	return sbf.Info(option)
}

func (s *store) BFMAdd(key string, items []string) []int {
	sbf := s.getOrCreateBloomFilter(key)
	return sbf.MAdd(items)
}

func (s *store) BFMExists(key string, items []string) []int {
	sbf, exists := s.getBloomFilter(key)
	if !exists {
		return make([]int, len(items))
	}

	return sbf.MExists(items)
}

func (s *store) BFReserve(key string, errorRate float64, capacity uint32, expansion uint32) error {
	s.expireIfNeeded(key)

	// Check if key exists (any type) - BF.RESERVE should fail if key already exists
	if _, exists := s.data[key]; exists {
		return errors.New("item exists")
	}

	sbf := data_structure.NewScalableBloomFilter(errorRate, uint64(capacity), int(expansion))
	s.data[key] = &RObj{
		Type:     ObjBloomFilter,
		Encoding: EncBloomFilter,
		Value:    sbf,
	}

	return nil
}

func (s *store) getBloomFilter(key string) (data_structure.ScalableBloomFilter, bool) {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	sbf, ok := rObj.Value.(data_structure.ScalableBloomFilter)
	if !ok {
		panic("Operation called on object not type BloomFilter")
	}

	return sbf, true
}

// getOrCreateBloomFilter returns the bloom filter for the key, creating one with default settings if it doesn't exist.
// Panics if the key exists but holds a different type.
func (s *store) getOrCreateBloomFilter(key string) data_structure.ScalableBloomFilter {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if exists {
		sbf, ok := rObj.Value.(data_structure.ScalableBloomFilter)
		if !ok {
			panic("Operation called on object not type BloomFilter")
		}
		return sbf
	}

	// Create new bloom filter with default settings
	sbf := data_structure.NewScalableBloomFilter(
		config.BF_DEFAULT_ERROR_RATE,
		uint64(config.BF_DEFAULT_CAPACITY),
		config.BF_DEFAULT_EXPANSION,
	)

	s.data[key] = &RObj{
		Type:     ObjBloomFilter,
		Encoding: EncBloomFilter,
		Value:    sbf,
	}

	return sbf
}
