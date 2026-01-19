package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/storage/types"
)

func (s *store) BFAdd(key string, item string) (int, error) {
	sbf, err := s.getOrCreateBloomFilter(key)
	if err != nil {
		return 0, err
	}

	result, delta := sbf.Add(item)
	s.usedMemory += delta
	return result, nil
}

func (s *store) BFCard(key string) (int, error) {
	sbf, err := s.getBloomFilter(key)
	if err != nil {
		return 0, err
	}

	if sbf == nil {
		return 0, nil
	}

	return sbf.Card(), nil
}

func (s *store) BFExists(key string, item string) (int, error) {
	sbf, err := s.getBloomFilter(key)
	if err != nil {
		return 0, err
	}

	if sbf == nil {
		return 0, nil
	}

	return sbf.Exists(item), nil
}

func (s *store) BFInfo(key string, option int) ([]any, error) {
	sbf, err := s.getBloomFilter(key)
	if err != nil {
		return nil, err
	}

	if sbf == nil {
		return nil, ErrKeyNotFoundError
	}

	return sbf.Info(option), nil
}

func (s *store) BFMAdd(key string, items []string) ([]int, error) {
	sbf, err := s.getOrCreateBloomFilter(key)
	if err != nil {
		return nil, err
	}

	result, delta := sbf.MAdd(items)
	s.usedMemory += delta
	return result, nil
}

func (s *store) BFMExists(key string, items []string) ([]int, error) {
	sbf, err := s.getBloomFilter(key)
	if err != nil {
		return nil, err
	}

	if sbf == nil {
		return make([]int, len(items)), nil
	}

	return sbf.MExists(items), nil
}

func (s *store) BFReserve(key string, errorRate float64, capacity uint32, expansion uint32) error {
	result := s.access(key, ObjAny, true)

	// Check if key exists (any type) - BF.RESERVE should fail if key already exists
	if result.exists {
		return errors.New("item exists")
	}

	sbf := types.NewScalableBloomFilter(errorRate, uint64(capacity), int(expansion))

	delta := s.data.Set(key, &RObj{
		objType:  ObjBloomFilter,
		encoding: EncBloomFilter,
		value:    sbf,
	})

	s.usedMemory += delta
	return nil
}

func (s *store) getBloomFilter(key string) (types.ScalableBloomFilter, error) {
	result := s.access(key, ObjBloomFilter, false)
	if result.err != nil {
		return nil, result.err
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	sbf := result.object.value.(types.ScalableBloomFilter)
	return sbf, nil
}

// getOrCreateBloomFilter returns the bloom filter for the key, creating one with default settings if it doesn't exist.
func (s *store) getOrCreateBloomFilter(key string) (types.ScalableBloomFilter, error) {
	result := s.access(key, ObjBloomFilter, true)
	if result.err != nil {
		return nil, result.err
	}

	if result.exists {
		return result.object.value.(types.ScalableBloomFilter), nil
	}

	// Create new bloom filter with default settings
	sbf := types.NewScalableBloomFilter(
		s.config.BFDefaultErrorRate,
		uint64(s.config.BFDefaultCapacity),
		s.config.BFDefaultExpansion,
	)

	delta := s.data.Set(key, &RObj{
		objType:  ObjBloomFilter,
		encoding: EncBloomFilter,
		value:    sbf,
	})

	s.usedMemory += delta
	return sbf, nil
}
