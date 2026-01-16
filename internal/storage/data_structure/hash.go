package data_structure

import (
	"errors"
	"math"
	"strconv"
	"github.com/DmitriyVTitov/size"
)

type Hash interface {
	Get(key string) (string, bool)
	MGet(keys ...string) []*string
	GetAll() []string
	GetKeys() []string
	GetValues() []string
	Size() uint32
	IncBy(key string, increment int64) (int64, error)
	Set(fieldValue map[string]string) int64
	SetNX(key, value string) bool
	Delete(keys ...string) int64
	Exists(key string) bool
	MemoryUsage() int64
}

type simpleHash struct {
	contents map[string]string
}

func NewHash() Hash {
	return &simpleHash{
		contents: make(map[string]string),
	}
}

func (s *simpleHash) Get(key string) (string, bool) {
	value, exists := s.contents[key]
	return value, exists
}

func (s *simpleHash) GetAll() []string {
	if len(s.contents) == 0 {
		return []string{}
	}

	result := make([]string, 0, len(s.contents)*2)

	for key, value := range s.contents {
		result = append(result, key, value)
	}

	return result
}

func (s *simpleHash) GetKeys() []string {
	if len(s.contents) == 0 {
		return []string{}
	}

	result := make([]string, 0, len(s.contents))

	for key := range s.contents {
		result = append(result, key)
	}

	return result
}

func (s *simpleHash) GetValues() []string {
	if len(s.contents) == 0 {
		return []string{}
	}
	
	result := make([]string, 0, len(s.contents))
	
	for _, value := range s.contents {
		result = append(result, value)
	}
	
	return result
}

func (s *simpleHash) IncBy(key string, increment int64) (int64, error) {
	value, exists := s.contents[key]

	if !exists {
		s.contents[key] = strconv.FormatInt(increment, 10)
		return increment, nil
	}

	valueInt, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.New("hash value is not an integer")
	}

	// Check for overflow
	if (increment > 0 && valueInt > math.MaxInt64-increment) ||
		(increment < 0 && valueInt < math.MinInt64-increment) {
		return 0, errors.New("value is not an integer or out of range")
	}

	valueInt += increment
	s.contents[key] = strconv.FormatInt(valueInt, 10)
	return valueInt, nil
}

func (s *simpleHash) MGet(keys ...string) []*string {
	result := make([]*string, len(keys))

	for i, key := range keys {
		if value, exists := s.contents[key]; exists {
			v := value
			result[i] = &v
		}
		// result[i] remains nil for non-existent keys
	}

	return result
}

func (s *simpleHash) Set(fieldValue map[string]string) int64 {
	if len(fieldValue) == 0 {
		return 0
	}
	
	added := int64(0)
	for key, value := range fieldValue {
		if _, exists := s.contents[key]; !exists {
			added++
		}
		s.contents[key] = value
	}

	return added
}

func (s *simpleHash) SetNX(key, value string) bool {
	if _, exists := s.contents[key]; exists {
		return false
	}
	
	s.contents[key] = value
	return true
}

func (s *simpleHash) Delete(keys ...string) int64 {
	if len(keys) == 0 {
		return 0
	}
	
	deleted := int64(0)
	for _, key := range keys {
		if _, exists := s.contents[key]; exists {
			delete(s.contents, key)
			deleted++
		}
	}
	
	return deleted
}

func (s *simpleHash) Size() uint32 {
	return uint32(len(s.contents))
}

func (s *simpleHash) Exists(key string) bool {
	_, exists := s.contents[key]
	return exists
}

func (s *simpleHash) MemoryUsage() int64 {
	return int64(size.Of(s))
}