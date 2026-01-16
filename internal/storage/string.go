package storage

import (
	"math"
	"strconv"
	"time"
)

func newStringObject(s string) *RObj {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &RObj{
			objType:     ObjString,
			encoding: EncInt,
			value:    v,
		}
	}

	return &RObj{
		objType:     ObjString,
		encoding: EncRaw,
		value:    s,
	}
}

func (s *store) Set(key string, value string) {
	s.delete(key)
	s.data.Set(key, newStringObject(value))
}

func (s *store) SetEx(key string, value string, ttlSeconds uint64) {
	s.delete(key) // Clean up existing key if any

	expireAt := uint64(time.Now().UnixMilli()) + ttlSeconds*1000
	s.data.Set(key, newStringObject(value))
	s.expires.Set(key, expireAt)
}

func (s *store) Get(key string) (*string, error) {
	result := s.access(key, ObjString)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	// Encoding must be raw or int
	rObj := result.object
	if rObj.encoding == EncInt {
		val := strconv.FormatInt(rObj.value.(int64), 10)
		return &val, nil
	}

	val := rObj.value.(string)
	return &val, nil
}

func (s *store) Del(key string) bool {
	return s.delete(key)
}

func (s *store) IncrBy(key string, increment int64) (*int64, error) {
	result := s.access(key, ObjString)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		s.Set(key, strconv.FormatInt(increment, 10))
		return &increment, nil
	}

	// Encoding must be raw or int
	rObj := result.object
	if rObj.encoding != EncInt {
		return nil, ErrValueIsNotIntegerOrOutOfRangeError
	}

	val := rObj.value.(int64)
	if (increment > 0 && val > math.MaxInt64-increment) || (increment < 0 && val < math.MinInt64-increment) {
		return nil, ErrValueIsNotIntegerOrOutOfRangeError
	}

	val += increment
	rObj.value = val
	return &val, nil
}
