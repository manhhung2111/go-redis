package storage

import (
	"math"
	"strconv"
	"time"
)

func newStringObject(s string) *RObj {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &RObj{
			Type:     ObjString,
			Encoding: EncInt,
			Value:    v,
		}
	}

	return &RObj{
		Type:     ObjString,
		Encoding: EncRaw,
		Value:    s,
	}
}

func (s *store) Set(key string, value string) {
	s.delete(key)
	s.data[key] = newStringObject(value)
}

func (s *store) SetEx(key string, value string, ttlSeconds uint64) {
	expireAt := uint64(time.Now().UnixMilli()) + ttlSeconds*1000
	s.data[key] = newStringObject(value)
	s.expires[key] = expireAt
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
	if rObj.Encoding == EncInt {
		val := strconv.FormatInt(rObj.Value.(int64), 10)
		return &val, nil
	}

	val := rObj.Value.(string)
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
	if rObj.Encoding != EncInt {
		return nil, ErrValueIsNotIntegerOrOutOfRangeError
	}

	val := rObj.Value.(int64)
	if (increment > 0 && val > math.MaxInt64-increment) || (increment < 0 && val < math.MinInt64-increment) {
		return nil, ErrValueIsNotIntegerOrOutOfRangeError
	}

	val += increment
	rObj.Value = val
	return &val, nil
}
