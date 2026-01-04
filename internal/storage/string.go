package storage

import (
	"math"
	"strconv"
	"time"
)

func newStringObject(s string) *RObj {
	// Try int encoding
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

func (o *RObj) StringValue() (string, bool) {
	if o == nil || o.Type != ObjString {
		return "", false
	}

	switch o.Encoding {
	case EncRaw:
		return o.Value.(string), true
	case EncInt:
		return strconv.FormatInt(o.Value.(int64), 10), true
	default:
		return "", false
	}
}

func (s *store) Set(key string, value string) {
	s.data[key] = newStringObject(value)
	delete(s.expires, key)
}

func (s *store) SetEx(key string, value string, ttlSeconds uint64) {
	expireAt := uint64(time.Now().UnixMilli()) + ttlSeconds*1000
	s.data[key] = newStringObject(value)
	s.expires[key] = expireAt
}

func (s *store) Get(key string) (*RObj, bool) {
	obj, ok := s.data[key]
	if !ok {
		return nil, false
	}

	if ttl, ok := s.expires[key]; ok {
		now := time.Now().UnixMilli()
		if ttl <= uint64(now) {
			s.Del(key)
			return nil, false
		}
	}

	return obj, true
}

func (s *store) Del(key string) bool {
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
		delete(s.expires, key)
		return true
	}
	return false
}

func (o *RObj) IncrBy(increment int64) (int64, bool) {
	if o.Type != ObjString {
		return 0, false
	}

	var v int64
	switch o.Encoding {
	case EncInt:
		v = o.Value.(int64)
	case EncRaw:
		var err error
		v, err = strconv.ParseInt(o.Value.(string), 10, 64)
		if err != nil {
			return 0, false
		}
	default:
		return 0, false
	}

	if (increment > 0 && v > math.MaxInt64-increment) || (increment < 0 && v < math.MinInt64-increment) {
		return 0, false
	}

	v += increment
	o.Encoding = EncInt
	o.Value = v
	return v, true
}