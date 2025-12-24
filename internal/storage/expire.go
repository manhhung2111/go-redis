package storage

import "time"

type ExpireOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
}


func (s *store) TTL(key string) int64 {
	obj, ok := s.data[key]
	if !ok || obj == nil {
		return -2
	}

	if expireAt, ok := s.expires[key]; ok {
		now := uint64(time.Now().UnixMilli())
		if expireAt <= now {
			s.Del(key)
			return -2
		}
		return int64((expireAt - now) / 1000)
	}

	return -1
}

func (s *store) Expire(key string, ttlSeconds int64, opt ExpireOptions) bool {
	if _, ok := s.data[key]; !ok {
		return false
	}

	now := time.Now().UnixMilli()
	newExpireAt := uint64(now + ttlSeconds*1000)

	oldExpireAt, hasExpire := s.expires[key]

	// NX: set only if key has no expire
	if opt.NX && hasExpire {
		return false
	}

	// XX: set only if key has expire
	if opt.XX && !hasExpire {
		return false
	}

	// GT / LT only apply if key already has expire
	if hasExpire {
		if oldExpireAt <= uint64(now) {
			s.Del(key)
			return false
		}

		oldTTL := int64((oldExpireAt - uint64(now)) / 1000)

		if opt.GT && oldTTL >= ttlSeconds {
			return false
		}
		if opt.LT && oldTTL <= ttlSeconds {
			return false
		}
	}

	s.expires[key] = newExpireAt
	return true
}
