package storage

import (
	"time"

	"github.com/manhhung2111/go-redis/internal/protocol"
)

type ExpireOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
}

func (s *store) TTL(key string) int64 {
	result := s.access(key, ObjAny, false)
	if result.expired || !result.exists {
		return protocol.KeyNotExists
	}

	if expireAt, ok := s.expires.Get(key); ok {
		now := uint64(time.Now().UnixMilli())
		return int64((expireAt - now) / 1000)
	}

	return protocol.NoExpire
}

func (s *store) Expire(key string, ttlSeconds int64, opt ExpireOptions) bool {
	result := s.access(key, ObjAny, true)
	if result.expired || !result.exists {
		return false
	}

	now := time.Now().UnixMilli()
	newExpireAt := uint64(now + ttlSeconds*1000)

	oldExpireAt, hasExpire := s.expires.Get(key)

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
		oldTTL := int64((oldExpireAt - uint64(now)) / 1000)

		if opt.GT && oldTTL >= ttlSeconds {
			return false
		}
		if opt.LT && oldTTL <= ttlSeconds {
			return false
		}
	}

	s.expires.Set(key, newExpireAt)
	return true
}
