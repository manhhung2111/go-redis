package storage

import (
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
)

type storageAccessResult struct {
	object  *RObj
	exists  bool
	expired bool
	err     error // Non-nil if type mismatch
}

// Centralized function to access key.
// isWrite indicates if this is a write operation - only writes trigger memory checks.
func (s *store) access(key string, expectedType ObjectType, isWrite bool) storageAccessResult {
	result := storageAccessResult{}

	// Check memory limit for write operations before proceeding
	if isWrite && s.usedMemory > config.MAXMEMORY_LIMIT {
		if config.EVICTION_POLICY == config.NoEviction {
			result.err = ErrOutOfMemoryError
			return result
		}
		s.performEvictions()
		// Check again after eviction
		if s.usedMemory > config.MAXMEMORY_LIMIT {
			result.err = ErrOutOfMemoryError
			return result
		}
	}

	if exp, hasExpire := s.expires.Get(key); hasExpire {
		if exp <= uint64(time.Now().UnixMilli()) {
			s.delete(key)
			result.expired = true
			return result
		}
	}

	obj, exists := s.data.Get(key)
	if exists {
		if config.EVICTION_POLICY == config.AllKeysLRU || config.EVICTION_POLICY == config.VolatileLRU {
			obj.lru = uint32(time.Now().Unix())
		}
	}

	result.object = obj
	result.exists = exists

	// Skip type check if expectedType is ObjAny
	if exists && expectedType != ObjAny && obj.objType != expectedType {
		result.err = ErrWrongTypeError
	}

	return result
}

// Centralized function to delete key
func (s *store) delete(key string) bool {
	ok, delta1 := s.data.Delete(key)
	if !ok {
		return false
	}

	_, delta2 := s.expires.Delete(key)
	s.usedMemory -= delta1 + delta2
	return true
}
