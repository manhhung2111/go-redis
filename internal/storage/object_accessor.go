package storage

import "time"

type storageAccessResult struct {
	object  *RObj
	exists  bool
	expired bool
	typeErr error // Non-nil if type mismatch
}

// Centralized function to access key
func (s *store) access(key string, expectedType ObjectType) storageAccessResult {
	result := storageAccessResult{}

	if exp, hasExpire := s.expires.Get(key); hasExpire {
		if exp <= uint64(time.Now().UnixMilli()) {
			s.delete(key)
			result.expired = true
			return result
		}
	}

	obj, exists := s.data.Get(key)
	result.object = obj
	result.exists = exists

	// Skip type check if expectedType is ObjAny
	if exists && expectedType != ObjAny && obj.Type != expectedType {
		result.typeErr = ErrWrongTypeError
	}

	return result
}

// Centralized function to delete key
func (s *store) delete(key string) bool {
	_, ok := s.data.Get(key)
	if ok {
		s.data.Delete(key)
		s.expires.Delete(key)
		return true
	}
	return false
}
