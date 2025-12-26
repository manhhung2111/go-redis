package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) HGet(key string, field string) (string, bool) {
	rObj, exists := s.data[key]
	if !exists {
		return "", false
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return "", false
	}

	return hash.Get(field)
}

func (s *store) HGetAll(key string) []string {
	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return []string{}
	}

	return hash.GetAll()
}

func (s *store) HMGet(key string, fields []string) []*string {
	rObj, exists := s.data[key]
	if !exists {
		// Return slice with nil pointers for all fields when key doesn't exist
		result := make([]*string, len(fields))
		return result
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		// Return slice with nil pointers for all fields when type is wrong
		result := make([]*string, len(fields))
		return result
	}

	return hash.MGet(fields...)
}

func (s *store) HIncrBy(key string, field string, increment int64) (int64, error) {
	rObj, exists := s.data[key]
	if !exists {
		hash := data_structure.NewHash()
		res, err := hash.IncBy(field, increment)
		if err != nil {
			return 0, err
		}
		s.data[key] = &RObj{
			Type:     ObjHash,
			Encoding: EncHashTable,
			Value:    hash,
		}

		return res, nil
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		panic("HIncrBy called on RObj not type Hash")
	}

	return hash.IncBy(field, increment)
}

func (s *store) HKeys(key string) []string {
	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return []string{}
	}

	return hash.GetKeys()
}

func (s *store) HVals(key string) []string {
	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return []string{}
	}

	return hash.GetValues()
}

func (s *store) HLen(key string) uint32 {
	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return 0
	}

	return hash.Size()
}

func (s *store) HSet(key string, fieldValue map[string]string) int64 {
	rObj, exists := s.data[key]
	if !exists {
		hash := data_structure.NewHash()
		added := hash.Set(fieldValue)
		s.data[key] = &RObj{
			Type:     ObjHash,
			Encoding: EncHashTable,
			Value:    hash,
		}

		return added
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		panic("HSet called on RObj not type Hash")
	}

	return hash.Set(fieldValue)
}

func (s *store) HSetNx(key string, field string, value string) int64 {
	rObj, exists := s.data[key]
	if !exists {
		// Create new hash if key doesn't exist
		hash := data_structure.NewHash()
		hash.SetNX(field, value)
		s.data[key] = &RObj{
			Type:     ObjHash,
			Encoding: EncHashTable,
			Value:    hash,
		}
		return 1
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return 0
	}

	canSet := hash.SetNX(field, value)
	if canSet {
		return 1
	}

	return 0
}

func (s *store) HDel(key string, fields []string) int64 {
	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return 0
	}

	deleted := hash.Delete(fields...)
	if (hash.Size() == 0) {
		s.Del(key)
	}

	return deleted
}

func (s *store) HExists(key, field string) int64 {
	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	hash, ok := rObj.Value.(data_structure.Hash)
	if !ok {
		return 0
	}

	fieldExists := hash.Exists(field)
	if fieldExists {
		return 1
	}

	return 0
}
