package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) HGet(key string, field string) (*string, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	val, ok := hash.Get(field)
	if !ok {
		return nil, nil
	}

	return &val, nil
}

func (s *store) HGetAll(key string) ([]string, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return []string{}, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.GetAll(), nil
}

func (s *store) HMGet(key string, fields []string) ([]*string, error) {
	nilResult := make([]*string, len(fields))

	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nilResult, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.MGet(fields...), nil
}

func (s *store) HIncrBy(key string, field string, increment int64) (int64, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if !result.exists {
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

	hash := result.object.Value.(data_structure.Hash)
	return hash.IncBy(field, increment)
}

func (s *store) HKeys(key string) ([]string, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return []string{}, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.GetKeys(), nil
}

func (s *store) HVals(key string) ([]string, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return []string{}, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.GetValues(), nil
}

func (s *store) HLen(key string) (uint32, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.expired || !result.exists {
		return 0, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.Size(), nil
}

func (s *store) HSet(key string, fieldValue map[string]string) (int64, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if !result.exists {
		hash := data_structure.NewHash()
		added := hash.Set(fieldValue)
		s.data[key] = &RObj{
			Type:     ObjHash,
			Encoding: EncHashTable,
			Value:    hash,
		}
		return added, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	return hash.Set(fieldValue), nil
}

func (s *store) HSetNx(key string, field string, value string) (int64, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if !result.exists {
		hash := data_structure.NewHash()
		hash.SetNX(field, value)
		s.data[key] = &RObj{
			Type:     ObjHash,
			Encoding: EncHashTable,
			Value:    hash,
		}
		return 1, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	canSet := hash.SetNX(field, value)
	if canSet {
		return 1, nil
	}
	return 0, nil
}

func (s *store) HDel(key string, fields []string) (int64, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.expired || !result.exists {
		return 0, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	deleted := hash.Delete(fields...)
	if hash.Size() == 0 {
		s.delete(key)
	}
	return deleted, nil
}

func (s *store) HExists(key, field string) (int64, error) {
	result := s.access(key, ObjHash)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.expired || !result.exists {
		return 0, nil
	}

	hash := result.object.Value.(data_structure.Hash)
	if hash.Exists(field) {
		return 1, nil
	}
	return 0, nil
}
