package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) GeoAdd(key string, items []data_structure.GeoPoint, options data_structure.ZAddOptions) (*uint32, error) {
	result := s.access(key, ObjZSet)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if !result.exists {
		zset := data_structure.NewZSet()
		added := zset.GeoAdd(items, options)
		s.data.Set(key, &RObj{
			Type:     ObjZSet,
			Encoding: EncSortedSet,
			Value:    zset,
		})
		return added, nil
	}

	zset := result.object.Value.(data_structure.ZSet)
	return zset.GeoAdd(items, options), nil
}

func (s *store) GeoDist(key, member1, member2, unit string) (*float64, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return nil, nil
	}

	return zset.GeoDist(member1, member2, unit), nil
}

func (s *store) GeoHash(key string, members []string) ([]*string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return make([]*string, len(members)), nil
	}

	return zset.GeoHash(members), nil
}

func (s *store) GeoPos(key string, members []string) ([]*data_structure.GeoPoint, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return make([]*data_structure.GeoPoint, len(members)), nil
	}

	return zset.GeoPos(members), nil
}

func (s *store) GeoSearch(key string, options data_structure.GeoSearchOptions) ([]data_structure.GeoResult, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []data_structure.GeoResult{}, nil
	}

	return zset.GeoSearch(options), nil
}
