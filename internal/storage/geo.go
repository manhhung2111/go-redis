package storage

import "github.com/manhhung2111/go-redis/internal/storage/types"

func (s *store) GeoAdd(key string, items []types.GeoPoint, options types.ZAddOptions) (*uint32, error) {
	result := s.access(key, ObjZSet, true)
	if result.err != nil {
		return nil, result.err
	}

	if !result.exists {
		zset := types.NewZSet()
		added, _ := zset.GeoAdd(items, options)
		delta := s.data.Set(key, &RObj{
			objType:  ObjZSet,
			encoding: EncSortedSet,
			value:    zset,
		})

		s.usedMemory += delta
		return added, nil
	}

	zset := result.object.value.(types.ZSet)
	added, delta := zset.GeoAdd(items, options)
	s.usedMemory += delta
	return added, nil
}

func (s *store) GeoDist(key, member1, member2, unit string) (*float64, error) {
	zset, err := s.getZSet(key, false)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return nil, nil
	}

	return zset.GeoDist(member1, member2, unit), nil
}

func (s *store) GeoHash(key string, members []string) ([]*string, error) {
	zset, err := s.getZSet(key, false)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return make([]*string, len(members)), nil
	}

	return zset.GeoHash(members), nil
}

func (s *store) GeoPos(key string, members []string) ([]*types.GeoPoint, error) {
	zset, err := s.getZSet(key, false)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return make([]*types.GeoPoint, len(members)), nil
	}

	return zset.GeoPos(members), nil
}

func (s *store) GeoSearch(key string, options types.GeoSearchOptions) ([]types.GeoResult, error) {
	zset, err := s.getZSet(key, false)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []types.GeoResult{}, nil
	}

	return zset.GeoSearch(options), nil
}
