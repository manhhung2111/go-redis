package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) GeoAdd(key string, items []data_structure.GeoPoint, options data_structure.ZAddOptions) *uint32 {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		zset := data_structure.NewZSet()
		result := zset.GeoAdd(items, options)
		s.data[key] = &RObj{
			Type:     ObjZSet,
			Encoding: EncSortedSet,
			Value:    zset,
		}
		return result
	}

	zset, ok := rObj.Value.(data_structure.ZSet)
	if !ok {
		panic("GeoAdd operation called on object not type ZSet")
	}

	return zset.GeoAdd(items, options)
}

func (s *store) GeoDist(key, member1, member2, unit string) *float64 {
	zset, exists := s.getZSet(key)
	if !exists {
		return nil
	}

	return zset.GeoDist(member1, member2, unit)
}

func (s *store) GeoHash(key string, members []string) []*string {
	zset, exists := s.getZSet(key)
	if !exists {
		return make([]*string, len(members))
	}

	return zset.GeoHash(members)
}

func (s *store) GeoPos(key string, members []string) []*data_structure.GeoPoint {
	zset, exists := s.getZSet(key)
	if !exists {
		return make([]*data_structure.GeoPoint, len(members))
	}

	return zset.GeoPos(members)
}

func (s *store) GeoSearch(key string, options data_structure.GeoSearchOptions) []data_structure.GeoResult {
	zset, exists := s.getZSet(key)
	if !exists {
		return []data_structure.GeoResult{}
	}

	return zset.GeoSearch(options)
}
