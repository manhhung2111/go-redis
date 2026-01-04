package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

func (s *store) ZAdd(key string, scoreMember map[string]float64, options data_structure.ZAddOptions) *uint32 {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		zset := data_structure.NewZSet()
		result := zset.ZAdd(scoreMember, options)
		s.data[key] = &RObj{
			Type:     ObjZSet,
			Encoding: EncSortedSet,
			Value:    zset,
		}

		return result
	}

	zset, ok := rObj.Value.(data_structure.ZSet)
	if !ok {
		panic("ZAdd operation called on object not type ZSet")
	}

	return zset.ZAdd(scoreMember, options)
}

func (s *store) ZCard(key string) uint32 {
	zset, exists := s.getZSet(key)
	if !exists {
		return 0
	}
	return zset.ZCard()
}

func (s *store) ZCount(key string, minScore float64, maxScore float64) uint32 {
	zset, exists := s.getZSet(key)
	if !exists {
		return 0
	}
	return zset.ZCount(minScore, maxScore)
}

func (s *store) ZIncrBy(key string, member string, increment float64) (float64, bool) {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if !exists {
		zset := data_structure.NewZSet()
		result, succeeded := zset.ZIncrBy(member, increment)

		if succeeded {
			s.data[key] = &RObj{
				Type:     ObjZSet,
				Encoding: EncSortedSet,
				Value:    zset,
			}
		}

		return result, succeeded
	}

	zset, ok := rObj.Value.(data_structure.ZSet)
	if !ok {
		panic("ZIncrBy operation called on object not type ZSet")
	}

	return zset.ZIncrBy(member, increment)
}

func (s *store) ZLexCount(key string, minValue string, maxValue string) uint32 {
	zset, exists := s.getZSet(key)
	if !exists {
		return 0
	}

	return zset.ZLexCount(minValue, maxValue)
}

func (s *store) ZMScore(key string, members []string) []*float64 {
	zset, exists := s.getZSet(key)
	if !exists {
		return make([]*float64, len(members))
	}
	return zset.ZMScore(members)
}

func (s *store) ZPopMax(key string, count int) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZPopMax(count)
}

func (s *store) ZPopMin(key string, count int) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZPopMin(count)
}

func (s *store) ZRandMember(key string, count int, withScores bool) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRandMember(count, withScores)
}

func (s *store) ZRangeByLex(key string, start string, stop string) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRangeByLex(start, stop, false)
}

func (s *store) ZRangeByRank(key string, start int, stop int, withScores bool) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRangeByRank(start, stop, withScores)
}

func (s *store) ZRangeByScore(key string, start float64, stop float64, withScores bool) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRangeByScore(start, stop, withScores)
}

func (s *store) ZRank(key string, member string, withScore bool) []any {
	zset, exists := s.getZSet(key)
	if !exists {
		return nil
	}

	return zset.ZRank(member, withScore)
}

func (s *store) ZRem(key string, members []string) uint32 {
	zset, exists := s.getZSet(key)
	if !exists {
		return 0
	}

	return uint32(zset.ZRem(members))
}

func (s *store) ZRevRangeByLex(key string, start string, stop string) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRevRangeByLex(start, stop, false)
}

func (s *store) ZRevRangeByRank(key string, start int, stop int, withScores bool) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRevRangeByRank(start, stop, withScores)
}

func (s *store) ZRevRangeByScore(key string, start float64, stop float64, withScores bool) []string {
	zset, exists := s.getZSet(key)
	if !exists {
		return []string{}
	}

	return zset.ZRevRangeByScore(start, stop, withScores)
}

func (s *store) ZRevRank(key string, member string, withScore bool) []any {
	zset, exists := s.getZSet(key)
	if !exists {
		return nil
	}

	return zset.ZRevRank(member, withScore)
}

func (s *store) ZScore(key string, member string) *float64 {
	zset, exists := s.getZSet(key)
	if !exists {
		return nil
	}

	return zset.ZScore(member)
}

func (s *store) getZSet(key string) (data_structure.ZSet, bool) {
	if expired := s.expireIfNeeded(key); expired {
		return nil, false
	}

	rObj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	zset, ok := rObj.Value.(data_structure.ZSet)
	if !ok {
		panic("Operation called on object not type ZSet")
	}

	return zset, true
}
