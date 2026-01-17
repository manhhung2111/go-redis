package storage

import (
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) ZAdd(key string, scoreMember map[string]float64, options data_structure.ZAddOptions) (*uint32, error) {
	result := s.access(key, ObjZSet)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if !result.exists {
		zset := data_structure.NewZSet()
		added, _ := zset.ZAdd(scoreMember, options)
		delta := s.data.Set(key, &RObj{
			objType:  ObjZSet,
			encoding: EncSortedSet,
			value:    zset,
		})
		s.usedMemory += delta
		return added, nil
	}

	zset := result.object.value.(data_structure.ZSet)
	added, delta := zset.ZAdd(scoreMember, options)
	s.usedMemory += delta
	return added, nil
}

func (s *store) ZCard(key string) (uint32, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return 0, err
	}

	if zset == nil {
		return 0, nil
	}

	return zset.ZCard(), nil
}

func (s *store) ZCount(key string, minScore float64, maxScore float64) (uint32, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return 0, err
	}

	if zset == nil {
		return 0, nil
	}

	return zset.ZCount(minScore, maxScore), nil
}

func (s *store) ZIncrBy(key string, member string, increment float64) (float64, error) {
	result := s.access(key, ObjZSet)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if !result.exists {
		zset := data_structure.NewZSet()
		res, succeeded, _ := zset.ZIncrBy(member, increment)

		if !succeeded {
			return 0, ErrValueIsNotValidFloatError
		}

		delta := s.data.Set(key, &RObj{
			objType:  ObjZSet,
			encoding: EncSortedSet,
			value:    zset,
		})
		s.usedMemory += delta

		return res, nil
	}

	zset := result.object.value.(data_structure.ZSet)
	res, succeeded, delta := zset.ZIncrBy(member, increment)

	if !succeeded {
		return 0, ErrValueIsNotValidFloatError
	}
	s.usedMemory += delta

	return res, nil
}

func (s *store) ZLexCount(key string, minValue string, maxValue string) (uint32, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return 0, err
	}

	if zset == nil {
		return 0, nil
	}

	return zset.ZLexCount(minValue, maxValue), nil
}

func (s *store) ZMScore(key string, members []string) ([]*float64, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return make([]*float64, len(members)), nil
	}

	return zset.ZMScore(members), nil
}

func (s *store) ZPopMax(key string, count int) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	res, delta := zset.ZPopMax(count)
	s.usedMemory += delta
	return res, nil
}

func (s *store) ZPopMin(key string, count int) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	res, delta := zset.ZPopMin(count)
	s.usedMemory += delta
	return res, nil
}

func (s *store) ZRandMember(key string, count int, withScores bool) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRandMember(count, withScores), nil
}

func (s *store) ZRangeByLex(key string, start string, stop string) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRangeByLex(start, stop, false), nil
}

func (s *store) ZRangeByRank(key string, start int, stop int, withScores bool) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRangeByRank(start, stop, withScores), nil
}

func (s *store) ZRangeByScore(key string, start float64, stop float64, withScores bool) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRangeByScore(start, stop, withScores), nil
}

func (s *store) ZRank(key string, member string, withScore bool) ([]any, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return nil, nil
	}

	return zset.ZRank(member, withScore), nil
}

func (s *store) ZRem(key string, members []string) (uint32, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return 0, err
	}

	if zset == nil {
		return 0, nil
	}

	res, delta := zset.ZRem(members)
	s.usedMemory += delta
	return uint32(res), nil
}

func (s *store) ZRevRangeByLex(key string, start string, stop string) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRevRangeByLex(start, stop, false), nil
}

func (s *store) ZRevRangeByRank(key string, start int, stop int, withScores bool) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRevRangeByRank(start, stop, withScores), nil
}

func (s *store) ZRevRangeByScore(key string, start float64, stop float64, withScores bool) ([]string, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return []string{}, nil
	}

	return zset.ZRevRangeByScore(start, stop, withScores), nil
}

func (s *store) ZRevRank(key string, member string, withScore bool) ([]any, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return nil, nil
	}

	return zset.ZRevRank(member, withScore), nil
}

func (s *store) ZScore(key string, member string) (*float64, error) {
	zset, err := s.getZSet(key)
	if err != nil {
		return nil, err
	}

	if zset == nil {
		return nil, nil
	}

	return zset.ZScore(member), nil
}

// getZSet is a helper that uses centralized access for expiration and type checking
func (s *store) getZSet(key string) (data_structure.ZSet, error) {
	result := s.access(key, ObjZSet)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	zset := result.object.value.(data_structure.ZSet)
	return zset, nil
}
