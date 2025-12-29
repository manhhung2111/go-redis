package storage

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/manhhung2111/go-redis/internal/util"
)

func (s *store) SAdd(key string, members ...string) int64 {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if exists {
		if rObj.Type != ObjSet {
			panic("SAdd called on non-set object")
		}

		if rObj.Encoding == EncIntSet {
			// Upgrade to SimpleSet if one of the following condition holds:
			// members contain an element can not be converted to int64
			// adding members resulting in exceeding the config.SET_MAX_INTSET_ENTRIES
			intset := rObj.Value.(data_structure.Set)
			added, succeeded := intset.Add(members...)
			if !succeeded {
				// Upgrade to SimpleSet
				simpleSet := data_structure.NewSimpleSet()
				simpleSet.Add(intset.Members()...)
				added, _ := simpleSet.Add(members...)

				// Update existing RObj
				rObj.Encoding = EncHashTable
				rObj.Value = simpleSet

				return added
			} else {
				return added
			}
		}

		simpleSet := rObj.Value.(data_structure.Set)
		added, _ := simpleSet.Add(members...)
		return added
	}

	// Key doesn't exist - create new set
	if canBeConvertedToInt64(members...) {
		intset := data_structure.NewIntSet()
		added, succeeded := intset.Add(members...)
		if succeeded {
			s.data[key] = &RObj{
				Type:     ObjSet,
				Encoding: EncIntSet,
				Value:    intset,
			}
			return added
		}
		// If IntSet failed (capacity), fall through to SimpleSet
	}

	simpleSet := data_structure.NewSimpleSet()
	added, _ := simpleSet.Add(members...)

	s.data[key] = &RObj{
		Type:     ObjSet,
		Encoding: EncHashTable,
		Value:    simpleSet,
	}

	return added
}

func (s *store) SCard(key string) int64 {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return 0
	}

	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	if rObj.Type != ObjSet {
		panic("SCard called on non-set object")
	}

	set := rObj.Value.(data_structure.Set)
	return set.Size()
}

func (s *store) SIsMember(key string, member string) bool {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return false
	}

	rObj, exists := s.data[key]
	if !exists {
		return false
	}

	if rObj.Type != ObjSet {
		panic("SIsMember called on non-set object")
	}

	set := rObj.Value.(data_structure.Set)
	return set.IsMember(member)
}

func (s *store) SMembers(key string) []string {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return []string{}
	}

	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	if rObj.Type != ObjSet {
		panic("SMembers called on non-set object")
	}

	set := rObj.Value.(data_structure.Set)
	return set.Members()
}

func (s *store) SMIsMember(key string, members ...string) []bool {
	result := make([]bool, len(members))
	for i := range members {
		result[i] = false
	}

	if isExpired := s.expireIfNeeded(key); isExpired {
		return result
	}

	rObj, exists := s.data[key]
	if !exists {
		return result
	}

	if rObj.Type != ObjSet {
		panic("SMIsMember called on non-set object")
	}

	set := rObj.Value.(data_structure.Set)
	return set.MIsMember(members...)
}

func (s *store) SRem(key string, members ...string) int64 {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return 0
	}

	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	if rObj.Type != ObjSet {
		panic("SRem called on non-set object")
	}

	set := rObj.Value.(data_structure.Set)
	return set.Delete(members...)
}

func (s *store) SPop(key string, count int) []string {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return []string{}
	}

	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	set := rObj.Value.(data_structure.Set)
	setLen := int(set.Size())
	if setLen == 0 {
		return []string{}
	}

	// Pop everything
	if count >= setLen {
		result := set.Members()
		s.Del(key)
		return result
	}

	indices := util.FloydSamplingIndices(setLen, count)
	members := set.Members()
	result := make([]string, 0, count)

	for idx, m := range members {
		if _, ok := indices[idx]; ok {
			result = append(result, m)
		}
	}

	set.Delete(result...)

	return result
}

func (s *store) SRandMember(key string, count int) []string {
	if count == 0 {
		return []string{}
	}

	if isExpired := s.expireIfNeeded(key); isExpired {
		return []string{}
	}

	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	set := rObj.Value.(data_structure.Set)
	setLen := int(set.Size())
	if setLen == 0 {
		return []string{}
	}

	// Pop everything
	if count > 0 {
		// If count >= size → return all members
		if count >= setLen {
			return set.Members()
		}

		indices := util.FloydSamplingIndices(setLen, count)
		members := set.Members()
		result := make([]string, 0, count)

		for idx, m := range members {
			if _, ok := indices[idx]; ok {
				result = append(result, m)
			}
		}

		return result
	}

	k := -count
	result := make([]string, 0, k)
	members := set.Members()

	for range k {
		result = append(result, members[rand.Intn(setLen)])
	}

	return result
}

func (s *store) expireIfNeeded(key string) bool {
	if exp, ok := s.expires[key]; ok {
		if exp <= uint64(time.Now().UnixMilli()) {
			s.Del(key)
			return true
		}
	}
	return false
}

func canBeConvertedToInt64(members ...string) bool {
	if len(members) == 0 || len(members) > config.SET_MAX_INTSET_ENTRIES {
		return false
	}

	for i := range members {
		if _, err := strconv.ParseInt(members[i], 10, 64); err != nil {
			return false
		}
	}

	return true
}
