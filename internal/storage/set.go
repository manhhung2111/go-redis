package storage

import (
	"math/rand"
	"strconv"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/manhhung2111/go-redis/internal/util"
)

func (s *store) SAdd(key string, members ...string) (int64, error) {
	result := s.access(key, ObjSet)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.exists {
		rObj := result.object
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

				return added, nil
			} else {
				return added, nil
			}
		}

		simpleSet := rObj.Value.(data_structure.Set)
		added, _ := simpleSet.Add(members...)
		return added, nil
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
			return added, nil
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

	return added, nil
}

func (s *store) SCard(key string) (int64, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return 0, nil
	}

	if result.typeErr != nil {
		return 0, result.typeErr
	}

	set := result.object.Value.(data_structure.Set)
	return set.Size(), nil
}

func (s *store) SIsMember(key string, member string) (bool, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return false, nil
	}

	if result.typeErr != nil {
		return false, result.typeErr
	}

	set := result.object.Value.(data_structure.Set)
	return set.IsMember(member), nil
}

func (s *store) SMembers(key string) ([]string, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return []string{}, nil
	}

	if result.typeErr != nil {
		return []string{}, result.typeErr
	}

	set := result.object.Value.(data_structure.Set)
	return set.Members(), nil
}

func (s *store) SMIsMember(key string, members ...string) ([]bool, error) {
	defaultResult := make([]bool, len(members))

	accessResult := s.access(key, ObjSet)
	if accessResult.expired || !accessResult.exists {
		return defaultResult, nil
	}

	if accessResult.typeErr != nil {
		return nil, accessResult.typeErr
	}

	set := accessResult.object.Value.(data_structure.Set)
	return set.MIsMember(members...), nil
}

func (s *store) SRem(key string, members ...string) (int64, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return 0, nil
	}

	if result.typeErr != nil {
		return 0, result.typeErr
	}

	set := result.object.Value.(data_structure.Set)
	return set.Delete(members...), nil
}

func (s *store) SPop(key string, count int) ([]string, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return []string{}, nil
	}

	if result.typeErr != nil {
		return []string{}, result.typeErr
	}

	set := result.object.Value.(data_structure.Set)
	setLen := int(set.Size())

	if setLen == 0 {
		return []string{}, nil
	}

	// Pop everything
	if count >= setLen {
		members := set.Members()
		s.delete(key)
		return members, nil
	}

	indices := util.FloydSamplingIndices(setLen, count)
	members := set.Members()
	popped := make([]string, 0, count)

	for idx, m := range members {
		if _, ok := indices[idx]; ok {
			popped = append(popped, m)
		}
	}

	set.Delete(popped...)

	return popped, nil
}

func (s *store) SRandMember(key string, count int) ([]string, error) {
	result := s.access(key, ObjSet)
	if result.expired || !result.exists {
		return []string{}, nil
	}

	if result.typeErr != nil {
		return []string{}, result.typeErr
	}

	if count == 0 {
		return []string{}, nil
	}

	set := result.object.Value.(data_structure.Set)
	setLen := int(set.Size())
	if setLen == 0 {
		return []string{}, nil
	}

	if count > 0 {
		// If count >= size → return all members
		if count >= setLen {
			return set.Members(), nil
		}

		indices := util.FloydSamplingIndices(setLen, count)
		members := set.Members()
		selected := make([]string, 0, count)

		for idx, m := range members {
			if _, ok := indices[idx]; ok {
				selected = append(selected, m)
			}
		}

		return selected, nil
	}

	k := -count
	selected := make([]string, 0, k)
	members := set.Members()

	for range k {
		selected = append(selected, members[rand.Intn(setLen)])
	}

	return selected, nil
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
