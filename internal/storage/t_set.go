package storage

import (
	"math/rand"
	"time"
)

type simpleSet map[string]struct{}

func (s *store) SAdd(key string, members ...string) int64 {
	s.expireIfNeeded(key)

	obj, exists := s.data[key]
	if exists {
		set := obj.Value.(simpleSet)

		var added int64 = 0
		for _, m := range members {
			if _, ok := set[m]; !ok {
				set[m] = struct{}{}
				added++
			}
		}
		return added
	}

	set := make(simpleSet, len(members))
	for _, m := range members {
		set[m] = struct{}{}
	}

	s.data[key] = &RObj{
		Type:     ObjSet,
		Encoding: EncHashTable,
		Value:    set,
	}

	return int64(len(set))
}

func (s *store) SCard(key string) int64 {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return 0
	}

	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	return int64(len(rObj.Value.(simpleSet)))
}

func (s *store) SIsMember(key string, member string) bool {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return false
	}

	rObj, exists := s.data[key]
	if !exists {
		return false
	}

	set := rObj.Value.(simpleSet)
	_, ok := set[member]
	return ok
}

func (s *store) SMembers(key string) []string {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return []string{}
	}

	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	set := rObj.Value.(simpleSet)
	result := make([]string, 0, len(set))

	for k := range set {
		result = append(result, k)
	}

	return result
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

	set := rObj.Value.(simpleSet)
	for i := range members {
		if _, ok := set[members[i]]; ok {
			result[i] = true
		}
	}

	return result
}

func (s *store) SRem(key string, members ...string) int64 {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return 0
	}

	rObj, exists := s.data[key]
	if !exists {
		return 0
	}

	set := rObj.Value.(simpleSet)
	var removedMembers int64 = 0
	for i := range members {
		if _, ok := set[members[i]]; ok {
			delete(set, members[i])
			removedMembers++
		}
	}

	return removedMembers
}

func (s *store) SPop(key string, count int) []string {
	if isExpired := s.expireIfNeeded(key); isExpired {
		return []string{}
	}
	
	rObj, exists := s.data[key]
	if !exists {
		return []string{}
	}

	set := rObj.Value.(simpleSet)
	setLen := len(set)
	if setLen == 0 {
		return []string{}
	}

	// Pop everything
	if count >= setLen {
		result := make([]string, 0, setLen)
		for m := range set {
			result = append(result, m)
		}
		s.Del(key)
		return result
	}

	indices := floydSamplingIndices(setLen, count)
	result := make([]string, 0, count)

	idx := 0
	for m := range set {
		if _, ok := indices[idx]; ok {
			result = append(result, m)
			delete(set, m)
		}
		idx++
	}

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

	set := rObj.Value.(simpleSet)
	setLen := len(set)
	if setLen == 0 {
		return []string{}
	}

	// Pop everything
	if count > 0 {
		// If count >= size → return all members
		if count >= setLen {
			result := make([]string, 0, setLen)
			for m := range set {
				result = append(result, m)
			}
			return result
		}

		indices := floydSamplingIndices(setLen, count)

		result := make([]string, 0, count)
		idx := 0
		for m := range set {
			if _, ok := indices[idx]; ok {
				result = append(result, m)
			}
			idx++
		}

		return result
	}

	k := -count
	result := make([]string, 0, k)

	members := make([]string, 0, setLen)
	for m := range set {
		members = append(members, m)
	}

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

func floydSamplingIndices(n, k int) map[int]struct{} {
	selected := make(map[int]struct{}, k)

	for i := n - k; i < n; i++ {
		r := rand.Intn(i + 1)
		if _, exists := selected[r]; exists {
			selected[i] = struct{}{}
		} else {
			selected[r] = struct{}{}
		}
	}

	return selected
}