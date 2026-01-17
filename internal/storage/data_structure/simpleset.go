package data_structure

import "github.com/DmitriyVTitov/size"

type simpleSet struct {
	contents map[string]struct{}
}

func NewSimpleSet() Set {
	return &simpleSet{
		contents: make(map[string]struct{}),
	}
}

func (s *simpleSet) Add(members ...string) (int64, bool, int64) {
	var added int64 = 0
	var delta int64 = 0
	for _, m := range members {
		if _, ok := s.contents[m]; !ok {
			s.contents[m] = struct{}{}
			added++
			delta += StringMapEntrySize(m)
		}
	}
	return added, true, delta
}

func (s *simpleSet) Delete(members ...string) (int64, int64) {
	var removedMembers int64 = 0
	var delta int64 = 0
	for i := range members {
		if _, ok := s.contents[members[i]]; ok {
			delta -= StringMapEntrySize(members[i])
			delete(s.contents, members[i])
			removedMembers++
		}
	}

	return removedMembers, delta
}

func (s *simpleSet) IsMember(member string) bool {
	_, ok := s.contents[member]
	return ok
}

func (s *simpleSet) MIsMember(members ...string) []bool {
	result := make([]bool, len(members))
	for i := range members {
		_, ok := s.contents[members[i]]
		result[i] = ok
	}

	return result
}

func (s *simpleSet) Members() []string {
	result := make([]string, 0, len(s.contents))

	for k := range s.contents {
		result = append(result, k)
	}

	return result
}

func (s *simpleSet) Size() int64 {
	return int64(len(s.contents))
}

func (s *simpleSet) MemoryUsage() int64 {
	return int64(size.Of(s))
}
