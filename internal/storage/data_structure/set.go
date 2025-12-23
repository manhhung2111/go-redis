package data_structure

type Set interface {
	Add(members ...string) int64
	Size() int64
	IsMember(member string) bool
	Members() []string
	MIsMember(members ...string) []bool
	Delete(members ...string) int64
}

