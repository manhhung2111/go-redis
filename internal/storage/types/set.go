package types

type Set interface {
	Add(members ...string) (int64, bool, int64)
	Size() int64
	IsMember(member string) bool
	Members() []string
	MIsMember(members ...string) []bool
	Delete(members ...string) (int64, int64)
	MemoryUsage() int64
}

