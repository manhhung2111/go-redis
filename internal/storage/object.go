package storage

type ObjectType uint8
type ObjectEncoding uint8

const (
	ObjString ObjectType = iota
	ObjSet
)

const (
	EncRaw ObjectEncoding = iota // string
	EncInt                       // int64
	EncHashTable
)

type RObj struct {
	Type     ObjectType
	Encoding ObjectEncoding
	Value    any
}

type Store interface {
	Get(key string) (*RObj, bool)
	Set(key string, value string)
	SetEx(key string, value string, ttlSeconds uint64)
	Del(key string) bool

	TTL(key string) int64
	Expire(key string, ttlSeconds int64, opt ExpireOptions) bool

	SAdd(key string, members ...string) int64
	SCard(key string) int64
	SIsMember(key string, member string) bool
	SMembers(key string) []string
	SMIsMember(key string, member ...string) []bool
	SRem(key string, members ...string) int64
	SPop(key string, count int) []string
	SRandMember(key string, count int) []string
}

type store struct {
	data    map[string]*RObj
	expires map[string]uint64
}

func NewStore() Store {
	return &store{
		data:    make(map[string]*RObj),
		expires: make(map[string]uint64),
	}
}
