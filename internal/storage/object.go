package storage

type ObjectType uint8
type ObjectEncoding uint8

const (
	ObjString ObjectType = iota
	ObjSet
	ObjList
	ObjHash
)

const (
	EncRaw ObjectEncoding = iota // string
	EncInt                       // int64
	EncIntSet
	EncHashTable
	EncQuickList
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

	LPush(key string, elements ...string) uint32
	LPop(key string, count uint32) []string
	RPush(key string, elements ...string) uint32
	RPop(key string, count uint32) []string
	LRange(key string, start, end int32) []string
	LIndex(key string, index int32) (string, bool)
	LLen(key string) uint32
	LRem(key string, count int32, element string) uint32
	LSet(key string, index int32, element string) error
	LTrim(key string, start, end int32)

	HGet(key, field string) (string, bool)
	HGetAll(key string) []string
	HMGet(key string, fields []string) []*string
	HIncrBy(key string, field string, increment int64) (int64, error)
	HKeys(key string) []string
	HVals(key string) []string
	HLen(key string) uint32
	HSet(key string, fieldValue map[string]string) int64
	HSetNx(key, field, value string) int64
	HDel(key string, fields []string) int64
	HExists(key, field string) int64
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
