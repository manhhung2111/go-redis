package storage

type ObjectType uint8
type ObjectEncoding uint8

const (
	ObjString ObjectType = iota
)

const (
	EncRaw ObjectEncoding = iota // string
	EncInt                       // int64
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
