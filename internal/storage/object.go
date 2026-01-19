package storage

import "github.com/manhhung2111/go-redis/internal/config"

type ObjectType uint8
type ObjectEncoding uint8

const (
	ObjString ObjectType = iota
	ObjSet
	ObjList
	ObjHash
	ObjZSet
	ObjBloomFilter
	ObjCuckooFilter
	ObjHyperLogLog
	ObjCountMinSketch

	// ObjAny is a sentinel value to skip type checking in access()
	ObjAny ObjectType = 255
)

const (
	EncRaw ObjectEncoding = iota // string
	EncInt                       // int64
	EncIntSet
	EncHashTable
	EncQuickList
	EncSortedSet
	EncBloomFilter
	EncCuckooFilter
	EncHyperLogLog
	EncCountMinSketch
)

type RObj struct {
	objType  ObjectType
	encoding ObjectEncoding
	value    any
	lru      uint32
}

type evictionPoolEntry struct {
	key  string
	idle uint32
}

type store struct {
	config       *config.Config
	data         Dict[string, *RObj]
	expires      Dict[string, uint64]
	evictionPool []*evictionPoolEntry
	usedMemory   int64 // Memory usage in bytes, accounting only for data and expires dictionaries (excludes eviction pool)
}

func NewStore(cfg *config.Config) Store {
	data, delta1 := newDict[string, *RObj]()
	expires, delta2 := newDict[string, uint64]()
	return &store{
		config:       cfg,
		data:         data,
		expires:      expires,
		evictionPool: make([]*evictionPoolEntry, 0, cfg.EvictionPoolSize),
		usedMemory:   delta1 + delta2,
	}
}

func (s *store) Exists(key string) bool {
	result := s.access(key, ObjAny, false)
	return result.object != nil
}
