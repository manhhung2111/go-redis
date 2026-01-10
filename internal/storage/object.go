package storage

import "github.com/manhhung2111/go-redis/internal/storage/data_structure"

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
	Type     ObjectType
	Encoding ObjectEncoding
	Value    any
}

type Store interface {
	Get(key string) (*string, error)
	Set(key string, value string)
	SetEx(key string, value string, ttlSeconds uint64)
	Del(key string) bool
	IncrBy(key string, increment int64) (*int64, error)
	Exists(key string) bool

	TTL(key string) int64
	Expire(key string, ttlSeconds int64, opt ExpireOptions) bool

	SAdd(key string, members ...string) (int64, error)
	SCard(key string) (int64, error)
	SIsMember(key string, member string) (bool, error)
	SMembers(key string) ([]string, error)
	SMIsMember(key string, member ...string) ([]bool, error)
	SRem(key string, members ...string) (int64, error)
	SPop(key string, count int) ([]string, error)
	SRandMember(key string, count int) ([]string, error)

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

	ZAdd(key string, scoreMember map[string]float64, options data_structure.ZAddOptions) *uint32
	ZCard(key string) uint32
	ZCount(key string, minScore, maxScore float64) uint32
	ZIncrBy(key string, member string, increment float64) (float64, bool)
	ZLexCount(key, minValue, maxValue string) uint32
	ZMScore(key string, members []string) []*float64
	ZPopMax(key string, count int) []string
	ZPopMin(key string, count int) []string
	ZRandMember(key string, count int, withScores bool) []string
	ZRangeByRank(key string, start, stop int, withScores bool) []string
	ZRangeByLex(key string, start, stop string) []string
	ZRangeByScore(key string, start, stop float64, withScores bool) []string
	ZRevRangeByRank(key string, start, stop int, withScores bool) []string
	ZRevRangeByLex(key string, start, stop string) []string
	ZRevRangeByScore(key string, start, stop float64, withScores bool) []string
	ZRank(key string, member string, withScore bool) []any
	ZRem(key string, members []string) uint32
	ZRevRank(key, member string, withScore bool) []any
	ZScore(key, member string) *float64

	GeoAdd(key string, items []data_structure.GeoPoint, options data_structure.ZAddOptions) *uint32
	GeoDist(key, member1, member2, unit string) *float64
	GeoHash(key string, members []string) []*string
	GeoPos(key string, members []string) []*data_structure.GeoPoint
	GeoSearch(key string, options data_structure.GeoSearchOptions) []data_structure.GeoResult

	BFAdd(key string, item string) int
	BFCard(key string) int
	BFExists(key string, item string) int
	BFInfo(key string, option int) []any
	BFMAdd(key string, items []string) []int
	BFMExists(key string, items []string) []int
	BFReserve(key string, errorRate float64, capacity uint32, expansion uint32) error

	CFAdd(key string, item string) int
	CFAddNx(key string, item string) int
	CFCount(key string, item string) int
	CFDel(key string, item string) int
	CFExists(key string, item string) int
	CFInfo(key string) []any
	CFMExists(key string, items []string) []int
	CFReserve(key string, capacity, bucketSize, maxIterations uint64, expansionRate int) error

	PFAdd(key string, items []string) int
	PFCount(keys []string) (int, error)
	PFMerge(destKey string, sourceKeys []string) error

	CMSIncrBy(key string, itemIncrement map[string]uint64) []uint64
	CMSInfo(key string) []any
	CMSInitByDim(key string, width, depth uint64) error
	CMSInitByProb(key string, errorRate, probability float64) error
	CMSQuery(key string, items []string) []uint64
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

func (s *store) Exists(key string) bool {
	result := s.access(key, ObjAny)
	return result.object != nil
}
