package storage

import "github.com/manhhung2111/go-redis/internal/storage/types"

type StringStore interface {
	Get(key string) (*string, error)
	Set(key string, value string)
	SetEx(key string, value string, ttlSeconds uint64)
	Del(key string) bool
	IncrBy(key string, increment int64) (*int64, error)
	Exists(key string) bool
}

type ExpireStore interface {
	TTL(key string) int64
	Expire(key string, ttlSeconds int64, opt ExpireOptions) bool
	ActiveExpireCycle() int
}

type SetStore interface {
	SAdd(key string, members ...string) (int64, error)
	SCard(key string) (int64, error)
	SIsMember(key string, member string) (bool, error)
	SMembers(key string) ([]string, error)
	SMIsMember(key string, member ...string) ([]bool, error)
	SRem(key string, members ...string) (int64, error)
	SPop(key string, count int) ([]string, error)
	SRandMember(key string, count int) ([]string, error)
}

type ListStore interface {
	LPush(key string, elements ...string) (uint32, error)
	LPop(key string, count uint32) ([]string, error)
	RPush(key string, elements ...string) (uint32, error)
	RPop(key string, count uint32) ([]string, error)
	LRange(key string, start, end int32) ([]string, error)
	LIndex(key string, index int32) (*string, error)
	LLen(key string) (uint32, error)
	LRem(key string, count int32, element string) (uint32, error)
	LSet(key string, index int32, element string) error
	LTrim(key string, start, end int32) error
}

type HashStore interface {
	HGet(key, field string) (*string, error)
	HGetAll(key string) ([]string, error)
	HMGet(key string, fields []string) ([]*string, error)
	HIncrBy(key string, field string, increment int64) (int64, error)
	HKeys(key string) ([]string, error)
	HVals(key string) ([]string, error)
	HLen(key string) (uint32, error)
	HSet(key string, fieldValue map[string]string) (int64, error)
	HSetNx(key, field, value string) (int64, error)
	HDel(key string, fields []string) (int64, error)
	HExists(key, field string) (int64, error)
}

type ZSetStore interface {
	ZAdd(key string, scoreMember map[string]float64, options types.ZAddOptions) (*uint32, error)
	ZCard(key string) (uint32, error)
	ZCount(key string, minScore, maxScore float64) (uint32, error)
	ZIncrBy(key string, member string, increment float64) (float64, error)
	ZLexCount(key, minValue, maxValue string) (uint32, error)
	ZMScore(key string, members []string) ([]*float64, error)
	ZPopMax(key string, count int) ([]string, error)
	ZPopMin(key string, count int) ([]string, error)
	ZRandMember(key string, count int, withScores bool) ([]string, error)
	ZRangeByRank(key string, start, stop int, withScores bool) ([]string, error)
	ZRangeByLex(key string, start, stop string) ([]string, error)
	ZRangeByScore(key string, start, stop float64, withScores bool) ([]string, error)
	ZRevRangeByRank(key string, start, stop int, withScores bool) ([]string, error)
	ZRevRangeByLex(key string, start, stop string) ([]string, error)
	ZRevRangeByScore(key string, start, stop float64, withScores bool) ([]string, error)
	ZRank(key string, member string, withScore bool) ([]any, error)
	ZRem(key string, members []string) (uint32, error)
	ZRevRank(key, member string, withScore bool) ([]any, error)
	ZScore(key, member string) (*float64, error)
}

type GeoStore interface {
	GeoAdd(key string, items []types.GeoPoint, options types.ZAddOptions) (*uint32, error)
	GeoDist(key, member1, member2, unit string) (*float64, error)
	GeoHash(key string, members []string) ([]*string, error)
	GeoPos(key string, members []string) ([]*types.GeoPoint, error)
	GeoSearch(key string, options types.GeoSearchOptions) ([]types.GeoResult, error)
}

type BloomFilterStore interface {
	BFAdd(key string, item string) (int, error)
	BFCard(key string) (int, error)
	BFExists(key string, item string) (int, error)
	BFInfo(key string, option int) ([]any, error)
	BFMAdd(key string, items []string) ([]int, error)
	BFMExists(key string, items []string) ([]int, error)
	BFReserve(key string, errorRate float64, capacity uint32, expansion uint32) error
}

type CuckooFilterStore interface {
	CFAdd(key string, item string) (int, error)
	CFAddNx(key string, item string) (int, error)
	CFCount(key string, item string) (int, error)
	CFDel(key string, item string) (int, error)
	CFExists(key string, item string) (int, error)
	CFInfo(key string) ([]any, error)
	CFMExists(key string, items []string) ([]int, error)
	CFReserve(key string, capacity, bucketSize, maxIterations uint64, expansionRate int) error
}

type HyperLogLogStore interface {
	PFAdd(key string, items []string) (int, error)
	PFCount(keys []string) (int, error)
	PFMerge(destKey string, sourceKeys []string) error
}

type CMSStore interface {
	CMSIncrBy(key string, itemIncrement map[string]uint64) ([]uint64, error)
	CMSInfo(key string) ([]any, error)
	CMSInitByDim(key string, width, depth uint64) error
	CMSInitByProb(key string, errorRate, probability float64) error
	CMSQuery(key string, items []string) ([]uint64, error)
}

// Store combines all storage interfaces
type Store interface {
	StringStore
	ExpireStore
	SetStore
	ListStore
	HashStore
	ZSetStore
	GeoStore
	BloomFilterStore
	CuckooFilterStore
	HyperLogLogStore
	CMSStore
}
