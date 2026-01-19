package command

import "github.com/manhhung2111/go-redis/internal/protocol"

type StringCommands interface {
	Get(cmd protocol.RedisCmd) []byte
	Set(cmd protocol.RedisCmd) []byte
	Del(cmd protocol.RedisCmd) []byte
	Incr(cmd protocol.RedisCmd) []byte
	IncrBy(cmd protocol.RedisCmd) []byte
	Decr(cmd protocol.RedisCmd) []byte
	DecrBy(cmd protocol.RedisCmd) []byte
	MGet(cmd protocol.RedisCmd) []byte
	MSet(cmd protocol.RedisCmd) []byte
}

type ExpireCommands interface {
	TTL(cmd protocol.RedisCmd) []byte
	Expire(cmd protocol.RedisCmd) []byte
	ActiveExpireCycle() int
}

type SetCommands interface {
	SAdd(cmd protocol.RedisCmd) []byte
	SCard(cmd protocol.RedisCmd) []byte
	SIsMember(cmd protocol.RedisCmd) []byte
	SMembers(cmd protocol.RedisCmd) []byte
	SMIsMember(cmd protocol.RedisCmd) []byte
	SRem(cmd protocol.RedisCmd) []byte
	SPop(cmd protocol.RedisCmd) []byte
	SRandMember(cmd protocol.RedisCmd) []byte
}

type ListCommands interface {
	LPush(cmd protocol.RedisCmd) []byte
	LPop(cmd protocol.RedisCmd) []byte
	RPush(cmd protocol.RedisCmd) []byte
	RPop(cmd protocol.RedisCmd) []byte
	LRange(cmd protocol.RedisCmd) []byte
	LIndex(cmd protocol.RedisCmd) []byte
	LLen(cmd protocol.RedisCmd) []byte
	LRem(cmd protocol.RedisCmd) []byte
	LSet(cmd protocol.RedisCmd) []byte
	LTrim(cmd protocol.RedisCmd) []byte
	LPushX(cmd protocol.RedisCmd) []byte
	RPushX(cmd protocol.RedisCmd) []byte
}

type HashCommands interface {
	HGet(cmd protocol.RedisCmd) []byte
	HGetAll(cmd protocol.RedisCmd) []byte
	HMGet(cmd protocol.RedisCmd) []byte
	HIncrBy(cmd protocol.RedisCmd) []byte
	HKeys(cmd protocol.RedisCmd) []byte
	HVals(cmd protocol.RedisCmd) []byte
	HLen(cmd protocol.RedisCmd) []byte
	HSet(cmd protocol.RedisCmd) []byte
	HSetNx(cmd protocol.RedisCmd) []byte
	HDel(cmd protocol.RedisCmd) []byte
	HExists(cmd protocol.RedisCmd) []byte
}

type ZSetCommands interface {
	ZAdd(cmd protocol.RedisCmd) []byte
	ZCard(cmd protocol.RedisCmd) []byte
	ZCount(cmd protocol.RedisCmd) []byte
	ZIncrBy(cmd protocol.RedisCmd) []byte
	ZLexCount(cmd protocol.RedisCmd) []byte
	ZMScore(cmd protocol.RedisCmd) []byte
	ZPopMax(cmd protocol.RedisCmd) []byte
	ZPopMin(cmd protocol.RedisCmd) []byte
	ZRandMember(cmd protocol.RedisCmd) []byte
	ZRange(cmd protocol.RedisCmd) []byte
	ZRank(cmd protocol.RedisCmd) []byte
	ZRem(cmd protocol.RedisCmd) []byte
	ZRevRank(cmd protocol.RedisCmd) []byte
	ZScore(cmd protocol.RedisCmd) []byte
}

type GeoCommands interface {
	GeoAdd(cmd protocol.RedisCmd) []byte
	GeoDist(cmd protocol.RedisCmd) []byte
	GeoHash(cmd protocol.RedisCmd) []byte
	GeoPos(cmd protocol.RedisCmd) []byte
	GeoSearch(cmd protocol.RedisCmd) []byte
}

type BloomFilterCommands interface {
	BFAdd(cmd protocol.RedisCmd) []byte
	BFCard(cmd protocol.RedisCmd) []byte
	BFExists(cmd protocol.RedisCmd) []byte
	BFInfo(cmd protocol.RedisCmd) []byte
	BFMAdd(cmd protocol.RedisCmd) []byte
	BFMExists(cmd protocol.RedisCmd) []byte
	BFReserve(cmd protocol.RedisCmd) []byte
}

type CuckooFilterCommands interface {
	CFAdd(cmd protocol.RedisCmd) []byte
	CFAddNx(cmd protocol.RedisCmd) []byte
	CFCount(cmd protocol.RedisCmd) []byte
	CFDel(cmd protocol.RedisCmd) []byte
	CFExists(cmd protocol.RedisCmd) []byte
	CFInfo(cmd protocol.RedisCmd) []byte
	CFMExists(cmd protocol.RedisCmd) []byte
	CFReserve(cmd protocol.RedisCmd) []byte
}

type HyperLogLogCommands interface {
	PFAdd(cmd protocol.RedisCmd) []byte
	PFCount(cmd protocol.RedisCmd) []byte
	PFMerge(cmd protocol.RedisCmd) []byte
}

type CMSCommands interface {
	CMSIncrBy(cmd protocol.RedisCmd) []byte
	CMSInfo(cmd protocol.RedisCmd) []byte
	CMSInitByDim(cmd protocol.RedisCmd) []byte
	CMSInitByProb(cmd protocol.RedisCmd) []byte
	CMSQuery(cmd protocol.RedisCmd) []byte
}

type Redis interface {
	HandleCommand(cmd protocol.RedisCmd) []byte
	Ping(cmd protocol.RedisCmd) []byte
	StringCommands
	ExpireCommands
	SetCommands
	ListCommands
	HashCommands
	ZSetCommands
	GeoCommands
	BloomFilterCommands
	CuckooFilterCommands
	HyperLogLogCommands
	CMSCommands
}
