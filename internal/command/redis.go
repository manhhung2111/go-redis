package command

import (
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

type Redis interface {
	ActiveExpireCycle() int
	HandleCommand(cmd core.RedisCmd) []byte
	Ping(cmd core.RedisCmd) []byte

	Get(cmd core.RedisCmd) []byte
	Set(cmd core.RedisCmd) []byte
	Del(cmd core.RedisCmd) []byte
	TTL(cmd core.RedisCmd) []byte
	Expire(cmd core.RedisCmd) []byte

	Incr(cmd core.RedisCmd) []byte
	IncrBy(cmd core.RedisCmd) []byte
	Decr(cmd core.RedisCmd) []byte
	DecrBy(cmd core.RedisCmd) []byte

	MGet(cmd core.RedisCmd) []byte
	MSet(cmd core.RedisCmd) []byte

	SAdd(cmd core.RedisCmd) []byte
	SCard(cmd core.RedisCmd) []byte
	SIsMember(cmd core.RedisCmd) []byte
	SMembers(cmd core.RedisCmd) []byte
	SMIsMember(cmd core.RedisCmd) []byte
	SRem(cmd core.RedisCmd) []byte
	SPop(cmd core.RedisCmd) []byte
	SRandMember(cmd core.RedisCmd) []byte

	LPush(cmd core.RedisCmd) []byte
	LPop(cmd core.RedisCmd) []byte
	RPush(cmd core.RedisCmd) []byte
	RPop(cmd core.RedisCmd) []byte
	LRange(cmd core.RedisCmd) []byte
	LIndex(cmd core.RedisCmd) []byte
	LLen(cmd core.RedisCmd) []byte
	LRem(cmd core.RedisCmd) []byte
	LSet(cmd core.RedisCmd) []byte
	LTrim(cmd core.RedisCmd) []byte
	LPushX(cmd core.RedisCmd) []byte
	RPushX(cmd core.RedisCmd) []byte

	HGet(cmd core.RedisCmd) []byte
	HGetAll(cmd core.RedisCmd) []byte
	HMGet(cmd core.RedisCmd) []byte
	HIncrBy(cmd core.RedisCmd) []byte
	HKeys(cmd core.RedisCmd) []byte
	HVals(cmd core.RedisCmd) []byte
	HLen(cmd core.RedisCmd) []byte
	HSet(cmd core.RedisCmd) []byte
	HSetNx(cmd core.RedisCmd) []byte
	HDel(cmd core.RedisCmd) []byte
	HExists(cmd core.RedisCmd) []byte

	ZAdd(cmd core.RedisCmd) []byte
	ZCard(cmd core.RedisCmd) []byte
	ZCount(cmd core.RedisCmd) []byte
	ZIncrBy(cmd core.RedisCmd) []byte
	ZLexCount(cmd core.RedisCmd) []byte
	ZMScore(cmd core.RedisCmd) []byte
	ZPopMax(cmd core.RedisCmd) []byte
	ZPopMin(cmd core.RedisCmd) []byte
	ZRandMember(cmd core.RedisCmd) []byte
	ZRange(cmd core.RedisCmd) []byte
	ZRank(cmd core.RedisCmd) []byte
	ZRem(cmd core.RedisCmd) []byte
	ZRevRank(cmd core.RedisCmd) []byte
	ZScore(cmd core.RedisCmd) []byte

	GeoAdd(cmd core.RedisCmd) []byte
	GeoDist(cmd core.RedisCmd) []byte
	GeoHash(cmd core.RedisCmd) []byte
	GeoPos(cmd core.RedisCmd) []byte
	GeoSearch(cmd core.RedisCmd) []byte

	BFAdd(cmd core.RedisCmd) []byte
	BFCard(cmd core.RedisCmd) []byte
	BFExists(cmd core.RedisCmd) []byte
	BFInfo(cmd core.RedisCmd) []byte
	BFMAdd(cmd core.RedisCmd) []byte
	BFMExists(cmd core.RedisCmd) []byte
	BFReserve(cmd core.RedisCmd) []byte

	CFAdd(cmd core.RedisCmd) []byte
	CFAddNx(cmd core.RedisCmd) []byte
	CFCount(cmd core.RedisCmd) []byte
	CFDel(cmd core.RedisCmd) []byte
	CFExists(cmd core.RedisCmd) []byte
	CFInfo(cmd core.RedisCmd) []byte
	CFMExists(cmd core.RedisCmd) []byte
	CFReserve(cmd core.RedisCmd) []byte

	PFAdd(cmd core.RedisCmd) []byte
	PFCount(cmd core.RedisCmd) []byte
	PFMerge(cmd core.RedisCmd) []byte

	CMSIncrBy(cmd core.RedisCmd) []byte
	CMSInfo(cmd core.RedisCmd) []byte
	CMSInitByDim(cmd core.RedisCmd) []byte
	CMSInitByProb(cmd core.RedisCmd) []byte
	CMSQuery(cmd core.RedisCmd) []byte
}

type CommandHandler func(cmd core.RedisCmd) []byte

type redis struct {
	Store    storage.Store
	handlers map[string]CommandHandler
}

func NewRedis(
	store storage.Store,
) Redis {
	redis := &redis{Store: store}
	redis.handlers = map[string]CommandHandler{
		"PING": redis.Ping,

		"SET":    redis.Set,
		"GET":    redis.Get,
		"DEL":    redis.Del,
		"TTL":    redis.TTL,
		"EXPIRE": redis.Expire,
		"INCR":   redis.Incr,
		"INCRBY": redis.IncrBy,
		"DECR":   redis.Decr,
		"DECRBY": redis.DecrBy,
		"MGET":   redis.MGet,
		"MSET":   redis.MSet,

		"SADD":        redis.SAdd,
		"SCARD":       redis.SCard,
		"SISMEMBER":   redis.SIsMember,
		"SMEMBERS":    redis.SMembers,
		"SMISMEMBER":  redis.SMIsMember,
		"SREM":        redis.SRem,
		"SPOP":        redis.SPop,
		"SRANDMEMBER": redis.SRandMember,

		"LPUSH":  redis.LPush,
		"LPOP":   redis.LPop,
		"RPUSH":  redis.RPush,
		"RPOP":   redis.RPop,
		"LRANGE": redis.LRange,
		"LINDEX": redis.LIndex,
		"LLEN":   redis.LLen,
		"LREM":   redis.LRem,
		"LSET":   redis.LSet,
		"LTRIM":  redis.LTrim,
		"LPUSHX": redis.LPushX,
		"RPUSHX": redis.RPushX,

		"HGET":    redis.HGet,
		"HGETALL": redis.HGetAll,
		"HMGET":   redis.HMGet,
		"HINCRBY": redis.HIncrBy,
		"HKEYS":   redis.HKeys,
		"HVALS":   redis.HVals,
		"HLEN":    redis.HLen,
		"HSET":    redis.HSet,
		"HSETNX":  redis.HSetNx,
		"HDEL":    redis.HDel,
		"HEXISTS": redis.HExists,

		"ZADD":        redis.ZAdd,
		"ZCARD":       redis.ZCard,
		"ZCOUNT":      redis.ZCount,
		"ZINCRBY":     redis.ZIncrBy,
		"ZLEXCOUNT":   redis.ZLexCount,
		"ZMSCORE":     redis.ZMScore,
		"ZPOPMAX":     redis.ZPopMax,
		"ZPOPMIN":     redis.ZPopMin,
		"ZRANDMEMBER": redis.ZRandMember,
		"ZRANGE":      redis.ZRange,
		"ZRANK":       redis.ZRank,
		"ZREM":        redis.ZRem,
		"ZREVRANK":    redis.ZRevRank,
		"ZSCORE":      redis.ZScore,

		"GEOADD":    redis.GeoAdd,
		"GEODIST":   redis.GeoDist,
		"GEOHASH":   redis.GeoHash,
		"GEOPOS":    redis.GeoPos,
		"GEOSEARCH": redis.GeoSearch,

		"BF.ADD":     redis.BFAdd,
		"BF.CARD":    redis.BFCard,
		"BF.EXISTS":  redis.BFExists,
		"BF.INFO":    redis.BFInfo,
		"BF.MADD":    redis.BFMAdd,
		"BF.MEXISTS": redis.BFMExists,
		"BF.RESERVE": redis.BFReserve,

		"CF.ADD":     redis.CFAdd,
		"CF.ADDNX":   redis.CFAddNx,
		"CF.COUNT":   redis.CFCount,
		"CF.DEL":     redis.CFDel,
		"CF.EXISTS":  redis.CFExists,
		"CF.INFO":    redis.CFInfo,
		"CF.MEXISTS": redis.CFMExists,
		"CF.RESERVE": redis.CFReserve,

		"PFADD":   redis.PFAdd,
		"PFCOUNT": redis.PFCount,
		"PFMERGE": redis.PFMerge,

		"CMS.INCRBY":     redis.CMSIncrBy,
		"CMS.INFO":       redis.CMSInfo,
		"CMS.INITBYDIM":  redis.CMSInitByDim,
		"CMS.INITBYPROB": redis.CMSInitByProb,
		"CMS.QUERY":      redis.CMSQuery,
	}

	return redis
}

func (r *redis) HandleCommand(cmd core.RedisCmd) []byte {
	handler, ok := r.handlers[cmd.Cmd]
	if !ok {
		return core.EncodeResp(util.InvalidCommand(cmd.Cmd), false)
	}
	return handler(cmd)
}

func (r *redis) ActiveExpireCycle() int {
	return r.Store.ActiveExpireCycle()
}
