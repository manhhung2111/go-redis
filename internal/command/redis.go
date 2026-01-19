package command

import (
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/errors"
)

type Redis interface {
	ActiveExpireCycle() int
	HandleCommand(cmd protocol.RedisCmd) []byte
	Ping(cmd protocol.RedisCmd) []byte

	Get(cmd protocol.RedisCmd) []byte
	Set(cmd protocol.RedisCmd) []byte
	Del(cmd protocol.RedisCmd) []byte
	TTL(cmd protocol.RedisCmd) []byte
	Expire(cmd protocol.RedisCmd) []byte

	Incr(cmd protocol.RedisCmd) []byte
	IncrBy(cmd protocol.RedisCmd) []byte
	Decr(cmd protocol.RedisCmd) []byte
	DecrBy(cmd protocol.RedisCmd) []byte

	MGet(cmd protocol.RedisCmd) []byte
	MSet(cmd protocol.RedisCmd) []byte

	SAdd(cmd protocol.RedisCmd) []byte
	SCard(cmd protocol.RedisCmd) []byte
	SIsMember(cmd protocol.RedisCmd) []byte
	SMembers(cmd protocol.RedisCmd) []byte
	SMIsMember(cmd protocol.RedisCmd) []byte
	SRem(cmd protocol.RedisCmd) []byte
	SPop(cmd protocol.RedisCmd) []byte
	SRandMember(cmd protocol.RedisCmd) []byte

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

	GeoAdd(cmd protocol.RedisCmd) []byte
	GeoDist(cmd protocol.RedisCmd) []byte
	GeoHash(cmd protocol.RedisCmd) []byte
	GeoPos(cmd protocol.RedisCmd) []byte
	GeoSearch(cmd protocol.RedisCmd) []byte

	BFAdd(cmd protocol.RedisCmd) []byte
	BFCard(cmd protocol.RedisCmd) []byte
	BFExists(cmd protocol.RedisCmd) []byte
	BFInfo(cmd protocol.RedisCmd) []byte
	BFMAdd(cmd protocol.RedisCmd) []byte
	BFMExists(cmd protocol.RedisCmd) []byte
	BFReserve(cmd protocol.RedisCmd) []byte

	CFAdd(cmd protocol.RedisCmd) []byte
	CFAddNx(cmd protocol.RedisCmd) []byte
	CFCount(cmd protocol.RedisCmd) []byte
	CFDel(cmd protocol.RedisCmd) []byte
	CFExists(cmd protocol.RedisCmd) []byte
	CFInfo(cmd protocol.RedisCmd) []byte
	CFMExists(cmd protocol.RedisCmd) []byte
	CFReserve(cmd protocol.RedisCmd) []byte

	PFAdd(cmd protocol.RedisCmd) []byte
	PFCount(cmd protocol.RedisCmd) []byte
	PFMerge(cmd protocol.RedisCmd) []byte

	CMSIncrBy(cmd protocol.RedisCmd) []byte
	CMSInfo(cmd protocol.RedisCmd) []byte
	CMSInitByDim(cmd protocol.RedisCmd) []byte
	CMSInitByProb(cmd protocol.RedisCmd) []byte
	CMSQuery(cmd protocol.RedisCmd) []byte
}

type CommandHandler func(cmd protocol.RedisCmd) []byte

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

func (r *redis) HandleCommand(cmd protocol.RedisCmd) []byte {
	handler, ok := r.handlers[cmd.Cmd]
	if !ok {
		return protocol.EncodeResp(errors.InvalidCommand(cmd.Cmd), false)
	}
	return handler(cmd)
}

func (r *redis) ActiveExpireCycle() int {
	return r.Store.ActiveExpireCycle()
}
