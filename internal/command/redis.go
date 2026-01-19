package command

import (
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/errors"
)

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
