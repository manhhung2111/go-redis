package command

import (
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

type Redis interface {
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
