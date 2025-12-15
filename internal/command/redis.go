package command

import (
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
)

type IRedis interface {
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
}

type Redis struct {
	Store storage.Store
}

func NewRedis(
	store storage.Store,
) IRedis {
	return &Redis{
		Store: store,
	}
}