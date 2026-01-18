package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Supports `TTL key` */
func (redis *redis) TTL(cmd core.RedisCmd) []byte {
	if len(cmd.Args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	ttl := redis.Store.TTL(cmd.Args[0])

	if ttl == core.KeyNotExists {
		return core.RespTTLKeyNotExist
	}

	if ttl == core.NoExpire {
		return core.RespTTLKeyExistNoExpire
	}

	return core.EncodeResp(ttl, false)
}

/* Supports EXPIRE key seconds [NX | XX | GT | LT] */
func (redis *redis) Expire(cmd core.RedisCmd) []byte {
	args := cmd.Args
	argsLen := len(args)

	if argsLen < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	ttlSeconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || ttlSeconds <= 0 {
		return core.EncodeResp(util.InvalidExpireTime(cmd.Cmd), false)
	}

	var opt storage.ExpireOptions

	for i := 2; i < argsLen; i++ {
		cmdOpt := strings.ToUpper(args[i])
		switch cmdOpt {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "GT":
			opt.GT = true
		case "LT":
			opt.LT = true
		default:
			return core.EncodeResp(util.InvalidCommandOption(cmdOpt, cmd.Cmd), false)
		}
	}

	// The GT, LT and NX options are mutually exclusive.
	if (opt.NX && opt.XX) || (opt.GT && opt.LT) || (opt.NX && (opt.GT || opt.LT)) {
		return core.RespExpireOptionsNotCompatible
	}

	ok := redis.Store.Expire(key, ttlSeconds, opt)
	if !ok {
		return core.RespExpireTimeoutNotSet
	}

	return core.RespExpireTimeoutSet
}
