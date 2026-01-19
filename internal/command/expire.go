package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/errors"
)

/* Supports `TTL key` */
func (redis *redis) TTL(cmd protocol.RedisCmd) []byte {
	if len(cmd.Args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	ttl := redis.Store.TTL(cmd.Args[0])

	if ttl == protocol.KeyNotExists {
		return protocol.RespTTLKeyNotExist
	}

	if ttl == protocol.NoExpire {
		return protocol.RespTTLKeyExistNoExpire
	}

	return protocol.EncodeResp(ttl, false)
}

/* Supports EXPIRE key seconds [NX | XX | GT | LT] */
func (redis *redis) Expire(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	argsLen := len(args)

	if argsLen < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	ttlSeconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || ttlSeconds <= 0 {
		return protocol.EncodeResp(errors.InvalidExpireTime(cmd.Cmd), false)
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
			return protocol.EncodeResp(errors.InvalidCommandOption(cmdOpt, cmd.Cmd), false)
		}
	}

	// The GT, LT and NX options are mutually exclusive.
	if (opt.NX && opt.XX) || (opt.GT && opt.LT) || (opt.NX && (opt.GT || opt.LT)) {
		return protocol.RespExpireOptionsNotCompatible
	}

	ok := redis.Store.Expire(key, ttlSeconds, opt)
	if !ok {
		return protocol.RespExpireTimeoutNotSet
	}

	return protocol.RespExpireTimeoutSet
}
