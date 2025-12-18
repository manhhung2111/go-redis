package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/constant"
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

	if ttl == constant.KEY_NOT_EXISTS {
		return constant.RESP_TTL_KEY_NOT_EXIST
	}

	if ttl == constant.NO_EXPIRE {
		return constant.RESP_TTL_KEY_EXIST_NO_EXPIRE
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
		return constant.RESP_EXPIRE_OPTIONS_NOT_COMPATIBLE
	}

	ok := redis.Store.Expire(key, ttlSeconds, opt)
	if !ok {
		return constant.RESP_EXPIRE_TIMEOUT_NOT_SET
	}

	return constant.RESP_EXPIRE_TIMEOUT_SET
}
