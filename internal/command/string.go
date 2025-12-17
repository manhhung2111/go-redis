package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Supports `GET key` */
func (redis *redis) Get(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(cmd.Args[0])
	if !exists {
		return constant.RESP_NIL_BULK_STRING
	}

	value, ok := rObj.StringValue()
	if !ok {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(value, false)
}

/* Supports `SET key value [NX | XX] [EX seconds]` */
func (redis *redis) Set(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	args := cmd.Args
	key, value := args[0], args[1]

	var (
		nx        bool // SET if Not eXists
		xx        bool // SET if eXists
		expireSec uint64
	)

	for i := 2; i < argsLen; i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX":
			if i+1 >= argsLen {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}

			ttl, err := strconv.ParseUint(args[i+1], 10, 64)
			if err != nil || ttl <= 0 {
				return core.EncodeResp(util.InvalidExpireTime(cmd.Cmd), false)
			}

			expireSec = ttl
			i++
		default:
			return core.EncodeResp(util.InvalidCommandOption(opt, cmd.Cmd), false)
		}
	}

	if nx && xx {
		return core.EncodeResp(util.InvalidCommandOption("NX|XX", cmd.Cmd), false)
	}

	_, exists := redis.Store.Get(key)

	if nx && exists {
		return constant.RESP_NIL_BULK_STRING
	}

	if xx && !exists {
		return constant.RESP_NIL_BULK_STRING
	}

	if expireSec > 0 {
		redis.Store.SetEx(key, value, expireSec)
	} else {
		redis.Store.Set(key, value)
	}

	return constant.RESP_OK
}

/* Supports `DEL key [key...]` */
func (redis *redis) Del(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var deletedKeys int64 = 0
	for i := range argsLen {
		if isDeleted := redis.Store.Del(cmd.Args[i]); isDeleted {
			deletedKeys++
		}
	}

	return core.EncodeResp(deletedKeys, false)
}

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

/* Support MGET key [key ...] */
func (redis *redis) MGet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	res := make([]any, len(args))

	for i := 0; i < len(args); i++ {
		rObj, ok := redis.Store.Get(args[i])
		if !ok {
			res[i] = nil
			continue
		}

		if s, ok := rObj.StringValue(); ok {
			res[i] = s
		} else {
			res[i] = nil
		}
	}

	return core.EncodeResp(res, false)
}

/* Support MSET key value [key value ...] */
func (redis *redis) MSet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) == 0 || len(args)&1 == 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	for i := 0; i < len(args); i += 2 {
		redis.Store.Set(args[i], args[i+1])
	}

	return constant.RESP_OK
}
