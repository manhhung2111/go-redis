package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support SADD key member [member ...] */
func (redis *redis) SAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	rObj, existing := redis.Store.Get(key)
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	added := redis.Store.SAdd(key, args[1:]...)
	return core.EncodeResp(added, false)
}

/* Support SCARD key */
func (redis *redis) SCard(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	rObj, existing := redis.Store.Get(key)
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	setLen := redis.Store.SCard(key)
	return core.EncodeResp(setLen, false)
}

/* Support SISMEMBER key member */
func (redis *redis) SIsMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	rObj, existing := redis.Store.Get(key)
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	var isMember int64 = 0
	if redis.Store.SIsMember(key, args[1]) {
		isMember = 1
	}

	return core.EncodeResp(isMember, false)
}

/* Support SMEMBERS key */
func (redis *redis) SMembers(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	members := redis.Store.SMembers(args[0])
	return core.EncodeResp(members, false)
}

/* Support SMISMEMBER key member [member ...] */
func (redis *redis) SMIsMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	isMembers := redis.Store.SMIsMember(args[0], args[1:]...)
	result := make([]int64, len(isMembers))

	for i := range isMembers {
		result[i] = 0
		if isMembers[i] {
			result[i] = 1
		}
	}

	return core.EncodeResp(result, false)
}

/* Support SREM key member [member ...] */
func (redis *redis) SRem(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	removedElements := redis.Store.SRem(args[0], args[1:]...)
	return core.EncodeResp(removedElements, false)
}

/* Support SPOP key [count] */
func (redis *redis) SPop(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return constant.RESP_NIL_BULK_STRING
	}

	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount <= 0 {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}

		count = int(newCount)
	}

	poppedElements := redis.Store.SPop(args[0], count)
	if len(poppedElements) == 0 {
		return constant.RESP_NIL_BULK_STRING
	}

	if len(args) == 1 {
		return core.EncodeResp(poppedElements[0], false)
	}
	return core.EncodeResp(poppedElements, false)
}

/* Support SRANDMEMBER key [count] */
func (redis *redis) SRandMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return constant.RESP_NIL_BULK_STRING
	}

	if existing && rObj.Type != storage.ObjSet {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}

		count = int(newCount)
	}

	randMembers := redis.Store.SRandMember(args[0], count)
	if len(args) == 1 && len(randMembers) > 0 {
		return core.EncodeResp(randMembers[0], false)
	}

	return core.EncodeResp(randMembers, false)
}
