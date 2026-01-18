package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support SADD key member [member ...] */
func (redis *redis) SAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	added, err := redis.Store.SAdd(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(added, false)
}

/* Support SCARD key */
func (redis *redis) SCard(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	setLen, err := redis.Store.SCard(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(setLen, false)
}

/* Support SISMEMBER key member */
func (redis *redis) SIsMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var isMember int64 = 0
	exists, err := redis.Store.SIsMember(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if exists {
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

	members, err := redis.Store.SMembers(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(members, false)
}

/* Support SMISMEMBER key member [member ...] */
func (redis *redis) SMIsMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	isMembers, err := redis.Store.SMIsMember(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

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

	removedElements, err := redis.Store.SRem(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(removedElements, false)
}

/* Support SPOP key [count] */
func (redis *redis) SPop(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount <= 0 {
			return core.RespValueOutOfRangeMustPositive
		}

		count = int(newCount)
	}

	poppedElements, err := redis.Store.SPop(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if len(poppedElements) == 0 {
		return core.RespNilBulkString
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

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return core.RespValueOutOfRangeMustPositive
		}

		count = int(newCount)
	}

	randMembers, err := redis.Store.SRandMember(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if len(args) == 1 && len(randMembers) > 0 {
		return core.EncodeResp(randMembers[0], false)
	}

	return core.EncodeResp(randMembers, false)
}
