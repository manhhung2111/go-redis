package command

import (
	"fmt"
	"strings"

	"github.com/manhhung2111/go-redis/internal/errors"
	"github.com/manhhung2111/go-redis/internal/protocol"
)

/* Support SAVE */
func (redis *redis) Save(cmd protocol.RedisCmd) []byte {
	if len(cmd.Args) != 0 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	if err := redis.Persistence.Save(); err != nil {
		return protocol.EncodeResp(persistenceError(err), false)
	}

	return protocol.RespOK
}

/* Support BGSAVE */
func (redis *redis) BGSave(cmd protocol.RedisCmd) []byte {
	if len(cmd.Args) != 0 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	if err := redis.Persistence.BackgroundSave(); err != nil {
		return protocol.EncodeResp(persistenceError(err), false)
	}

	return protocol.EncodeResp("Background saving started", true)
}

/* Support LASTSAVE */
func (redis *redis) LastSave(cmd protocol.RedisCmd) []byte {
	if len(cmd.Args) != 0 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	return protocol.EncodeResp(redis.Persistence.LastSave().Unix(), false)
}

func persistenceError(err error) error {
	if strings.HasPrefix(err.Error(), "ERR ") {
		return err
	}
	return fmt.Errorf("ERR %w", err)
}
