package test

import (
	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
)

func newTestRedis() command.Redis {
	return command.NewRedis(
		storage.NewStore(config.NewConfig()),
	)
}

func cmd(name string, args ...string) protocol.RedisCmd {
	return protocol.RedisCmd{
		Cmd:  name,
		Args: args,
	}
}
