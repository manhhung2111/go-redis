package test

import (
	"os"

	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/persistence"
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
)

func newTestRedis() command.Redis {
	cfg := config.NewConfig()
	cfg.SnapshotEnabled = false
	cfg.SnapshotDir = os.TempDir()
	store := storage.NewStore(cfg)
	manager, err := persistence.NewManager(cfg, store)
	if err != nil {
		panic(err)
	}
	return command.NewRedis(store, manager)
}

func cmd(name string, args ...string) protocol.RedisCmd {
	return protocol.RedisCmd{
		Cmd:  name,
		Args: args,
	}
}
