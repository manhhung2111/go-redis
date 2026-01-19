//go:build wireinject
// +build wireinject

//
// go:generate go run github.com/google/wire/cmd/wire

package wiring

import (
	"github.com/google/wire"
	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/server"
	"github.com/manhhung2111/go-redis/internal/storage"
)

var WireSet = wire.NewSet(
	storage.WireSet,
	command.WireSet,
	server.WireSet,
)

func InitializeServer(cfg *config.Config) (*server.Server, error) {
	wire.Build(WireSet)
	return nil, nil
}
