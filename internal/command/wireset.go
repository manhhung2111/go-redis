package command

import (
	"github.com/google/wire"
)

var WireSet = wire.NewSet(
	NewRedis,
)