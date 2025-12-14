package storage

import "github.com/google/wire"

var WireSet = wire.NewSet(
	NewStore,
)