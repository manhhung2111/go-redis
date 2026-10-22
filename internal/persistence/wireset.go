package persistence

import "github.com/google/wire"

var WireSet = wire.NewSet(
	NewManager,
)
