package test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/persistence"
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage"
)

type persistentTestRedis struct {
	command.Redis
	manager *persistence.Manager
	path    string
}

func newPersistentTestRedis(t *testing.T, dir string) persistentTestRedis {
	t.Helper()
	cfg := config.NewConfig()
	cfg.SnapshotDir = dir
	store := storage.NewStore(cfg)
	manager, err := persistence.NewManager(cfg, store)
	require.NoError(t, err)
	return persistentTestRedis{
		Redis:   command.NewRedis(store, manager),
		manager: manager,
		path:    filepath.Join(dir, cfg.SnapshotFilename),
	}
}

var errSaveInProgress = persistence.ErrSaveInProgress

func TestSaveCommand(t *testing.T) {
	dir := t.TempDir()
	r := newPersistentTestRedis(t, dir)
	r.Set(cmd("SET", "a", "1"))
	r.RPush(cmd("RPUSH", "l", "x", "y"))

	assert.Equal(t, protocol.RespOK, r.Save(cmd("SAVE")))
	assert.FileExists(t, r.path)

	// A fresh instance on the same directory sees the data after loading
	r2 := newPersistentTestRedis(t, dir)
	require.NoError(t, r2.manager.Load())
	assert.Equal(t, protocol.EncodeResp("1", false), r2.Get(cmd("GET", "a")))
	assert.Equal(t, protocol.EncodeResp([]string{"x", "y"}, false), r2.LRange(cmd("LRANGE", "l", "0", "-1")))
}

func TestBGSaveCommand(t *testing.T) {
	r := newPersistentTestRedis(t, t.TempDir())
	r.Set(cmd("SET", "a", "1"))

	assert.Equal(t, protocol.EncodeResp("Background saving started", true), r.BGSave(cmd("BGSAVE")))
	assert.Equal(t, protocol.EncodeResp(errSaveInProgress, false), r.BGSave(cmd("BGSAVE")))
	assert.Equal(t, protocol.EncodeResp(errSaveInProgress, false), r.Save(cmd("SAVE")))
	r.manager.Wait()
}

func TestLastSaveCommand(t *testing.T) {
	r := newPersistentTestRedis(t, t.TempDir())
	before := time.Now().Unix()

	r.Save(cmd("SAVE"))
	resp := r.LastSave(cmd("LASTSAVE"))
	value, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, value.(int64), before)
}

func TestPersistenceCommandsRejectArgs(t *testing.T) {
	r := newPersistentTestRedis(t, t.TempDir())
	for _, name := range []string{"SAVE", "BGSAVE", "LASTSAVE"} {
		resp := r.HandleCommand(cmd(name, "extra"))
		assert.Contains(t, string(resp), "wrong number of arguments", name)
	}
}
