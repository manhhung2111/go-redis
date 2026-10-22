package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T) *config.Config {
	cfg := config.NewConfig()
	cfg.SnapshotDir = t.TempDir()
	cfg.SnapshotIntervalSec = 1
	return cfg
}

func newTestManager(t *testing.T, cfg *config.Config) (*Manager, storage.Store) {
	t.Helper()
	store := storage.NewStore(cfg)
	m, err := NewManager(cfg, store)
	require.NoError(t, err)
	return m, store
}

func snapshotPath(cfg *config.Config) string {
	return filepath.Join(cfg.SnapshotDir, cfg.SnapshotFilename)
}

func TestNewManagerValidation(t *testing.T) {
	cfg := testConfig(t)
	cfg.SnapshotIntervalSec = 0
	_, err := NewManager(cfg, storage.NewStore(cfg))
	assert.Error(t, err)

	cfg = testConfig(t)
	cfg.SnapshotDir = filepath.Join(cfg.SnapshotDir, "missing")
	_, err = NewManager(cfg, storage.NewStore(cfg))
	assert.Error(t, err)

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o644))
	cfg = testConfig(t)
	cfg.SnapshotDir = file
	_, err = NewManager(cfg, storage.NewStore(cfg))
	assert.Error(t, err)

	// Disabled snapshots skip validation
	cfg.SnapshotEnabled = false
	_, err = NewManager(cfg, storage.NewStore(cfg))
	assert.NoError(t, err)
}

func TestSaveAndLoad(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")
	store.SAdd("s", "a", "b")

	require.NoError(t, m.Save())
	assert.FileExists(t, snapshotPath(cfg))
	assert.NoFileExists(t, snapshotPath(cfg)+".tmp")
	assert.NoError(t, m.LastSaveErr())

	m2, store2 := newTestManager(t, cfg)
	require.NoError(t, m2.Load())
	v, _ := store2.Get("k")
	require.NotNil(t, v)
	assert.Equal(t, "v", *v)
	n, _ := store2.SCard("s")
	assert.Equal(t, int64(2), n)
}

func TestLoadMissingFile(t *testing.T) {
	m, store := newTestManager(t, testConfig(t))
	require.NoError(t, m.Load())
	assert.Equal(t, 0, store.KeyCount())
}

func TestLoadDisabled(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")
	require.NoError(t, m.Save())

	cfg.SnapshotLoadOnStart = false
	m2, store2 := newTestManager(t, cfg)
	require.NoError(t, m2.Load())
	assert.Equal(t, 0, store2.KeyCount())
}

func TestLoadCorruptFile(t *testing.T) {
	cfg := testConfig(t)
	require.NoError(t, os.WriteFile(snapshotPath(cfg), []byte("GREDIS garbage that is long enough"), 0o644))

	m, _ := newTestManager(t, cfg)
	assert.Error(t, m.Load())
}

func TestLoadRemovesStaleTmp(t *testing.T) {
	cfg := testConfig(t)
	tmp := snapshotPath(cfg) + ".tmp"
	require.NoError(t, os.WriteFile(tmp, []byte("partial"), 0o644))

	m, _ := newTestManager(t, cfg)
	require.NoError(t, m.Load())
	assert.NoFileExists(t, tmp)
}

func TestBackgroundSave(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")

	require.NoError(t, m.BackgroundSave())
	assert.True(t, m.InProgress())
	assert.ErrorIs(t, m.BackgroundSave(), ErrSaveInProgress)
	assert.ErrorIs(t, m.Save(), ErrSaveInProgress)

	// Writes after the snapshot was taken must not leak into it
	store.Set("after", "x")

	m.Wait()
	assert.False(t, m.InProgress())
	require.NoError(t, m.LastSaveErr())

	m2, store2 := newTestManager(t, cfg)
	require.NoError(t, m2.Load())
	assert.Equal(t, 1, store2.KeyCount())
	assert.False(t, store2.Exists("after"))
}

func TestCronSchedulesSaves(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")
	start := time.Now()

	m.Cron(start)
	assert.False(t, m.InProgress(), "must not save before the interval elapses")

	m.Cron(start.Add(2 * time.Second))
	assert.True(t, m.InProgress())

	require.Eventually(t, func() bool {
		m.Cron(start.Add(2 * time.Second))
		return !m.InProgress()
	}, 5*time.Second, 5*time.Millisecond)
	assert.FileExists(t, snapshotPath(cfg))
	assert.False(t, m.LastSave().Before(start))

	// Interval restarts from the end of the last attempt
	m.Cron(time.Now())
	assert.False(t, m.InProgress())
}

func TestCronDisabled(t *testing.T) {
	cfg := testConfig(t)
	cfg.SnapshotEnabled = false
	m, _ := newTestManager(t, cfg)

	m.Cron(time.Now().Add(time.Hour))
	assert.False(t, m.InProgress())
	assert.NoFileExists(t, snapshotPath(cfg))
}

func TestFailedSaveKeepsPreviousSnapshot(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "old")
	require.NoError(t, m.Save())
	before, err := os.ReadFile(snapshotPath(cfg))
	require.NoError(t, err)
	lastSave := m.LastSave()

	// A directory in place of the temp file makes the write fail
	require.NoError(t, os.Mkdir(snapshotPath(cfg)+".tmp", 0o755))
	store.Set("k", "new")

	require.NoError(t, m.BackgroundSave())
	m.Wait()
	assert.Error(t, m.LastSaveErr())
	assert.Equal(t, lastSave, m.LastSave())

	after, err := os.ReadFile(snapshotPath(cfg))
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestShutdownSaves(t *testing.T) {
	cfg := testConfig(t)
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")

	require.NoError(t, m.BackgroundSave())
	store.Set("k2", "v2")
	require.NoError(t, m.Shutdown())

	m2, store2 := newTestManager(t, cfg)
	require.NoError(t, m2.Load())
	assert.Equal(t, 2, store2.KeyCount())
}

func TestShutdownSaveDisabled(t *testing.T) {
	cfg := testConfig(t)
	cfg.SnapshotSaveOnShutdown = false
	m, store := newTestManager(t, cfg)
	store.Set("k", "v")

	require.NoError(t, m.Shutdown())
	assert.NoFileExists(t, snapshotPath(cfg))
}
