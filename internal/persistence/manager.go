// Package persistence schedules and writes keyspace snapshots to disk.
//
// All Manager methods must be called from the event-loop goroutine, the same
// goroutine that executes commands. The only work done elsewhere is writing an
// already-encoded snapshot to disk, which never touches the store.
package persistence

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage"
)

var ErrSaveInProgress = errors.New("ERR Background save already in progress")

type saveResult struct {
	err      error
	size     int
	duration time.Duration
}

type Manager struct {
	config  *config.Config
	store   storage.Store
	path    string
	tmpPath string

	inProgress  bool
	done        chan saveResult
	lastAttempt time.Time // end of the last save attempt, successful or not
	lastSave    time.Time // end of the last successful save
	lastSaveErr error
}

func NewManager(cfg *config.Config, store storage.Store) (*Manager, error) {
	path := filepath.Join(cfg.SnapshotDir, cfg.SnapshotFilename)
	m := &Manager{
		config:      cfg,
		store:       store,
		path:        path,
		tmpPath:     path + ".tmp",
		done:        make(chan saveResult, 1),
		lastAttempt: time.Now(),
		lastSave:    time.Now(),
	}

	if cfg.SnapshotEnabled {
		if err := m.validate(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) validate() error {
	if m.config.SnapshotIntervalSec < 1 {
		return fmt.Errorf("snapshot: interval must be at least 1 second, got %d", m.config.SnapshotIntervalSec)
	}
	if m.config.SnapshotFilename == "" {
		return errors.New("snapshot: filename must not be empty")
	}

	info, err := os.Stat(m.config.SnapshotDir)
	if err != nil {
		return fmt.Errorf("snapshot: directory %q: %w", m.config.SnapshotDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("snapshot: %q is not a directory", m.config.SnapshotDir)
	}

	probe, err := os.CreateTemp(m.config.SnapshotDir, ".write-probe-*")
	if err != nil {
		return fmt.Errorf("snapshot: directory %q is not writable: %w", m.config.SnapshotDir, err)
	}
	probe.Close()
	os.Remove(probe.Name())
	return nil
}

// Load restores the store from the snapshot file if one exists. A missing
// file is not an error; a corrupt or unreadable one is.
func (m *Manager) Load() error {
	if err := os.Remove(m.tmpPath); err == nil {
		log.Printf("snapshot: removed stale temporary file %s", m.tmpPath)
	}

	if !m.config.SnapshotLoadOnStart {
		return nil
	}

	start := time.Now()
	data, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("snapshot: no snapshot found at %s, starting empty", m.path)
		return nil
	}
	if err != nil {
		return fmt.Errorf("snapshot: read %s: %w", m.path, err)
	}

	if err := m.store.LoadSnapshot(data); err != nil {
		return fmt.Errorf("snapshot: load %s: %w (move the file aside to start empty)", m.path, err)
	}

	log.Printf("snapshot: loaded %d keys (%d bytes) from %s in %s",
		m.store.KeyCount(), len(data), m.path, time.Since(start).Round(time.Millisecond))
	return nil
}

// Cron runs on every server tick: it collects finished background saves and
// starts a new one when the interval has elapsed.
func (m *Manager) Cron(now time.Time) {
	m.Poll()

	if !m.config.SnapshotEnabled || m.inProgress {
		return
	}
	interval := time.Duration(m.config.SnapshotIntervalSec) * time.Second
	if now.Sub(m.lastAttempt) < interval {
		return
	}

	if err := m.BackgroundSave(); err != nil {
		log.Printf("snapshot: background save failed to start: %v", err)
	}
}

// BackgroundSave encodes the keyspace on the calling (event-loop) goroutine
// and writes it to disk from a separate goroutine.
func (m *Manager) BackgroundSave() error {
	if m.inProgress {
		return ErrSaveInProgress
	}

	start := time.Now()
	data, err := m.store.Snapshot()
	if err != nil {
		m.finish(saveResult{err: err})
		return err
	}
	encodeTime := time.Since(start)
	log.Printf("snapshot: encoded %d keys (%d bytes) in %s, writing in background",
		m.store.KeyCount(), len(data), encodeTime.Round(time.Millisecond))

	m.inProgress = true
	go func() {
		writeStart := time.Now()
		err := writeFileAtomic(m.path, m.tmpPath, data)
		m.done <- saveResult{err: err, size: len(data), duration: time.Since(writeStart)}
	}()
	return nil
}

// Poll collects the result of a finished background save without blocking.
func (m *Manager) Poll() {
	if !m.inProgress {
		return
	}
	select {
	case res := <-m.done:
		m.inProgress = false
		m.finish(res)
	default:
	}
}

// Wait blocks until an in-flight background save, if any, has finished.
func (m *Manager) Wait() {
	if !m.inProgress {
		return
	}
	res := <-m.done
	m.inProgress = false
	m.finish(res)
}

// Save encodes and writes a snapshot synchronously.
func (m *Manager) Save() error {
	if m.inProgress {
		return ErrSaveInProgress
	}

	start := time.Now()
	data, err := m.store.Snapshot()
	if err == nil {
		err = writeFileAtomic(m.path, m.tmpPath, data)
	}
	m.finish(saveResult{err: err, size: len(data), duration: time.Since(start)})
	return err
}

// Shutdown waits for a running background save and then, if enabled, writes
// a final snapshot.
func (m *Manager) Shutdown() error {
	m.Wait()
	if !m.config.SnapshotEnabled || !m.config.SnapshotSaveOnShutdown {
		return nil
	}
	log.Println("snapshot: saving before shutdown")
	return m.Save()
}

func (m *Manager) finish(res saveResult) {
	m.lastAttempt = time.Now()
	m.lastSaveErr = res.err
	if res.err != nil {
		log.Printf("snapshot: save to %s failed: %v", m.path, res.err)
		return
	}
	m.lastSave = m.lastAttempt
	log.Printf("snapshot: saved %d bytes to %s (write %s)", res.size, m.path, res.duration.Round(time.Millisecond))
}

// LastSave returns the time of the last successful save. Before the first
// save it is the server start time.
func (m *Manager) LastSave() time.Time {
	return m.lastSave
}

// LastSaveErr returns the error of the most recent save attempt, if any.
func (m *Manager) LastSaveErr() error {
	return m.lastSaveErr
}

func (m *Manager) InProgress() bool {
	return m.inProgress
}

// writeFileAtomic writes data to tmpPath, fsyncs it, renames it over path and
// fsyncs the directory, so path always holds either the old or the new
// snapshot in full.
func writeFileAtomic(path, tmpPath string, data []byte) (err error) {
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.Remove(tmpPath)
		}
	}()

	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
