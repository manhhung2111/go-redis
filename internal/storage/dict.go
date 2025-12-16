package storage

import (
	"time"

	"github.com/manhhung2111/go-redis/internal/constant"
)

type Store interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	SetEx(key string, value any, ttlSeconds int64)
	Del(key string) bool
	GetEntry(key string) *Entry
	SetExpire(key string, ttlSeconds int64) bool
	SetValue(key string, value any) bool
}

type Entry struct {
	Value    any
	ExpireAt int64 // in milliseconds
}

type store struct {
	data map[string]*Entry
}

func NewStore() Store {
	return &store{
		data: make(map[string]*Entry),
	}
}

func (store *store) Get(key string) (any, bool) {
	entry, ok := store.data[key]
	if !ok {
		return nil, false
	}

	now := time.Now().UnixMilli()
	if entry.ExpireAt != constant.NO_EXPIRE && entry.ExpireAt <= now {
		store.Del(key)
		return nil, false
	}

	return entry.Value, true
}

func (store *store) Set(key string, value any) {
	store.setWithTTL(key, value, constant.NO_EXPIRE)
}

func (store *store) SetEx(key string, value any, ttlSeconds int64) {
	store.setWithTTL(key, value, ttlSeconds)
}

func (store *store) Del(key string) bool {
	_, ok := store.data[key]
	if ok {
		delete(store.data, key)
		return true
	}
	return false
}

func (store *store) GetEntry(key string) *Entry {
	entry, ok := store.data[key]
	if !ok {
		return nil
	}
	return entry
}

func (store *store) SetExpire(key string, ttlSeconds int64) bool {
	entry, ok := store.data[key]
	if !ok {
		return false
	}

	if ttlSeconds <= 0 {
		return false
	}

	entry.ExpireAt = time.Now().UnixMilli() + ttlSeconds*1000
	return true
}

func (store *store) SetValue(key string, value any) bool {
	entry, ok := store.data[key]
	if !ok {
		return false
	}

	entry.Value = value
	return true
}

func (store *store) setWithTTL(key string, value any, ttlSeconds int64) {
	var expireAt int64 = constant.NO_EXPIRE
	if ttlSeconds > 0 {
		expireAt = time.Now().UnixMilli() + ttlSeconds*1000
	}

	store.data[key] = &Entry{
		Value:    value,
		ExpireAt: expireAt,
	}
}
