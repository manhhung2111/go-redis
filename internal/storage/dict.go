package storage

import (
	"time"

	"github.com/manhhung2111/go-redis/internal/constant"
)

type Entry struct {
	Value    any
	ExpireAt int64 // in milliseconds
}

type Store struct {
	data map[string]*Entry
}

func NewStore() Store {
	return Store{
		data: make(map[string]*Entry),
	}
}

func (store Store) Get(key string) (any, bool) {
	val, ok := store.data[key]
	if !ok {
		return nil, false
	}

	now := time.Now().UnixMilli()
	if val.ExpireAt != constant.NO_EXPIRE && val.ExpireAt <= now {
		store.Del(key)
		return nil, false
	}

	return val.Value, true
}

func (store Store) Set(key string, value any) {
	store.setWithTTL(key, value, constant.NO_EXPIRE)
}

func (store Store) SetEx(key string, value any, ttlSeconds int64) {
	store.setWithTTL(key, value, ttlSeconds)
}

func (store *Store) Del(key string) bool {
	_, ok := store.data[key]
	if ok {
		delete(store.data, key)
		return true
	}
	return false
}

func (store Store) setWithTTL(key string, value any, ttlSeconds int64) {
	var expireAt int64 = constant.NO_EXPIRE
	if ttlSeconds > 0 {
		expireAt = time.Now().UnixMilli() + ttlSeconds*1000
	}

	store.data[key] = &Entry{
		Value:    value,
		ExpireAt: expireAt,
	}
}
