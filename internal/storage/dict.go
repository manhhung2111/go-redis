package storage

import (
	"math/rand/v2"

	"github.com/DmitriyVTitov/size"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

type Dict[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V) int64
	Delete(key K) (bool, int64)
	Len() int
	GetRandomKey() K
	Empty() bool
}

type dict[K comparable, V any] struct {
	contents map[K]V
	keys     []K
	keyIndex map[K]int
}

func newDict[K comparable, V any]() (Dict[K, V], int64) {
	d := &dict[K, V]{
		contents: make(map[K]V),
		keys:     make([]K, 0),
		keyIndex: make(map[K]int),
	}

	return d, int64(size.Of(d))
}

func (d *dict[K, V]) Get(key K) (V, bool) {
	v, ok := d.contents[key]
	return v, ok
}

func (d *dict[K, V]) Set(key K, value V) int64 {
	var delta int64

	if oldValue, exists := d.contents[key]; exists {
		// Key exists: only value changes
		delta -= int64(size.Of(oldValue))
		delta += int64(size.Of(value))
	} else {
		// New key: add key, value, and overhead

		// Add key to keyIndex map (key + int index + map overhead)
		delta += int64(size.Of(key))
		delta += int64(size.Of(0)) // int index
		delta += data_structure.MapOverheadPerKey

		// Add key to keys slice
		oldCap := cap(d.keys)
		d.keyIndex[key] = len(d.keys)
		d.keys = append(d.keys, key)
		newCap := cap(d.keys)

		// If slice capacity increased, account for reallocation
		if newCap > oldCap {
			// New backing array allocated
			delta += int64(size.Of(key)) * int64(newCap-oldCap)
		}

		// Add key-value to contents map (key + value + map overhead)
		delta += int64(size.Of(key))
		delta += int64(size.Of(value))
		delta += data_structure.MapOverheadPerKey
	}

	d.contents[key] = value

	return delta
}

func (d *dict[K, V]) Delete(key K) (bool, int64) {
	value, exists := d.contents[key]
	if !exists {
		return false, 0
	}

	var delta int64

	// Memory freed from contents map (key + value + map overhead)
	delta -= int64(size.Of(key))
	delta -= int64(size.Of(value))
	delta -= data_structure.MapOverheadPerKey

	// Memory freed from keyIndex map (key + int + map overhead)
	delta -= int64(size.Of(key))
	delta -= int64(size.Of(0)) // int index
	delta -= data_structure.MapOverheadPerKey

	delete(d.contents, key)

	idx := d.keyIndex[key]
	delete(d.keyIndex, key)

	lastIdx := len(d.keys) - 1
	if idx != lastIdx {
		lastKey := d.keys[lastIdx]
		d.keys[idx] = lastKey
		d.keyIndex[lastKey] = idx
	}

	d.keys = d.keys[:lastIdx]
	return true, delta
}

func (d *dict[K, V]) Len() int {
	return len(d.contents)
}

func (d *dict[K, V]) GetRandomKey() K {
	randomIdx := rand.IntN(len(d.keys))
	return d.keys[randomIdx]
}

func (d *dict[K, V]) Empty() bool {
	return len(d.contents) == 0
}
