package storage

import "math/rand/v2"

type Dict[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K) bool
	Len() int
	GetRandomKey() K
	Empty() bool
}

type dict[K comparable, V any] struct {
	contents map[K]V
	keys     []K
	keyIndex map[K]int
}

func newDict[K comparable, V any]() Dict[K, V] {
	return &dict[K, V]{
		contents: make(map[K]V),
		keys:     make([]K, 0),
		keyIndex: make(map[K]int),
	}
}

func (d *dict[K, V]) Get(key K) (V, bool) {
	v, ok := d.contents[key]
	return v, ok
}

func (d *dict[K, V]) Set(key K, value V) {
	if _, exists := d.contents[key]; !exists {
		d.keyIndex[key] = len(d.keys)
		d.keys = append(d.keys, key)
	}
	d.contents[key] = value
}

func (d *dict[K, V]) Delete(key K) bool {
	if _, exists := d.contents[key]; !exists {
		return false
	}

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
	return true
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