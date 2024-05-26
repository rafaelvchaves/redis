package cache

import "sync"

type TypedSyncMap[K any, V any] struct {
	m *sync.Map
}

func NewTypedSyncMap[K any, V any]() *TypedSyncMap[K, V] {
	return &TypedSyncMap[K, V]{
		m: &sync.Map{},
	}
}

func (m *TypedSyncMap[K, V]) Get(key K) (V, bool) {
	var zero V
	value, ok := m.m.Load(key)
	if !ok {
		return zero, false
	}
	return value.(V), true
}

func (m *TypedSyncMap[K, V]) Put(key K, value V) {
	m.m.Store(key, value)
}

func (m *TypedSyncMap[K, V]) Range(f func(K, V) bool) {
	g := func(a, b any) bool {
		return f(a.(K), b.(V))
	}
	m.m.Range(g)
}
