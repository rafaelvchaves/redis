package cache

type Map[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	Range(func(K, V) bool)
	Len() int
}
