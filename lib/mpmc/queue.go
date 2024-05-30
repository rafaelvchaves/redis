// Package mpmc provides a implementation for a multiple-producer,
// multiple-consumer queue.
package mpmc

import (
	"fmt"
	"sync"
)

type Queue[T any] struct {
	mu        sync.RWMutex
	consumers map[string]chan<- T
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{consumers: make(map[string]chan<- T)}
}

func (q *Queue[T]) AddConsumer(id string, c chan<- T) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.consumers[id]; ok {
		return fmt.Errorf("consumer already exists")
	}
	q.consumers[id] = c
	return nil
}

func (q *Queue[T]) RemoveConsumer(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.consumers, id)
}

func (q *Queue[T]) Broadcast(value T) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, c := range q.consumers {
		select {
		case c <- value:
		default:
			// Avoid blocking if channel is full.
		}

	}
}
