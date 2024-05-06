package optional

import "fmt"

// Value represents a value that may or may not hold an element of type T.
type Value[T any] struct {
	value   T
	present bool
}

// None returns an option with no value.
func None[T any]() Value[T] {
	return Value[T]{}
}

// Some returns an option populated with the given value.
func Some[T any](value T) Value[T] {
	return Value[T]{
		value:   value,
		present: true,
	}
}

// Get gets the value, along with whether it is present.
func (v Value[T]) Get() (T, bool) {
	return v.value, v.present
}

func (v Value[T]) String() string {
	if !v.present {
		return "None"
	}
	return fmt.Sprintf("Some(%+v)", v.value)
}
