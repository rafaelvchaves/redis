package resp

import (
	"strconv"
)

type Type interface {
	Serialize() []byte
}

type Array []Type

func (a Array) Serialize() []byte {
	result := append([]byte{'*'}, []byte(strconv.Itoa(len(a)))...)
	result = append(result, '\r', '\n')
	for _, el := range a {
		result = append(result, el.Serialize()...)
	}
	return result
}

type String string

func (s String) Serialize() []byte {
	result := append([]byte{'+'}, []byte(s)...)
	result = append(result, '\r', '\n')
	return result
}

type BulkString string

func (b BulkString) Serialize() []byte {
	result := append([]byte{'$'}, []byte(strconv.Itoa(len(b)))...)
	result = append(result, '\r', '\n')
	result = append(result, []byte(b)...)
	result = append(result, '\r', '\n')
	return result
}

type SimpleError struct {
	Kind    string
	Message string
}

func (s SimpleError) Serialize() []byte {
	kind := s.Kind
	if kind == "" {
		kind = "ERR"
	}
	result := append([]byte{'-'}, []byte(kind)...)
	result = append(result, ' ')
	result = append(result, []byte(s.Message)...)
	result = append(result, '\r', '\n')
	return result
}

type Null struct{}

func (n Null) Serialize() []byte {
	return []byte{'_', '\r', '\n'}
}
