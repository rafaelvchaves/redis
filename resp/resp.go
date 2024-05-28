package resp

import (
	"fmt"
	"strconv"
)

type Value interface {
	Encode() []byte
}

type Array []Value

func (a Array) Encode() []byte {
	result := append([]byte{'*'}, []byte(strconv.Itoa(len(a)))...)
	result = append(result, '\r', '\n')
	for _, el := range a {
		result = append(result, el.Encode()...)
	}
	return result
}

type String string

func (s String) Encode() []byte {
	result := append([]byte{'+'}, []byte(s)...)
	result = append(result, '\r', '\n')
	return result
}

type BulkString string

func (b BulkString) Encode() []byte {
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

func (s SimpleError) Encode() []byte {
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

func (n Null) Encode() []byte {
	return []byte{'_', '\r', '\n'}
}

type NullBulkString struct{}

func (n NullBulkString) Encode() []byte {
	return []byte{'$', '-', '1', '\r', '\n'}
}

type RDBFile string

func (r RDBFile) Encode() []byte {
	result := append([]byte{'$'}, []byte(strconv.Itoa(len(r)))...)
	result = append(result, '\r', '\n')
	result = append(result, []byte(r)...)
	return result
}

type Integer int

func (i Integer) Encode() []byte {
	result := []byte{':'}
	result = append(result, []byte(strconv.Itoa(int(i)))...)
	result = append(result, '\r', '\n')
	return result
}

type Stream struct {
	Entries []Entry
}

type Entry struct {
	ID     EntryID
	Values Array
}

type EntryID struct {
	Time           int64
	SequenceNumber int64
}

func (i EntryID) Less(other EntryID) bool {
	return i.Time < other.Time || (i.Time == other.Time && i.SequenceNumber < other.SequenceNumber)
}

func (i EntryID) String() string {
	return fmt.Sprintf("%d-%d", i.Time, i.SequenceNumber)
}

func (s Stream) Encode() []byte {
	return nil
}
