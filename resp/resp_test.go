package resp_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/google/go-cmp/cmp"
)

func TestEncode(t *testing.T) {
	tests := map[string]struct {
		input resp.Value
		want  []byte
	}{
		"empty simple string": {
			input: resp.String(""),
			want:  []byte("+\r\n"),
		},
		"simple string": {
			input: resp.String("OK"),
			want:  []byte("+OK\r\n"),
		},
		"empty bulk string": {
			input: resp.BulkString(""),
			want:  []byte("$0\r\n\r\n"),
		},
		"bulk string": {
			input: resp.BulkString("hello"),
			want:  []byte("$5\r\nhello\r\n"),
		},
		"bulk string with newline and carriage return": {
			input: resp.BulkString("hello\r\n"),
			want:  []byte("$7\r\nhello\r\n\r\n"),
		},
		"empty array": {
			input: resp.Array{},
			want:  []byte("*0\r\n"),
		},
		"simple string array": {
			input: resp.Array{
				resp.String("OK"),
				resp.String("OK"),
			},
			want: []byte("*2\r\n+OK\r\n+OK\r\n"),
		},
		"bulk string array": {
			input: resp.Array{
				resp.BulkString("hello"),
				resp.BulkString("world"),
			},
			want: []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
		},
		"simple error": {
			input: resp.SimpleError{Message: "abcd"},
			want:  []byte("-ERR abcd\r\n"),
		},
		"simple error with prefix": {
			input: resp.SimpleError{Kind: "WRONGTYPE", Message: "abcd"},
			want:  []byte("-WRONGTYPE abcd\r\n"),
		},
		"null": {
			input: resp.Null{},
			want:  []byte("_\r\n"),
		},
		"RDB file": {
			input: resp.RDBFile("UkVESVMwMDEx+gly"),
			want:  []byte("$16\r\nUkVESVMwMDEx+gly"),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.input.Encode()
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Serialize (-want +got):%s\n", diff)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	tests := map[string]struct {
		input string
		want  resp.Value
	}{
		"simple string": {
			input: "+OK\r\n",
			want:  resp.String("OK"),
		},
		"bulk string": {
			input: "$5\r\nRedis\r\n",
			want:  resp.BulkString("Redis"),
		},
		"empty array": {
			input: "*0\r\n",
			want:  resp.Array{},
		},
		"array of simple strings": {
			input: "*2\r\n+hello\r\n+world\r\n",
			want:  resp.Array{resp.String("hello"), resp.String("world")},
		},
		"array of bulk strings": {
			input: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			want:  resp.Array{resp.BulkString("hello"), resp.BulkString("world")},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			if _, err := buffer.Write([]byte(test.input)); err != nil {
				t.Fatalf("buffer.Write: %v", err)
			}
			decoder := resp.NewDecoder(&buffer)
			got, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if diff := cmp.Diff(test.want, got, cmpOpts...); diff != "" {
				t.Errorf("Decode(%v) diff (-want +got):%s\n", []byte(test.input), diff)
			}
		})
	}
}

var cmpOpts = []cmp.Option{
	cmp.Comparer(func(a, b resp.Array) bool {
		if len(a) != len(b) {
			return false
		}
		sort.Slice(a, func(i, j int) bool {
			return less(a[i], a[j])
		})
		sort.Slice(b, func(i, j int) bool {
			return less(b[i], b[j])
		})
		n := len(a)
		for i := range n {
			if !compare(a[i], b[i]) {
				return false
			}
		}
		return true
	}),
}

func compare(a, b resp.Value) bool {
	return string(a.Encode()) == string(b.Encode())
}

func less(a, b resp.Value) bool {
	return string(a.Encode()) < string(b.Encode())
}
