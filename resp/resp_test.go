package resp_test

import (
	"testing"

	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/google/go-cmp/cmp"
)

func TestSerialize(t *testing.T) {
	tests := map[string]struct {
		input resp.Type
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
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.input.Serialize()
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Serialize (-want +got):%s\n", diff)
			}
		})
	}
}
