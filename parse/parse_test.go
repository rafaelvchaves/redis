package parse_test

import (
	"bytes"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/parse"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func TestParse(t *testing.T) {
	tests := map[string]struct {
		input resp.Array
		want  resp.Command
	}{
		"ECHO banana": {
			input: resp.Array{
				resp.BulkString("ECHO"),
				resp.BulkString("banana"),
			},
			want: resp.Echo{Message: resp.BulkString("banana")},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			if _, err := buffer.Write(test.input.Serialize()); err != nil {
				t.Fatalf("buffer.Write: %v", err)
			}
			parser := parse.NewParser(&buffer)
			gotCommand, err := parser.ParseCommand()
			if err != nil {
				t.Fatalf("ParseCommand: %v", err)
			}
			if gotCommand != test.want {
				t.Errorf("Command = %v, want %v", gotCommand, test.want)
			}
		})
	}
}
