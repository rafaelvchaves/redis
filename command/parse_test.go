package command_test

import (
	"testing"

	"github.com/codecrafters-io/redis-starter-go/command"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func TestParseCommand(t *testing.T) {
	tests := map[string]struct {
		input resp.Array
		want  command.Command
	}{
		"ECHO banana": {
			input: resp.Array{
				resp.BulkString("ECHO"),
				resp.BulkString("banana"),
			},
			want: command.Echo{Message: resp.BulkString("banana")},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := command.Parse(test.input)
			if err != nil {
				t.Fatalf("command.Parse(%v): %v", test.input, err)
			}
			if got != test.want {
				t.Errorf("command.Parse(%v) = %v, want %v", test.input, got, test.want)
			}
		})
	}
}
