package command_test

import (
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/command"
	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func TestParseCommand(t *testing.T) {
	tests := map[string]struct {
		input resp.Array
		want  command.Command
	}{
		"PING": {
			input: resp.Array{
				resp.BulkString("PING"),
			},
			want: command.Ping{},
		},
		"PING banana": {
			input: resp.Array{
				resp.BulkString("PING"),
				resp.BulkString("banana"),
			},
			want: command.Ping{Message: optional.Some(resp.BulkString("banana"))},
		},
		"ECHO banana": {
			input: resp.Array{
				resp.BulkString("ECHO"),
				resp.BulkString("banana"),
			},
			want: command.Echo{Message: resp.BulkString("banana")},
		},
		"SET foo 1": {
			input: resp.Array{
				resp.BulkString("SET"),
				resp.BulkString("foo"),
				resp.BulkString("1"),
			},
			want: command.Set{Key: resp.BulkString("foo"), Value: resp.BulkString("1")},
		},
		"GET foo": {
			input: resp.Array{
				resp.BulkString("GET"),
				resp.BulkString("foo"),
			},
			want: command.Get{Key: resp.BulkString("foo")},
		},
		"INFO replication": {
			input: resp.Array{
				resp.BulkString("INFO"),
				resp.BulkString("replication"),
			},
			want: command.Info{Section: resp.BulkString("replication")},
		},
		"REPLCONF GETACK *": {
			input: resp.Array{
				resp.BulkString("REPLCONF"),
				resp.BulkString("GETACK"),
				resp.BulkString("*"),
			},
			want: command.ReplConfig{Key: "GETACK", Value: "*"},
		},
		"PSYNC ? -1": {
			input: resp.Array{
				resp.BulkString("PSYNC"),
				resp.BulkString("?"),
				resp.BulkString("-1"),
			},
			want: command.PSync{},
		},
		"WAIT 1 6000": {
			input: resp.Array{
				resp.BulkString("WAIT"),
				resp.BulkString("1"),
				resp.BulkString("6000"),
			},
			want: command.Wait{ReplicaCount: 1, Timeout: 6 * time.Second},
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
