package parse_test

import (
	"bytes"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/parse"
	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParse(t *testing.T) {
	tests := map[string]struct {
		input       resp.Array
		wantCommand resp.Command
		wantArgs    []resp.Type
	}{
		"ECHO banana": {
			input: resp.Array{
				resp.BulkString("ECHO"),
				resp.BulkString("banana"),
			},
			wantCommand: resp.Echo,
			wantArgs:    []resp.Type{resp.BulkString("banana")},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			if _, err := buffer.Write(test.input.Serialize()); err != nil {
				t.Fatalf("buffer.Write: %v", err)
			}
			parser := parse.NewParser(&buffer)
			gotCommand, gotArgs, err := parser.ParseCommand()
			if err != nil {
				t.Fatalf("ParseCommand: %v", err)
			}
			if gotCommand != test.wantCommand {
				t.Errorf("Command = %v, want %v", gotCommand, test.wantCommand)
			}
			opts := []cmp.Option{
				cmpopts.SortSlices(func(a, b resp.Type) bool {
					return string(a.Serialize()) < string(b.Serialize())
				}),
			}
			if diff := cmp.Diff(test.wantArgs, gotArgs, opts...); diff != "" {
				t.Errorf("args (-want +got):%s\n", diff)
			}
		})
	}
}
