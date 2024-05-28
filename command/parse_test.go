package command_test

import (
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/command"
	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/gobwas/glob"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
		"CONFIG SET key1 value1 key2 value2": {
			input: resp.Array{
				resp.BulkString("CONFIG"),
				resp.BulkString("SET"),
				resp.BulkString("key1"),
				resp.BulkString("value1"),
				resp.BulkString("key2"),
				resp.BulkString("value2"),
			},
			want: command.ConfigSet{Pairs: map[resp.BulkString]resp.BulkString{
				"key1": "value1",
				"key2": "value2",
			}},
		},
		"CONFIG GET key1 key2": {
			input: resp.Array{
				resp.BulkString("CONFIG"),
				resp.BulkString("GET"),
				resp.BulkString("key1"),
				resp.BulkString("key2"),
			},
			want: command.ConfigGet{Keys: []resp.BulkString{"key1", "key2"}},
		},
		"KEYS *": {
			input: resp.Array{
				resp.BulkString("KEYS"),
				resp.BulkString("*"),
			},
			want: command.Keys{Pattern: glob.MustCompile("*")},
		},
		"TYPE foo": {
			input: resp.Array{
				resp.BulkString("TYPE"),
				resp.BulkString("foo"),
			},
			want: command.Type{Key: resp.BulkString("foo")},
		},
		"XADD stream_key 1526919030474-0 temperature 36 humidity 95": {
			input: resp.Array{
				resp.BulkString("XADD"),
				resp.BulkString("stream_key"),
				resp.BulkString("1526919030474-0"),
				resp.BulkString("temperature"),
				resp.BulkString("36"),
				resp.BulkString("humidity"),
				resp.BulkString("95"),
			},
			want: command.XAdd{
				StreamKey:      resp.BulkString("stream_key"),
				EntryIDPattern: resp.BulkString("1526919030474-0"),
				Pairs: resp.Array{
					resp.BulkString("temperature"),
					resp.BulkString("36"),
					resp.BulkString("humidity"),
					resp.BulkString("95"),
				},
			},
		},
		"XRANGE some_key 12345 67890": {
			input: resp.Array{
				resp.BulkString("XRANGE"),
				resp.BulkString("some_key"),
				resp.BulkString("12345"),
				resp.BulkString("67890"),
			},
			want: command.XRange{
				StreamKey: resp.BulkString("some_key"),
				Start:     resp.BulkString("12345"),
				End:       resp.BulkString("67890"),
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := command.Parse(test.input)
			if err != nil {
				t.Fatalf("command.Parse(%v): %v", test.input, err)
			}
			if diff := cmp.Diff(test.want, got, cmpOpts...); diff != "" {
				t.Errorf("command.Parse(%v) diff (-want +got):%s\n", test.input, diff)
			}
		})
	}
}

var cmpOpts = []cmp.Option{
	compareMaps[resp.BulkString, resp.BulkString](),
	compareSlices[resp.BulkString](),
	cmpopts.EquateComparable(
		command.Ping{},
		command.Echo{},
		command.Get{},
		command.Info{},
		command.PSync{},
		command.Wait{},
		command.Type{},
	),
	compareOptions[time.Time](cmp.Comparer((time.Time).Equal)),
}

func compareMaps[K comparable, V comparable]() cmp.Option {
	return cmp.Comparer(func(m1, m2 map[K]V) bool {
		if len(m1) != len(m2) {
			return false
		}
		for key, val1 := range m1 {
			val2, ok := m2[key]
			if !ok {
				return false
			}
			if val1 != val2 {
				return false
			}
		}
		return true
	})
}

func compareSlices[E comparable]() cmp.Option {
	return cmp.Comparer(func(s1, s2 []E) bool {
		if len(s1) != len(s2) {
			return false
		}
		s2Set := make(map[E]bool)
		for _, v := range s2 {
			s2Set[v] = true
		}
		for _, v := range s1 {
			if !s2Set[v] {
				return false
			}
		}
		return true
	})
}

func compareOptions[T any](opts ...cmp.Option) cmp.Option {
	return cmp.Comparer(func(o1, o2 optional.Value[T]) bool {
		v1, ok1 := o1.Get()
		v2, ok2 := o2.Get()
		if !ok1 && !ok2 {
			return true
		}
		return cmp.Equal(v1, v2, opts...) && ok1 && ok2
	})
}
