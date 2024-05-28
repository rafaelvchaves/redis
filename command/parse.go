package command

import (
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/gobwas/glob"
)

func Parse(array resp.Array) (Command, error) {
	if len(array) == 0 {
		return nil, fmt.Errorf("unexpected empty command")
	}
	input := make([]resp.BulkString, len(array))
	for i, e := range array {
		str, ok := e.(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", input[0])
		}
		input[i] = str
	}
	first := strings.ToUpper(string(input[0]))
	switch first {
	case "PING":
		var message optional.Value[resp.BulkString]
		if len(input) >= 2 {
			message = optional.Some(input[1])
		}
		return Ping{Message: message}, nil
	case "ECHO":
		var message resp.BulkString
		if len(input) >= 2 {
			message = input[1]
		}
		return Echo{Message: message}, nil
	case "GET":
		if len(input) < 2 {
			return nil, fmt.Errorf("expected at least 2 arguments, got %d", len(input))
		}
		return Get{Key: input[1]}, nil
	case "SET":
		if len(input) < 3 {
			return nil, fmt.Errorf("expected at least 3 arguments, got %d", len(input))
		}
		set := Set{Key: input[1], Value: input[2]}
		for i := 3; i < len(input); {
			switch opt := strings.ToUpper(string(input[i])); opt {
			case "PX":
				if i+1 >= len(input) {
					return nil, fmt.Errorf("PX must be followed by a value")
				}
				millis, err := strconv.ParseInt(string(input[i+1]), 10, 64)
				if err != nil {
					return nil, err
				}
				set.TTL = optional.Some(time.Now().Add(time.Duration(millis) * time.Millisecond))
				i += 2
			default:
				return nil, fmt.Errorf("unknown option %s", opt)
			}
		}
		return set, nil
	case "INFO":
		if len(input) < 2 {
			return nil, fmt.Errorf("only INFO <section> is supported")
		}
		return Info{Section: input[1]}, nil
	case "REPLCONF":
		if len(input) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		return ReplConfig{Key: input[1], Value: input[2]}, nil
	case "PSYNC":
		if len(input) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		var id optional.Value[resp.BulkString]
		if input[1] != resp.BulkString("?") {
			id = optional.Some(input[1])
		}
		var offset optional.Value[resp.BulkString]
		if input[2] != resp.BulkString("-1") {
			id = optional.Some(input[2])
		}
		return PSync{
			ReplicationID:     id,
			ReplicationOffset: offset,
		}, nil
	case "WAIT":
		if len(input) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		replicaCount, err := strconv.Atoi(string(input[1]))
		if err != nil {
			return nil, err
		}
		millis, err := strconv.ParseInt(string(input[2]), 10, 64)
		if err != nil {
			return nil, err
		}
		return Wait{ReplicaCount: replicaCount, Timeout: time.Duration(millis) * time.Millisecond}, nil
	case "CONFIG":
		if len(input) < 2 {
			return nil, fmt.Errorf("expected at least 1 argument")
		}
		switch strings.ToUpper(string(input[1])) {
		case "GET":
			n := len(input[2:])
			if n == 0 {
				return nil, fmt.Errorf("no keys specified")
			}
			return ConfigGet{Keys: input[2:]}, nil
		case "SET":
			args := input[2:]
			n := len(args)
			if n == 0 {
				return nil, fmt.Errorf("no key-value pairs specified")
			}
			if n%2 != 0 {
				return nil, fmt.Errorf("unmatched parameter %s", args[n-1])
			}
			pairs := make(map[resp.BulkString]resp.BulkString)
			for i := 0; i < n-1; i += 2 {
				key, value := args[i], args[i+1]
				pairs[key] = value
			}
			return ConfigSet{Pairs: pairs}, nil
		}
		return nil, fmt.Errorf("unknown command GET %s", input[1])
	case "KEYS":
		if len(input) < 2 {
			return nil, fmt.Errorf("expected 1 argument")
		}
		pattern, err := glob.Compile(string(input[1]))
		if err != nil {
			return nil, err
		}
		return Keys{Pattern: pattern}, nil
	}
	return nil, fmt.Errorf("unknown command %s", first)
}
