package command

import (
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func Parse(input resp.Array) (Command, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("unexpected empty command")
	}
	args := make([]resp.BulkString, len(input))
	for i, e := range input {
		str, ok := e.(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", args[0])
		}
		args[i] = str
	}
	first := strings.ToUpper(string(args[0]))
	switch first {
	case "PING":
		var message optional.Value[resp.BulkString]
		if len(args) >= 2 {
			message = optional.Some(args[1])
		}
		return Ping{Message: message}, nil
	case "ECHO":
		var message resp.BulkString
		if len(args) >= 2 {
			message = args[1]
		}
		return Echo{Message: message}, nil
	case "GET":
		if len(args) < 2 {
			return nil, fmt.Errorf("expected at least 2 arguments, got %d", len(args))
		}
		return Get{Key: args[1]}, nil
	case "SET":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected at least 3 arguments, got %d", len(args))
		}
		set := Set{Key: args[1], Value: args[2]}
		for i := 3; i < len(args); {
			switch opt := strings.ToUpper(string(args[i])); opt {
			case "PX":
				if i+1 >= len(args) {
					return nil, fmt.Errorf("PX must be followed by a value")
				}
				millis, err := strconv.ParseInt(string(args[i+1]), 10, 64)
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
		if len(args) < 2 {
			return nil, fmt.Errorf("only INFO <section> is supported")
		}
		return Info{Section: args[1]}, nil
	case "REPLCONF":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		return ReplConfig{Key: args[1], Value: args[2]}, nil
	case "PSYNC":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		var id optional.Value[resp.BulkString]
		if args[1] != resp.BulkString("?") {
			id = optional.Some(args[1])
		}
		var offset optional.Value[resp.BulkString]
		if args[2] != resp.BulkString("-1") {
			id = optional.Some(args[2])
		}
		return PSync{
			ReplicationID:     id,
			ReplicationOffset: offset,
		}, nil
	case "WAIT":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		replicaCount, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
		millis, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return nil, err
		}
		return Wait{ReplicaCount: replicaCount, Timeout: time.Duration(millis) * time.Millisecond}, nil
	}
	return nil, fmt.Errorf("unknown command %s", first)
}
