package parse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"time"

	"regexp"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

var (
	arrayRegex      = regexp.MustCompile(`^\*([0-9]+)$`)
	stringRegex     = regexp.MustCompile(`^\+(.*)$`)
	bulkStringRegex = regexp.MustCompile(`^\$([0-9]+)$`)
)

type Parser struct {
	scanner *bufio.Scanner
}

func NewParser(reader io.Reader) Parser {
	return Parser{scanner: bufio.NewScanner(reader)}
}

func Array(value resp.Value) ([]resp.BulkString, error) {
	array, ok := value.(resp.Array)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", value)
	}
	if len(array) == 0 {
		return nil, fmt.Errorf("unexpected empty command")
	}
	result := make([]resp.BulkString, len(array))
	for i, e := range array {
		str, ok := e.(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", array[0])
		}
		result[i] = str
	}
	return result, nil
}

func Command(input resp.Array) (resp.Command, error) {
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
		return resp.Ping{Message: message}, nil
	case "ECHO":
		var message resp.BulkString
		if len(args) >= 2 {
			message = args[1]
		}
		return resp.Echo{Message: message}, nil
	case "GET":
		if len(args) < 2 {
			return nil, fmt.Errorf("expected at least 2 arguments, got %d", len(args))
		}
		return resp.Get{Key: args[1]}, nil
	case "SET":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected at least 3 arguments, got %d", len(args))
		}
		set := resp.Set{Key: args[1], Value: args[2]}
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
		return resp.Info{Section: args[1]}, nil
	case "REPLCONF":
		if len(args) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		return resp.ReplConfig{Key: args[1], Value: args[2]}, nil
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
		return resp.PSync{
			ReplicationID:     id,
			ReplicationOffset: offset,
		}, nil
	}
	return nil, fmt.Errorf("unknown command %s", first)
}

func (p Parser) Parse() (resp.Value, error) {
	p.scanner.Scan()
	text := p.scanner.Text()
	switch {
	case arrayRegex.MatchString(text):
		return p.parseArray()
	case stringRegex.MatchString(text):
		matches := stringRegex.FindStringSubmatch(text)
		return resp.String(matches[1]), nil
	case bulkStringRegex.MatchString(text):
		return p.parseBulkString()
	}
	return nil, fmt.Errorf("no match found: %s", text)
}

func (p Parser) parseArray() (resp.Array, error) {
	matches := arrayRegex.FindStringSubmatch(p.scanner.Text())
	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return resp.Array{}, err
	}
	result := make(resp.Array, length)
	for i := range length {
		element, err := p.Parse()
		if err != nil {
			return resp.Array{}, err
		}
		result[i] = element
	}
	return result, nil
}

func (p Parser) parseBulkString() (resp.BulkString, error) {
	matches := bulkStringRegex.FindStringSubmatch(p.scanner.Text())
	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return "", err
	}
	var result resp.BulkString
	var received int
	for received < length {
		p.scanner.Scan()
		result += resp.BulkString(p.scanner.Text())
		received += len(p.scanner.Text())
	}
	return result, nil
}
