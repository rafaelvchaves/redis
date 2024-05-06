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

func (p Parser) ParseCommand() (resp.Command, error) {
	t, err := p.Parse()
	if err != nil {
		return nil, err
	}
	array, ok := t.(resp.Array)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", t)
	}
	if len(array) == 0 {
		return nil, fmt.Errorf("unexpected empty command")
	}
	inputs := make([]resp.BulkString, len(array))
	for i, e := range array {
		str, ok := e.(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", array[0])
		}
		inputs[i] = str
	}
	first := strings.ToUpper(string(inputs[0]))
	switch {
	case first == "PING":
		var message optional.Value[resp.BulkString]
		if len(inputs) >= 2 {
			message = optional.Some(inputs[1])
		}
		return resp.Ping{Message: message}, nil
	case first == "ECHO":
		var message resp.BulkString
		if len(inputs) >= 2 {
			message = inputs[1]
		}
		return resp.Echo{Message: message}, nil
	case first == "GET":
		if len(inputs) < 2 {
			return nil, fmt.Errorf("expected at least 2 arguments, got %d", len(inputs))
		}
		return resp.Get{Key: inputs[1]}, nil
	case first == "SET":
		if len(inputs) < 3 {
			return nil, fmt.Errorf("expected at least 3 arguments, got %d", len(inputs))
		}
		set := resp.Set{Key: inputs[1], Value: inputs[2]}
		for i := 3; i < len(inputs); {
			switch opt := strings.ToUpper(string(inputs[i])); opt {
			case "PX":
				if i+1 >= len(inputs) {
					return nil, fmt.Errorf("PX must be followed by a value")
				}
				millis, err := strconv.ParseInt(string(inputs[i+1]), 10, 64)
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
	case first == "INFO":
		if len(inputs) < 2 {
			return nil, fmt.Errorf("only INFO <section> is supported")
		}
		return resp.Info{Section: inputs[1]}, nil
	case first == "REPLCONF":
		if len(inputs) < 3 {
			return nil, fmt.Errorf("expected 2 arguments")
		}
		return resp.ReplConfig{Key: inputs[1], Value: inputs[2]}, nil
	}
	return nil, fmt.Errorf("unknown command %s", first)
}

func (p Parser) Parse() (resp.Value, error) {
	p.scanner.Scan()
	switch {
	case arrayRegex.MatchString(p.scanner.Text()):
		return p.parseArray()
	case stringRegex.MatchString(p.scanner.Text()):
		matches := stringRegex.FindStringSubmatch(p.scanner.Text())
		return resp.String(matches[1]), nil
	case bulkStringRegex.MatchString(p.scanner.Text()):
		return p.parseBulkString()
	}
	return nil, fmt.Errorf("no match found: %s", p.scanner.Text())
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
