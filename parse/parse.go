package parse

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"regexp"
	"strconv"

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

func (p Parser) ParseCommand() (resp.Command, []resp.Type, error) {
	t, err := p.parse()
	if err != nil {
		return "", nil, err
	}
	array, ok := t.(resp.Array)
	if !ok {
		return "", nil, nil
	}
	if len(array) == 0 {
		return "", nil, fmt.Errorf("unexpected empty command")
	}
	first, ok := array[0].(resp.BulkString)
	if !ok {
		return "", nil, fmt.Errorf("unexpected type: %T", array[0])
	}
	cmd := strings.ToUpper(string(first))
	switch {
	case cmd == "PING":
		return resp.Ping, array[1:], nil
	case cmd == "ECHO":
		return resp.Echo, array[1:], nil
	}
	return "", nil, fmt.Errorf("unknown command %s", cmd)
}

func (p Parser) parse() (resp.Type, error) {
	p.scanner.Scan()
	switch {
	case arrayRegex.MatchString(p.scanner.Text()):
		return p.parseArray()
	case stringRegex.MatchString(p.scanner.Text()):
		matches := arrayRegex.FindStringSubmatch(p.scanner.Text())
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
		element, err := p.parse()
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
