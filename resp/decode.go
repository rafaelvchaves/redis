package resp

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Decoder provides functionality to continuously decode incoming RESP-encoded
// byte streams.
type Decoder struct {
	scanner *bufio.Scanner
}

// NewDecoder provides a decoder that reads bytes from the provided reader.
func NewDecoder(reader io.Reader) Decoder {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanBytes)
	return Decoder{scanner: scanner}
}

var (
	arrayRegex      = regexp.MustCompile(`^\*([0-9]+)$`)
	stringRegex     = regexp.MustCompile(`^\+(.*)$`)
	bulkStringRegex = regexp.MustCompile(`^\$([0-9]+)$`)
	rdbFileRegex    = regexp.MustCompile(`^\$([0-9]+)$`)
)

// DecodeArray attempts to decode a RESP array from the input stream.
func (d Decoder) DecodeArray() (Array, error) {
	prefix := d.readUntil("\r\n")
	return d.decodeArray(prefix)
}

// DecodeRDBFile attempts to decode an RDB file from the input stream.
// Since the prefix for RDB files is the same as the prefix for bulk string,
// the caller is responsible for determining whether a file is being read.
func (d Decoder) DecodeRDBFile() (RDBFile, error) {
	prefix := d.readUntil("\r\n")
	matches := rdbFileRegex.FindStringSubmatch(prefix)
	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return "", err
	}
	result := d.readN(length)
	return RDBFile(result), nil
}

// Decode reads the next RESP value from the input stream. If the input is
// malformed, it returns an error.
func (d Decoder) Decode() (Value, error) {
	prefix := d.readUntil("\r\n")
	switch {
	case arrayRegex.MatchString(prefix):
		return d.decodeArray(prefix)
	case stringRegex.MatchString(prefix):
		return String(prefix[1:]), nil
	case bulkStringRegex.MatchString(prefix):
		return d.decodeBulkString(prefix)
	}
	return nil, fmt.Errorf("no match found: %v", []byte(prefix))
}

func (d Decoder) decodeArray(prefix string) (Array, error) {
	matches := arrayRegex.FindStringSubmatch(prefix)
	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return Array{}, err
	}
	result := make(Array, length)
	for i := range length {
		element, err := d.Decode()
		if err != nil {
			return Array{}, err
		}
		result[i] = element
	}
	return result, nil
}

func (d Decoder) decodeBulkString(prefix string) (BulkString, error) {
	matches := bulkStringRegex.FindStringSubmatch(prefix)
	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return "", err
	}
	result := d.readN(length)
	d.readUntil("\r\n")
	return BulkString(result), nil
}

// readUntil reads until [s], returning the bytes read up to but not including
// [s].
func (d Decoder) readUntil(s string) string {
	var result string
	for {
		d.scanner.Scan()
		result += d.scanner.Text()
		if strings.HasSuffix(result, s) {
			return result[:len(result)-len(s)]
		}
	}
}

// readN reads the next n bytes.
func (d Decoder) readN(n int) string {
	var bytes []byte
	i := 0
	for i < n {
		d.scanner.Scan()
		bytes = append(bytes, d.scanner.Bytes()...)
		i++
	}
	return string(bytes)
}
