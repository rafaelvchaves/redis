package redis

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/codecrafters-io/redis-starter-go/lib/cache"
	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
	lzf "github.com/zhuyie/golzf"
)

const (
	emptyRDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

	// Op Codes
	eof          = 0xff
	selectDB     = 0xfe
	expireTime   = 0xfd
	expireTimeMS = 0xfc
	resizeDB     = 0xfb
	aux          = 0xfa

	// Value types
	stringValue = 0

	// String types
	lengthPrefixed   stringType = 0
	stringEncodedInt stringType = 1
	compressedString stringType = 2
)

func loadRDBFile(dir, file string) ([]byte, error) {
	contents, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return hex.DecodeString(emptyRDB)
		}
		return nil, err
	}
	return contents, nil
}

type stringType int

type state struct {
	version         string
	auxiliaryFields map[resp.BulkString]resp.BulkString
	cache           cache.Map[resp.BulkString, CacheEntry]
}

func dump(_ state) ([]byte, error) {
	return hex.DecodeString(emptyRDB)
}

func load(rdb []byte) (state, error) {
	result := state{
		cache: cache.NewTypedSyncMap[resp.BulkString, CacheEntry](),
	}
	reader := bufio.NewReader(bytes.NewReader(rdb))

	// Read header, which contains "REDIS" magic string and version number.
	version, err := readHeader(reader)
	if err != nil {
		return state{}, err
	}
	result.version = version

	// Read auxilary fields.
	af, err := readAuxiliaryFields(reader)
	if err != nil {
		return state{}, err
	}
	result.auxiliaryFields = af

	next, err := reader.Peek(1)
	if err != nil {
		return state{}, err
	}
	if next[0] == selectDB {
		cache, err := readDB(reader)
		if err != nil {
			return state{}, err
		}
		result.cache = cache
	}
	return result, nil
}

func readDB(reader *bufio.Reader) (*cache.TypedSyncMap[resp.BulkString, CacheEntry], error) {
	result := cache.NewTypedSyncMap[resp.BulkString, CacheEntry]()
	// Skip over DB number to the resizeDB byte. The contents after this
	// should be two integers representing the size of the hash table
	// and the expire hash table.
	if _, err := reader.ReadBytes(resizeDB); err != nil {
		return nil, err
	}

	// Read hash table size.
	tableSize, err := readInt(reader)
	if err != nil {
		return nil, err
	}

	// Read and discard expire table size.
	_, err = readInt(reader)
	if err != nil {
		return nil, err
	}

	// Read key-value pairs.
	for range tableSize {
		key, value, err := readKeyValuePair(reader)
		if err != nil {
			return nil, err
		}
		result.Put(key, value)
	}
	return result, nil
}

func readHeader(reader *bufio.Reader) (string, error) {
	header, err := readN(reader, 9)
	if err != nil {
		return "", err
	}
	if string(header[:5]) != "REDIS" {
		return "", fmt.Errorf("magic string not found")
	}
	return string(header[5:]), nil
}

func readAuxiliaryFields(reader *bufio.Reader) (map[resp.BulkString]resp.BulkString, error) {
	result := make(map[resp.BulkString]resp.BulkString)
	for {
		next, err := reader.Peek(1)
		if err != nil {
			return nil, err
		}
		if next[0] != aux {
			// No more auxiliary fields to read.
			break
		}
		// Consume the aux op code.
		_, err = reader.ReadByte()
		if err != nil {
			return nil, err
		}
		key, err := readString(reader)
		if err != nil {
			return nil, err
		}
		value, err := readValue(reader, stringValue)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func readInt(reader *bufio.Reader) (int64, error) {
	b0, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	msb := b0 >> 0x6
	lsb := b0 & 0x3f
	switch msb {
	case 0:
		// The LSB represent the length.
		return int64(lsb), nil
	case 1:
		// The LSB + next byte (14 bits) represent the length.
		b1, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int64(b1) << 8 & int64(lsb), nil
	case 2:
		// The next 4 bytes represent the length (LSB discarded).
		bytes, err := readN(reader, 4)
		if err != nil {
			return 0, err
		}
		return asLittleEndian(bytes), nil
	case 3:
		// LSB denote the size of the integer.
		switch lsb {
		// Little endian, so b1->b4 are from least to most significant bits
		case 0:
			// A 8 bit integer follows.
			b1, err := reader.ReadByte()
			if err != nil {
				return 0, err
			}
			return int64(b1), nil
		case 1:
			// A 16 bit integer follows.
			bytes, err := readN(reader, 2)
			if err != nil {
				return 0, err
			}
			return asLittleEndian(bytes), nil
		case 2:
			// A 32 bit integer follows.
			bytes, err := readN(reader, 4)
			if err != nil {
				return 0, err
			}
			return asLittleEndian(bytes), nil
		case 3:
			// For compressed strings, read another int from the stream. This
			// represents the length of the compressed string.
			return readInt(reader)
		default:
			return 0, fmt.Errorf("invalid lsb in length encoding: %d", lsb)
		}
	default:
		return 0, fmt.Errorf("not supported: %d", msb)
	}
}

func readKeyValuePair(reader *bufio.Reader) (resp.BulkString, CacheEntry, error) {
	var entry CacheEntry
	// The first byte is either a flag denoting the key expiry timestamp, or
	// the value type if there is no expiry.
	next, err := reader.Peek(1)
	if err != nil {
		return "", entry, err
	}
	if next[0] == expireTime || next[0] == expireTimeMS {
		ttl, err := readTimestamp(reader)
		if err != nil {
			return "", entry, err
		}
		entry.ttl = optional.Some(ttl)
	}
	valueType, err := reader.ReadByte()
	if err != nil {
		return "", entry, err
	}
	key, err := readString(reader)
	if err != nil {
		return "", entry, err
	}
	value, err := readValue(reader, valueType)
	if err != nil {
		return "", entry, err
	}
	entry.value = value
	return key, entry, nil
}

func stringTypeFromEncoding(lengthEncoding byte) stringType {
	msb := lengthEncoding >> 0x6
	lsb := lengthEncoding & 0x3f
	switch msb {
	case 0, 1, 2:
		return stringValue
	default:
		switch lsb {
		case 0, 1, 2:
			return stringEncodedInt
		default:
			return compressedString
		}
	}
}

func readString(reader *bufio.Reader) (resp.BulkString, error) {
	length, err := readInt(reader)
	if err != nil {
		return "", err
	}
	result := make([]byte, length)
	for i := range length {
		c, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		result[i] = c
	}
	return resp.BulkString(result), nil
}

func readN(reader *bufio.Reader, n int) ([]byte, error) {
	result := make([]byte, n)
	for i := range n {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		result[i] = b
	}
	return result, nil
}

func readValue(reader *bufio.Reader, valueType byte) (resp.BulkString, error) {
	switch valueType {
	case stringValue:
		return readStringValue(reader)
	}
	return "", fmt.Errorf("unsupported value type: %d", valueType)
}

func readStringValue(reader *bufio.Reader) (resp.BulkString, error) {
	next, err := reader.Peek(1)
	if err != nil {
		return "", err
	}
	length := next[0]
	switch t := stringTypeFromEncoding(length); t {
	case stringValue:
		return readString(reader)
	case stringEncodedInt:
		i, err := readInt(reader)
		if err != nil {
			return "", err
		}
		return resp.BulkString(fmt.Sprint(i)), nil
	case compressedString:
		compressedSize, err := readInt(reader)
		if err != nil {
			return "", err
		}
		uncompressedSize, err := readInt(reader)
		if err != nil {
			return "", err
		}
		bytes, err := readN(reader, int(compressedSize))
		if err != nil {
			return "", err
		}
		output := make([]byte, uncompressedSize)
		if _, err := lzf.Decompress(bytes, output); err != nil {
			return "", err
		}
		return resp.BulkString(output), nil

	default:
		return "", fmt.Errorf("unsupported string type: %d", t)
	}
}

func readTimestamp(reader *bufio.Reader) (time.Time, error) {
	flag, err := reader.ReadByte()
	if err != nil {
		return time.Time{}, err
	}
	switch flag {
	case expireTime:
		// Next 4 bytes represent Unix timestamp as an unsigned integer.
		bytes, err := readN(reader, 4)
		if err != nil {
			return time.Time{}, err
		}
		seconds := asLittleEndian(bytes)
		return time.Unix(seconds, 0), nil

	case expireTimeMS:
		// Next 8 bytes represent Unix timestamp as an unsigned long.
		bytes, err := readN(reader, 8)
		if err != nil {
			return time.Time{}, err
		}
		millis := asLittleEndian(bytes)
		return time.Unix(millis/1000, 0), nil
	}
	return time.Time{}, fmt.Errorf("failed precondition: expected timestamp flag")
}

func asLittleEndian(bytes []byte) int64 {
	var result int64
	for i, b := range bytes {
		result |= int64(b) << (i * 8)
	}
	return result
}
