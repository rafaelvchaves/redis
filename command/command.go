package command

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Command any

type Ping struct {
	Message optional.Value[resp.BulkString]
}

type Echo struct {
	Message resp.BulkString
}

type Set struct {
	Key   resp.BulkString
	Value resp.Value
	TTL   optional.Value[time.Time]
}

type Get struct {
	Key resp.BulkString
}

type Info struct {
	Section resp.BulkString
}

type ReplConfig struct {
	Key   resp.BulkString
	Value resp.BulkString
}

type PSync struct {
	ReplicationID     optional.Value[resp.BulkString]
	ReplicationOffset optional.Value[resp.BulkString]
}
