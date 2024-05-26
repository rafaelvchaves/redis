package resp

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/optional"
)

type Command any

type Ping struct {
	Message optional.Value[BulkString]
}

type Echo struct {
	Message BulkString
}

type Set struct {
	Key   BulkString
	Value Value
	TTL   optional.Value[time.Time]
}

type Get struct {
	Key BulkString
}

type Info struct {
	Section BulkString
}

type ReplConfig struct {
	Key   BulkString
	Value BulkString
}

type PSync struct {
	ReplicationID     optional.Value[BulkString]
	ReplicationOffset optional.Value[BulkString]
}
