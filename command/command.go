package command

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Command interface {
	Details() Details
}

type Details struct {
	RequiresReplicaResponse bool
	PropagateToReplica      bool
}

type defaultDetailsImpl struct{}

func (d defaultDetailsImpl) Details() Details {
	return Details{}
}

type Ping struct {
	defaultDetailsImpl
	Message optional.Value[resp.BulkString]
}

type Echo struct {
	defaultDetailsImpl
	Message resp.BulkString
}

type Set struct {
	Key   resp.BulkString
	Value resp.Value
	TTL   optional.Value[time.Time]
}

func (s Set) Details() Details {
	return Details{
		PropagateToReplica: true,
	}
}

type Get struct {
	defaultDetailsImpl
	Key resp.BulkString
}

type Info struct {
	defaultDetailsImpl
	Section resp.BulkString
}

type ReplConfig struct {
	Key   resp.BulkString
	Value resp.BulkString
}

func (r ReplConfig) Details() Details {
	var result Details
	if r.Key == "GETACK" {
		result.RequiresReplicaResponse = true
	}
	return result
}

type PSync struct {
	defaultDetailsImpl
	ReplicationID     optional.Value[resp.BulkString]
	ReplicationOffset optional.Value[resp.BulkString]
}

type Wait struct {
	defaultDetailsImpl
	ReplicaCount int
	Timeout      time.Duration
}
