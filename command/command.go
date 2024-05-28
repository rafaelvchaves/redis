package command

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/gobwas/glob"
)

type Command interface {
	Details() Details
}

type Details struct {
	RequiresReplicaResponse bool
	PropagateToReplica      bool
}

type defaultDetailsImpl struct{}

func (defaultDetailsImpl) Details() Details {
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

type ConfigGet struct {
	Keys []resp.BulkString
}

func (ConfigGet) Details() Details {
	return Details{}
}

type ConfigSet struct {
	Pairs map[resp.BulkString]resp.BulkString
}

func (ConfigSet) Details() Details {
	return Details{}
}

type Keys struct {
	Pattern glob.Glob
}

func (Keys) Details() Details {
	return Details{}
}

type Type struct {
	defaultDetailsImpl
	Key resp.BulkString
}

type XAdd struct {
	StreamKey      resp.BulkString
	EntryIDPattern resp.BulkString
	Pairs          resp.Array
}

func (XAdd) Details() Details {
	return Details{}
}

type XRange struct {
	StreamKey resp.BulkString
	Start     resp.BulkString
	End       resp.BulkString
}

func (XRange) Details() Details {
	return Details{}
}

type XRead struct {
	Keys   []resp.BulkString
	Values []resp.BulkString
}

func (XRead) Details() Details {
	return Details{}
}
