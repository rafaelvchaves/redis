package redis

import (
	"fmt"
	"reflect"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Config struct {
	Replication Replication
}

// ToBulkString encodes the configuration as a RESP bulk string.
// The first argument is the section (if any) of the config to return.
// A value of "all" denotes that the entire config should be encoded.
func (c Config) ToBulkString(sections ...string) resp.BulkString {
	structsToEncode := map[string]any{}
	for _, section := range sections {
		switch section {
		case "replication":
			structsToEncode[section] = c.Replication
		default:
			structsToEncode[section] = struct{}{}
		}
	}
	var str string
	for section, config := range structsToEncode {
		t := reflect.TypeOf(config)
		str += "# " + section + "\n"
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			value := reflect.ValueOf(config).Field(i)
			tag := field.Tag.Get("json")
			if tag != "" {
				str += tag + ":" + fmt.Sprint(value.Interface()) + "\n"
			}
		}
	}
	return resp.BulkString(str)
}

type Replication struct {
	Role                    Role   `json:"role"`
	MasterReplicationID     string `json:"master_replid"`
	MasterReplicationOffset int    `json:"master_repl_offset"`
	MasterHost              string `json:"master_host"`
	MasterPort              int    `json:"master_port"`
}
