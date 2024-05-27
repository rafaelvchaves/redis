package main

import (
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/lib/random"
	"github.com/codecrafters-io/redis-starter-go/redis"
)

var (
	dbFileName = flag.String("dbfilename", "dump.rdb", "Name of the RDB file to load")
	dir        = flag.String("dir", "/tmp/redis-files", "Directory where RDB file is stored")
	port       = flag.Int("port", 6379, "Port to run Redis server on")
	replicaOf  = flag.String("replicaof", "", "Master to replicate. Format: \"<HOST> <PORT>\"")
)

func main() {
	flag.Parse()
	host := "0.0.0.0"
	info := redis.Info{
		Replication: redis.Replication{
			Role:                    redis.Master,
			MasterReplicationID:     random.AlphaNumeric(40),
			MasterReplicationOffset: 0,
		},
	}
	if *replicaOf != "" {
		info.Replication.Role = redis.Replica
		masterAddr := strings.Split(*replicaOf, " ")
		if len(masterAddr) != 2 {
			log.Fatalf("--replicaof should be in the format \"<HOST> <PORT>\"")
		}
		host := masterAddr[0]
		port, err := strconv.Atoi(masterAddr[1])
		if err != nil {
			log.Fatalf("failed to parse master port: %v", err)
		}
		info.Replication.MasterHost = host
		info.Replication.MasterPort = port
	}
	server := redis.NewServer(redis.ServerParams{
		Host:       host,
		Port:       *port,
		ServerInfo: info,
		Config: redis.Config{
			"dir":        *dir,
			"dbfilename": *dbFileName,
		},
	})
	server.Start()
}
