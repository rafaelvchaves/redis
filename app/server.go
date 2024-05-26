package main

import (
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/random"
	"github.com/codecrafters-io/redis-starter-go/redis"
)

var port = flag.Int("port", 6379, "Port to run Redis server on")
var replicaOf = flag.String("replicaof", "", "Master to replicate. Format: \"<HOST> <PORT>\"")

func main() {
	flag.Parse()
	host := "0.0.0.0"
	config := redis.Config{
		Replication: redis.Replication{
			Role:                    redis.Master,
			MasterReplicationID:     random.AlphaNumeric(40),
			MasterReplicationOffset: 0,
		},
	}
	if *replicaOf != "" {
		config.Replication.Role = redis.Replica
		masterAddr := strings.Split(*replicaOf, " ")
		if len(masterAddr) != 2 {
			log.Fatalf("--replicaof should be in the format \"<HOST> <PORT>\"")
		}
		host := masterAddr[0]
		port, err := strconv.Atoi(masterAddr[1])
		if err != nil {
			log.Fatalf("failed to parse master port: %v", err)
		}
		config.Replication.MasterHost = host
		config.Replication.MasterPort = port
	}
	server := redis.NewServer(redis.ServerParams{
		Host:   host,
		Port:   *port,
		Config: config,
	})
	server.Start()
}
