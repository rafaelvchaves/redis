package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/parse"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

var port = flag.Int("port", 6379, "Port to run Redis server on")

func main() {
	flag.Parse()
	host := "0.0.0.0"
	address := net.JoinHostPort(host, strconv.Itoa(*port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port", *port)
		os.Exit(1)
	}
	fmt.Printf("Listening at address %s...\n", address)
	defer listener.Close()
	server := &server{cache: &sync.Map{}}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Accepted connection with %s\n", conn.RemoteAddr())
		go server.handleConnection(conn)
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()
	parser := parse.NewParser(conn)
	for {
		cmd, err := parser.ParseCommand()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Serialize())
			continue
		}
		fmt.Printf("Received command %T%+v\n", cmd, cmd)
		conn.Write(s.execute(cmd).Serialize())
	}
}

type server struct {
	cache *sync.Map
}

type entry struct {
	value resp.Value
	ttl   optional.Value[time.Time]
}

func (s *server) execute(cmd resp.Command) resp.Value {
	switch command := cmd.(type) {
	case resp.Ping:
		if msg, ok := command.Message.Get(); ok {
			return msg
		}
		return resp.String("PONG")
	case resp.Echo:
		return command.Message
	case resp.Set:
		s.cache.Store(command.Key, entry{
			value: command.Value,
			ttl:   command.TTL,
		})
		return resp.String("OK")
	case resp.Get:
		value, ok := s.cache.Load(command.Key)
		if !ok {
			return resp.NullBulkString{}
		}
		entry := value.(entry)
		if ttl, ok := entry.ttl.Get(); ok && time.Now().After(ttl) {
			return resp.NullBulkString{}
		}
		return entry.value
	}
	return resp.SimpleError{Message: "unknown command"}
}
