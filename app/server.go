package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/parse"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func main() {
	address := "0.0.0.0:6379"
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
		cmd, args, err := parser.ParseCommand()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Serialize())
			continue
		}
		fmt.Printf("Received command %s, arguments %v\n", cmd, args)
		conn.Write(s.execute(cmd, args...).Serialize())
	}
}

type server struct {
	cache *sync.Map
}

func (s *server) execute(cmd resp.Command, args ...resp.Type) resp.Type {
	switch cmd {
	case resp.Ping:
		if len(args) == 0 {
			return resp.String("PONG")
		}
		return args[0]
	case resp.Echo:
		if len(args) == 0 {
			return resp.String("")
		}
		return args[0]
	case resp.Set:
		if len(args) != 2 {
			return resp.SimpleError{Message: "expected 2 arguments to SET"}
		}
		key, ok := args[0].(resp.BulkString)
		if !ok {
			return resp.SimpleError{Message: "expected string key type"}
		}
		s.cache.Store(key, args[1])
		return resp.String("OK")
	case resp.Get:
		if len(args) != 1 {
			return resp.SimpleError{Message: "expected 1 argument to GET"}
		}
		key, ok := args[0].(resp.BulkString)
		if !ok {
			return resp.SimpleError{Message: "expected string key type"}
		}
		value, ok := s.cache.Load(key)
		if !ok {
			return resp.Null{}
		}
		return value.(resp.Type)
	}
	return resp.SimpleError{Message: "unknown command " + string(cmd)}
}
