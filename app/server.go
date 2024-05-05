package main

import (
	"fmt"
	"net"
	"os"

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
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Accepted connection with %s\n", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	parser := parse.NewParser(conn)
	for {
		cmd, args, err := parser.ParseCommand()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Serialize())
			continue
		}
		fmt.Printf("Received command %s, arguments %v\n", cmd, args)
		conn.Write(execute(cmd, args...).Serialize())
	}
}

func execute(cmd resp.Command, args ...resp.Type) resp.Type {
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
	}
	return resp.SimpleError{Message: "unknown command " + string(cmd)}
}
