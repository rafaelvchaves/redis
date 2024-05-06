package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"reflect"
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
	server := &server{
		cache: &sync.Map{},
		info: info{
			replication: replication{
				role: "master",
			},
		},
	}
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

type info struct {
	replication replication
}

type replication struct {
	role string `json:"role"`
}

type server struct {
	cache *sync.Map
	info  info
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
	case resp.Info:
		switch command.Section {
		case "replication":
			return toBulkString("# Replication", s.info.replication)
		default:
			return resp.SimpleError{Message: "unknown section"}
		}
	}
	return resp.SimpleError{Message: "unknown command"}
}

func toBulkString(name string, section any) resp.BulkString {
	t := reflect.TypeOf(section)
	if t.Kind() != reflect.Struct {
		return ""
	}
	var str string
	str += name + "\n"
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := reflect.ValueOf(section).Field(i)
		tag := field.Tag.Get("json")
		fmt.Println(field, value, tag)
		if tag != "" {
			str += tag + ":" + value.String() + "\n"
		}
	}
	return resp.BulkString(str)
}
