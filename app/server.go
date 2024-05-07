package main

import (
	"encoding/hex"
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
	"github.com/codecrafters-io/redis-starter-go/random"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

var port = flag.Int("port", 6379, "Port to run Redis server on")
var masterHost = flag.String("replicaof", "", "Master host to replicate")
var masterPort string

func parseArgs() {
	flag.Parse()
	for i, arg := range os.Args {
		if arg == "--replicaof" {
			if i+2 >= len(os.Args) {
				fmt.Println("host and port must be specified after --replicaof")
				os.Exit(1)
			}
			masterPort = os.Args[i+2]
		}
	}
}

func main() {
	parseArgs()
	role := "master"
	if *masterHost != "" && masterPort != "" {
		role = "slave"
	}
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
		host:  host,
		port:  *port,
		cache: &sync.Map{},
		info: info{
			replication: replication{
				Role:                role,
				MasterReplicationID: random.AlphaNumeric(40),
			},
		},
	}
	if role == "slave" {
		go server.connectToMaster(*masterHost, masterPort)
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

func (s *server) connectToMaster(host string, port string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return
	}
	defer conn.Close()
	parser := parse.NewParser(conn)
	messages := []resp.Array{
		{
			resp.BulkString("PING"),
		},
		{
			resp.BulkString("REPLCONF"),
			resp.BulkString("listening-port"),
			resp.BulkString(strconv.Itoa(s.port)),
		},
		{
			resp.BulkString("REPLCONF"),
			resp.BulkString("capa"),
			resp.BulkString("psync2"),
		},
		{
			resp.BulkString("PSYNC"),
			resp.BulkString("?"),
			resp.BulkString("-1"),
		},
	}
	conn.Write(messages[0].Serialize())
	resp0, err := parser.Parse()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("got ", resp0)
	conn.Write(messages[1].Serialize())
	resp1, err := parser.Parse()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("got ", resp1)
	conn.Write(messages[2].Serialize())
	resp2, err := parser.Parse()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("got ", resp2)
	conn.Write(messages[3].Serialize())
	resp3, err := parser.Parse()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("got ", resp3)

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
		for _, message := range s.execute(cmd) {
			conn.Write(message.Serialize())
		}
	}
}

func (s *server) state() (string, error) {
	state, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return "", err
	}
	return string(state), nil
}

type info struct {
	replication replication
}

type replication struct {
	Role                    string `json:"role"`
	MasterReplicationID     string `json:"master_replid"`
	MasterReplicationOffset int    `json:"master_repl_offset"`
}

type server struct {
	host  string
	port  int
	cache *sync.Map
	info  info
}

type entry struct {
	value resp.Value
	ttl   optional.Value[time.Time]
}

func (s *server) execute(cmd resp.Command) []resp.Value {
	switch command := cmd.(type) {
	case resp.Ping:
		if msg, ok := command.Message.Get(); ok {
			return []resp.Value{msg}
		}
		return []resp.Value{resp.String("PONG")}
	case resp.Echo:
		return []resp.Value{command.Message}
	case resp.Set:
		s.cache.Store(command.Key, entry{
			value: command.Value,
			ttl:   command.TTL,
		})
		return []resp.Value{resp.String("OK")}
	case resp.Get:
		value, ok := s.cache.Load(command.Key)
		if !ok {
			return []resp.Value{resp.NullBulkString{}}
		}
		entry := value.(entry)
		if ttl, ok := entry.ttl.Get(); ok && time.Now().After(ttl) {
			return []resp.Value{resp.NullBulkString{}}
		}
		return []resp.Value{entry.value}
	case resp.Info:
		switch command.Section {
		case "replication":
			return []resp.Value{toBulkString("# Replication", s.info.replication)}
		default:
			return []resp.Value{resp.SimpleError{Message: "unknown section"}}
		}
	case resp.ReplConfig:
		return []resp.Value{resp.String("OK")}
	case resp.PSync:
		resync := fmt.Sprintf("FULLRESYNC %s %d", s.info.replication.Role, s.info.replication.MasterReplicationOffset)
		state, err := s.state()
		if err != nil {
			return []resp.Value{resp.SimpleError{Message: "failed to encode current state"}}
		}
		return []resp.Value{
			resp.String(resync),
			resp.RDBFile(state),
		}
	}
	return []resp.Value{resp.SimpleError{Message: "unknown command"}}
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
		if tag != "" {
			str += tag + ":" + fmt.Sprint(value.Interface()) + "\n"
		}
	}
	return resp.BulkString(str)
}
