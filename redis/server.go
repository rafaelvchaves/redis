package redis

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/cache"
	"github.com/codecrafters-io/redis-starter-go/command"
	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Server struct {
	host     string
	port     int
	cache    cache.Map[resp.BulkString, CacheEntry]
	config   Config
	master   net.Conn
	replicas cache.Map[net.Addr, net.Conn]
}

type CacheEntry struct {
	value resp.Value
	ttl   optional.Value[time.Time]
}

type Role string

const (
	Master  Role = "master"
	Replica Role = "slave"
)

type ServerParams struct {
	Host   string
	Port   int
	Role   Role
	Config Config
}

func NewServer(params ServerParams) *Server {
	return &Server{
		host:     params.Host,
		port:     params.Port,
		config:   params.Config,
		cache:    cache.NewTypedSyncMap[resp.BulkString, CacheEntry](),
		replicas: cache.NewTypedSyncMap[net.Addr, net.Conn](),
	}
}

func (s *Server) Start() {
	if s.config.Replication.Role == Replica {
		s.master = s.connectToMaster()
		go s.handleMasterConnection(s.master)
	}
	address := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port", s.port)
		os.Exit(1)
	}
	fmt.Printf("Listening at address %s...\n", address)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		fmt.Printf("Accepted connection with %s\n", conn.RemoteAddr())
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := resp.NewDecoder(conn)
	for {
		req, err := decoder.Decode()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Encode())
			continue
		}
		fmt.Printf("[%s]: received %v\n", s.config.Replication.Role, req)
		for _, message := range s.execute(req, conn) {
			conn.Write(message.Encode())
		}
	}
}

func (s *Server) handleMasterConnection(conn net.Conn) {
	defer conn.Close()
	decoder := resp.NewDecoder(conn)
	for {
		req, err := decoder.Decode()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Encode())
			continue
		}
		fmt.Printf("[%s]: received %v from master\n", s.config.Replication.Role, req)
		s.execute(req, conn)
	}
}

func (s *Server) execute(req resp.Value, conn net.Conn) []resp.Value {
	array, ok := req.(resp.Array)
	if !ok {
		return []resp.Value{resp.SimpleError{Message: "invalid input format"}}
	}
	cmd, err := command.Parse(array)
	if err != nil {
		fmt.Printf("unknown command %v\n", cmd)
		return []resp.Value{resp.SimpleError{Message: err.Error()}}
	}
	switch req := cmd.(type) {
	case command.Ping:
		return s.ping(req)
	case command.Echo:
		return s.echo(req)
	case command.Set:
		response := s.set(req)
		if s.config.Replication.Role == Master {
			s.propagate(array)
		}
		return response
	case command.Get:
		return s.get(req)
	case command.Info:
		return s.info(req)
	case command.ReplConfig:
		return s.replConfig(req)
	case command.PSync:
		return s.psync(req, conn)
	}
	return []resp.Value{resp.SimpleError{Message: "unknown command"}}
}

func (s *Server) ping(req command.Ping) []resp.Value {
	if msg, ok := req.Message.Get(); ok {
		return []resp.Value{msg}
	}
	return []resp.Value{resp.String("PONG")}
}

func (s *Server) echo(req command.Echo) []resp.Value {
	return []resp.Value{req.Message}
}

func (s *Server) set(req command.Set) []resp.Value {
	s.cache.Put(req.Key, CacheEntry{
		value: req.Value,
		ttl:   req.TTL,
	})
	return []resp.Value{resp.String("OK")}
}

func (s *Server) get(req command.Get) []resp.Value {
	entry, ok := s.cache.Get(req.Key)
	if !ok {
		return []resp.Value{resp.NullBulkString{}}
	}
	if ttl, ok := entry.ttl.Get(); ok && time.Now().After(ttl) {
		return []resp.Value{resp.NullBulkString{}}
	}
	return []resp.Value{entry.value}
}

func (s *Server) info(req command.Info) []resp.Value {
	switch req.Section {
	case "replication":
		return []resp.Value{s.config.ToBulkString("replication")}
	default:
		return []resp.Value{resp.SimpleError{Message: "unknown section"}}
	}
}

func (s *Server) replConfig(_ command.ReplConfig) []resp.Value {
	return []resp.Value{resp.String("OK")}
}

func (s *Server) psync(req command.PSync, conn net.Conn) []resp.Value {
	defaultID := resp.BulkString(s.config.Replication.MasterReplicationID)
	defaultOffset := resp.BulkString(strconv.Itoa(s.config.Replication.MasterReplicationOffset))
	id := req.ReplicationID.GetOrDefault(resp.BulkString(defaultID))
	offset := req.ReplicationOffset.GetOrDefault(resp.BulkString(defaultOffset))
	resync := fmt.Sprintf("FULLRESYNC %s %s", id, offset)
	state, err := s.state()
	if err != nil {
		return []resp.Value{resp.SimpleError{Message: "failed to encode current state"}}
	}
	s.replicas.Put(conn.RemoteAddr(), conn)
	return []resp.Value{
		resp.String(resync),
		resp.RDBFile(state),
	}
}

func (s *Server) state() (string, error) {
	state, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return "", err
	}
	return string(state), nil
}

func (s *Server) connectToMaster() net.Conn {
	host := s.config.Replication.MasterHost
	port := strconv.Itoa(s.config.Replication.MasterPort)
	addr := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return nil
	}
	decoder := resp.NewDecoder(conn)
	s.sendToMaster(conn, resp.BulkString("PING"))
	s.awaitResponse(decoder, "PONG")
	s.sendToMaster(conn,
		resp.BulkString("REPLCONF"),
		resp.BulkString("listening-port"),
		resp.BulkString(strconv.Itoa(s.port)),
	)
	s.awaitResponse(decoder, "OK")
	s.sendToMaster(conn,
		resp.BulkString("REPLCONF"),
		resp.BulkString("capa"),
		resp.BulkString("psync2"),
	)
	s.awaitResponse(decoder, "OK")
	s.sendToMaster(conn,
		resp.BulkString("PSYNC"),
		resp.BulkString("?"),
		resp.BulkString("-1"),
	)
	s.awaitSync(decoder)
	return conn
}

func (s *Server) sendToMaster(conn net.Conn, values ...resp.Value) {
	array := resp.Array(values)
	_, err := conn.Write(array.Encode())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func (s *Server) awaitResponse(decoder resp.Decoder, response string) {
	got, err := decoder.Decode()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if str := got.(resp.String); str != resp.String(response) {
		fmt.Printf("expected %v, got %v\n", response, str)
		os.Exit(1)
	}
}

func (s *Server) awaitSync(decoder resp.Decoder) {
	// The master is expected to first respond with a FULLRESYNC message.
	// Currently, the replica does not do anything with this.
	resync, err := decoder.Decode()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if str := resync.(resp.String); !strings.HasPrefix(string(str), "FULLRESYNC") {
		fmt.Printf("expected string with FULLRESYNC prefix, got %v\n", str)
		os.Exit(1)
	}

	// Next, the master sends its contents as a RDB file. The replica
	// would usually replace its state with the contents of the file.
	file, err := decoder.DecodeRDBFile()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("received RDB file:", []byte(file))
}

func (s *Server) propagate(cmd resp.Array) {
	fmt.Printf("propagating %v to replicas\n", cmd)
	s.replicas.Range(func(addr net.Addr, conn net.Conn) bool {
		if _, err := conn.Write(cmd.Encode()); err != nil {
			fmt.Println(err)
		}
		return true
	})
}
