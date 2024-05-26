package redis

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/cache"
	"github.com/codecrafters-io/redis-starter-go/optional"
	"github.com/codecrafters-io/redis-starter-go/parse"
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
	parser := parse.NewParser(conn)
	for {
		req, err := parser.Parse()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Serialize())
			continue
		}
		for _, message := range s.execute(req, conn) {
			conn.Write(message.Serialize())
		}
	}
}

func (s *Server) handleMasterConnection(conn net.Conn) {
	defer conn.Close()
	parser := parse.NewParser(conn)
	for {
		req, err := parser.Parse()
		if err != nil {
			conn.Write(resp.SimpleError{Message: err.Error()}.Serialize())
			continue
		}
		s.execute(req, conn)
	}
}

func (s *Server) execute(req resp.Value, conn net.Conn) []resp.Value {
	array, ok := req.(resp.Array)
	if !ok {
		return []resp.Value{resp.SimpleError{Message: "invalid input format"}}
	}
	cmd, err := parse.Command(array)
	if err != nil {
		return []resp.Value{resp.SimpleError{Message: err.Error()}}
	}
	fmt.Printf("Received command %T%+v\n", cmd, cmd)
	switch req := cmd.(type) {
	case resp.Ping:
		return s.ping(req)
	case resp.Echo:
		return s.echo(req)
	case resp.Set:
		response := s.set(req)
		if s.config.Replication.Role == Master {
			s.propagate(array)
		}
		return response
	case resp.Get:
		return s.get(req)
	case resp.Info:
		return s.info(req)
	case resp.ReplConfig:
		return s.replConfig(req)
	case resp.PSync:
		return s.psync(req, conn)
	}
	return []resp.Value{resp.SimpleError{Message: "unknown command"}}
}

func (s *Server) ping(req resp.Ping) []resp.Value {
	if msg, ok := req.Message.Get(); ok {
		return []resp.Value{msg}
	}
	return []resp.Value{resp.String("PONG")}
}

func (s *Server) echo(req resp.Echo) []resp.Value {
	return []resp.Value{req.Message}
}

func (s *Server) set(req resp.Set) []resp.Value {
	s.cache.Put(req.Key, CacheEntry{
		value: req.Value,
		ttl:   req.TTL,
	})
	return []resp.Value{resp.String("OK")}
}

func (s *Server) get(req resp.Get) []resp.Value {
	entry, ok := s.cache.Get(req.Key)
	if !ok {
		return []resp.Value{resp.NullBulkString{}}
	}
	if ttl, ok := entry.ttl.Get(); ok && time.Now().After(ttl) {
		return []resp.Value{resp.NullBulkString{}}
	}
	return []resp.Value{entry.value}
}

func (s *Server) info(req resp.Info) []resp.Value {
	switch req.Section {
	case "replication":
		return []resp.Value{s.config.ToBulkString("replication")}
	default:
		return []resp.Value{resp.SimpleError{Message: "unknown section"}}
	}
}

func (s *Server) replConfig(_ resp.ReplConfig) []resp.Value {
	return []resp.Value{resp.String("OK")}
}

func (s *Server) psync(req resp.PSync, conn net.Conn) []resp.Value {
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

	// Initiate the master-replica handshake.
	for _, message := range messages {
		conn.Write(message.Serialize())
		resp, err := parser.Parse()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("got ", resp)
	}
	return conn
}

func (s *Server) propagate(cmd resp.Array) {
	s.replicas.Range(func(addr net.Addr, conn net.Conn) bool {
		if _, err := conn.Write(cmd.Serialize()); err != nil {
			fmt.Println(err)
		}
		return true
	})
}
