package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/command"
	"github.com/codecrafters-io/redis-starter-go/lib/cache"
	"github.com/codecrafters-io/redis-starter-go/lib/ctxlog"
	"github.com/codecrafters-io/redis-starter-go/lib/optional"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Server struct {
	host           string
	port           int
	cache          cache.Map[resp.BulkString, CacheEntry]
	config         Config
	master         net.Conn
	replicas       cache.Map[net.Addr, net.Conn]
	bytesProcessed int
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

type roleCtxKeyType string

const (
	roleCtxKey roleCtxKeyType = "role"
)

func (s *Server) Start() {
	ctxlog.AddKeyPrefix(roleCtxKey)
	role := s.config.Replication.Role
	ctx := context.WithValue(context.Background(), roleCtxKey, string(role))
	if role == Replica {
		go s.connectToMaster(ctx)
	}
	address := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		ctxlog.Fatalf(ctx, "Failed to bind to port %d", s.port)
	}
	go func() {
		if s.config.Replication.Role == Replica {
			return
		}
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			s.replicas.Range(func(_ net.Addr, conn net.Conn) bool {
				input := resp.Array{
					resp.BulkString("REPLCONF"),
					resp.BulkString("GETACK"),
					resp.BulkString("*"),
				}
				if _, err := conn.Write(input.Encode()); err != nil {
					ctxlog.Errorf(ctx, "conn.Write: %v", err)
				}
				return true
			})
		}
	}()
	ctxlog.Infof(ctx, "Listening at address %s...", address)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			ctxlog.Errorf(ctx, "listener.Accept: %v", err)
			continue
		}
		ctxlog.Infof(ctx, "Accepted connection with %s", conn.RemoteAddr())
		go s.handleClientConnection(ctx, conn)
	}
}

func (s *Server) handleClientConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	decoder := resp.NewDecoder(conn)
	for {
		input, err := decoder.DecodeArray()
		if err != nil {
			continue
		}
		ctxlog.Infof(ctx, "received %v", input)
		cmd, err := command.Parse(input)
		if err != nil {
			ctxlog.Errorf(ctx, "command.Parse: %v", cmd)
			continue
		}
		details := cmd.Details()
		messages := s.execute(cmd, conn)
		for _, message := range messages {
			if _, err := conn.Write(message.Encode()); err != nil {
				ctxlog.Errorf(ctx, "conn.Write: %v", err)
			}
		}
		if s.config.Replication.Role == Master && details.PropagateToReplica {
			s.propagate(ctx, input)
		}
	}
}

func (s *Server) handleMasterConnection(ctx context.Context, conn net.Conn, decoder resp.Decoder) {
	for {
		input, err := decoder.DecodeArray()
		if err != nil {
			ctxlog.Errorf(ctx, "decoder.DecodeArray: %v", err)
			continue
		}
		ctxlog.Infof(ctx, "received %v", input)
		cmd, err := command.Parse(input)
		if err != nil {
			ctxlog.Errorf(ctx, "command.Parse: %v", err)
			continue
		}
		details := cmd.Details()
		messages := s.execute(cmd, conn)
		if details.RequiresReplicaResponse {
			for _, message := range messages {
				if _, err := conn.Write(message.Encode()); err != nil {
					ctxlog.Errorf(ctx, "conn.Write: %v", err)
				}
			}
		}
		s.bytesProcessed += len(input.Encode())
	}
}

func (s *Server) execute(cmd command.Command, conn net.Conn) []resp.Value {
	switch req := cmd.(type) {
	case command.Ping:
		return s.ping(req)
	case command.Echo:
		return s.echo(req)
	case command.Set:
		return s.set(req)
	case command.Get:
		return s.get(req)
	case command.Info:
		return s.info(req)
	case command.ReplConfig:
		return s.replConfig(req)
	case command.PSync:
		return s.psync(req, conn)
	case command.Wait:
		return s.wait(req)
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

func (s *Server) replConfig(cmd command.ReplConfig) []resp.Value {
	switch cmd.Key {
	case "GETACK":
		response := resp.Array{
			resp.BulkString("REPLCONF"),
			resp.BulkString("ACK"),
			resp.BulkString(strconv.Itoa(s.bytesProcessed)),
		}
		return []resp.Value{response}
	case "ACK":
		return nil
	default:
		return []resp.Value{resp.String("OK")}
	}
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

func (s *Server) wait(_ command.Wait) []resp.Value {
	return []resp.Value{resp.Integer(s.replicas.Len())}
}

func (s *Server) state() (string, error) {
	state, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return "", err
	}
	return string(state), nil
}

func (s *Server) connectToMaster(ctx context.Context) {
	host := s.config.Replication.MasterHost
	port := strconv.Itoa(s.config.Replication.MasterPort)
	addr := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		ctxlog.Fatalf(ctx, "Error connecting to master: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	decoder := resp.NewDecoder(conn)
	s.sendToMaster(ctx, conn, resp.BulkString("PING"))
	s.awaitResponse(ctx, decoder, "PONG")
	s.sendToMaster(ctx, conn,
		resp.BulkString("REPLCONF"),
		resp.BulkString("listening-port"),
		resp.BulkString(strconv.Itoa(s.port)),
	)
	s.awaitResponse(ctx, decoder, "OK")
	s.sendToMaster(ctx, conn,
		resp.BulkString("REPLCONF"),
		resp.BulkString("capa"),
		resp.BulkString("psync2"),
	)
	s.awaitResponse(ctx, decoder, "OK")
	s.sendToMaster(ctx, conn,
		resp.BulkString("PSYNC"),
		resp.BulkString("?"),
		resp.BulkString("-1"),
	)
	s.awaitSync(ctx, decoder)
	s.master = conn
	s.handleMasterConnection(ctx, conn, decoder)
}

func (s *Server) sendToMaster(ctx context.Context, conn net.Conn, values ...resp.Value) {
	array := resp.Array(values)
	_, err := conn.Write(array.Encode())
	if err != nil {
		ctxlog.Fatalf(ctx, "conn.Write: %v", err)
	}
}

func (s *Server) awaitResponse(ctx context.Context, decoder resp.Decoder, response string) {
	got, err := decoder.Decode()
	if err != nil {
		ctxlog.Fatalf(ctx, "decoder.Decode: %v", err)
	}
	if str := got.(resp.String); str != resp.String(response) {
		ctxlog.Fatalf(ctx, "awaitResponse: expected %v, got %v", response, str)
	}
}

func (s *Server) awaitSync(ctx context.Context, decoder resp.Decoder) {
	// The master is expected to first respond with a FULLRESYNC message.
	// Currently, the replica does not do anything with this.
	resync, err := decoder.Decode()
	if err != nil {
		ctxlog.Fatalf(ctx, "decoder.Decode: %v", err)
	}
	if str := resync.(resp.String); !strings.HasPrefix(string(str), "FULLRESYNC") {
		ctxlog.Fatalf(ctx, "awaitSync: expected string with FULLRESYNC prefix, got %v", str)
	}

	// Next, the master sends its contents as a RDB file. The replica
	// would usually replace its state with the contents of the file.
	file, err := decoder.DecodeRDBFile()
	if err != nil {
		ctxlog.Fatalf(ctx, "decoder.DecodeRDBFile: %v", err)
	}
	ctxlog.Infof(ctx, "received RDB file: %v", []byte(file))
}

func (s *Server) propagate(ctx context.Context, cmd resp.Array) {
	fmt.Printf("propagating %v to replicas\n", cmd)
	s.replicas.Range(func(addr net.Addr, conn net.Conn) bool {
		if _, err := conn.Write(cmd.Encode()); err != nil {
			ctxlog.Errorf(ctx, "conn.Write: %v", err)
		}
		return true
	})
}
