package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	serverInfo     Info
	config         Config
	master         net.Conn
	replicas       map[net.Addr]replica
	bytesProcessed int
}

type replica struct {
	conn      net.Conn
	bytesSent *atomic.Int32
	acks      chan int
}

type CacheEntry struct {
	value resp.Value
	ttl   optional.Value[time.Time]
}

func (c CacheEntry) String() string {
	ttl := "None"
	if t, ok := c.ttl.Get(); ok {
		ttl = t.Format(time.RFC3339)
	}
	return fmt.Sprintf("{value: %+v, ttl: %s}", c.value, ttl)
}

type Role string

const (
	Master  Role = "master"
	Replica Role = "slave"
)

type ServerParams struct {
	Host       string
	Port       int
	Role       Role
	ServerInfo Info
	Config     Config
}

func NewServer(params ServerParams) *Server {
	return &Server{
		host:       params.Host,
		port:       params.Port,
		serverInfo: params.ServerInfo,
		config:     params.Config,
		cache:      cache.NewTypedSyncMap[resp.BulkString, CacheEntry](),
		replicas:   make(map[net.Addr]replica),
	}
}

type roleCtxKeyType string

const (
	roleCtxKey roleCtxKeyType = "role"
)

func (s *Server) Start() {
	ctxlog.AddKeyPrefix(roleCtxKey)
	role := s.serverInfo.Replication.Role
	ctx := context.WithValue(context.Background(), roleCtxKey, string(role))
	if role == Master {
		rdb, err := loadRDBFile(s.config["dir"].(string), s.config["dbfilename"].(string))
		if err != nil {
			ctxlog.Fatalf(ctx, "loadRDBFile: %v", err)
		}
		state, err := load(rdb)
		if err != nil {
			ctxlog.Fatalf(ctx, "Failed to load RDB contents: %v", err)
		}
		s.cache = state.cache
	}
	if role == Replica {
		go s.connectToMaster(ctx)
	}
	address := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		ctxlog.Fatalf(ctx, "Failed to bind to port %d", s.port)
	}
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
			ctxlog.Errorf(ctx, "command.Parse: %v", err)
			msg := fmt.Sprintf("invalid command: %v", err)
			conn.Write(resp.SimpleError{Message: msg}.Encode())
			continue
		}
		details := cmd.Details()
		messages := s.execute(ctx, cmd, conn)
		for _, message := range messages {
			if _, err := conn.Write(message.Encode()); err != nil {
				ctxlog.Errorf(ctx, "conn.Write: %v", err)
			}
		}
		if s.serverInfo.Replication.Role == Master && details.PropagateToReplica {
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
		messages := s.execute(ctx, cmd, conn)
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

func (s *Server) execute(ctx context.Context, cmd command.Command, conn net.Conn) []resp.Value {
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
		return s.replConfig(req, conn)
	case command.PSync:
		return s.psync(req, conn)
	case command.Wait:
		return s.wait(ctx, req)
	case command.ConfigGet:
		return s.configGet(ctx, req)
	case command.Keys:
		return s.keys(ctx, req)
	case command.Type:
		return s.typeOf(ctx, req)
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
		return []resp.Value{s.serverInfo.ToBulkString("replication")}
	default:
		return []resp.Value{resp.SimpleError{Message: "unknown section"}}
	}
}

func (s *Server) replConfig(cmd command.ReplConfig, conn net.Conn) []resp.Value {
	switch cmd.Key {
	case "GETACK":
		response := resp.Array{
			resp.BulkString("REPLCONF"),
			resp.BulkString("ACK"),
			resp.BulkString(strconv.Itoa(s.bytesProcessed)),
		}
		return []resp.Value{response}
	case "ACK":
		offset, _ := strconv.Atoi(string(cmd.Value))
		s.replicas[conn.RemoteAddr()].acks <- offset
		return nil
	default:
		return []resp.Value{resp.String("OK")}
	}
}

func (s *Server) psync(req command.PSync, conn net.Conn) []resp.Value {
	defaultID := resp.BulkString(s.serverInfo.Replication.MasterReplicationID)
	defaultOffset := resp.BulkString(strconv.Itoa(s.serverInfo.Replication.MasterReplicationOffset))
	id := req.ReplicationID.GetOrDefault(resp.BulkString(defaultID))
	offset := req.ReplicationOffset.GetOrDefault(resp.BulkString(defaultOffset))
	resync := fmt.Sprintf("FULLRESYNC %s %s", id, offset)
	rdb, err := dump(state{cache: s.cache})
	if err != nil {
		return []resp.Value{resp.SimpleError{Message: "failed to encode current state"}}
	}
	s.replicas[conn.RemoteAddr()] = replica{conn: conn, bytesSent: &atomic.Int32{}, acks: make(chan int, 3)}
	return []resp.Value{
		resp.String(resync),
		resp.RDBFile(rdb),
	}
}

func (s *Server) wait(ctx context.Context, req command.Wait) []resp.Value {
	var wg sync.WaitGroup
	var count atomic.Int32
	for _, replica := range s.replicas {
		if replica.bytesSent.Load() == 0 {
			count.Add(1)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(125 * time.Millisecond)
			input := resp.Array{
				resp.BulkString("REPLCONF"),
				resp.BulkString("GETACK"),
				resp.BulkString("*"),
			}
			if _, err := replica.conn.Write(input.Encode()); err != nil {
				ctxlog.Errorf(ctx, "conn.Write: %v", err)
				return
			}
			// Wait for ack.
			select {
			case <-replica.acks:
				count.Add(1)
			case <-timer.C:
				return
			}
		}()
	}
	wg.Wait()
	timer := time.NewTimer(req.Timeout)
	var done bool
	for !done {
		select {
		case <-timer.C:
			done = true
		default:
			done = count.Load() >= int32(req.ReplicaCount)
		}
	}
	return []resp.Value{resp.Integer(count.Load())}
}

func (s *Server) configGet(_ context.Context, req command.ConfigGet) []resp.Value {
	var result resp.Array
	for _, key := range req.Keys {
		if value, ok := s.config[string(key)]; ok {
			result = append(result, key, resp.BulkString(fmt.Sprint(value)))
		}
	}
	return []resp.Value{result}
}

func (s *Server) keys(_ context.Context, req command.Keys) []resp.Value {
	var result resp.Array
	s.cache.Range(func(key resp.BulkString, _ CacheEntry) bool {
		if req.Pattern.Match(string(key)) {
			result = append(result, key)
		}
		return true
	})
	return []resp.Value{result}
}

func (s *Server) typeOf(_ context.Context, req command.Type) []resp.Value {
	entry, ok := s.cache.Get(req.Key)
	if !ok {
		return []resp.Value{resp.String("none")}
	}
	switch entry.value.(type) {
	case resp.BulkString:
		return []resp.Value{resp.String("string")}
	default:
		return []resp.Value{resp.String("none")}
	}
}

func (s *Server) connectToMaster(ctx context.Context) {
	host := s.serverInfo.Replication.MasterHost
	port := strconv.Itoa(s.serverInfo.Replication.MasterPort)
	addr := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		ctxlog.Fatalf(ctx, "Error connecting to master: %v", err)
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
	// replaces its state with the contents of the file.
	rdb, err := decoder.DecodeRDBFile()
	if err != nil {
		ctxlog.Fatalf(ctx, "decoder.DecodeRDBFile: %v", err)
	}
	state, err := load([]byte(rdb))
	if err != nil {
		ctxlog.Fatalf(ctx, "failed to load RDB: %v", err)
	}
	s.cache = state.cache
}

func (s *Server) propagate(ctx context.Context, cmd resp.Array) {
	ctxlog.Infof(ctx, "propagating %v to replicas\n", cmd)
	for _, replica := range s.replicas {
		bytes := cmd.Encode()
		if _, err := replica.conn.Write(bytes); err != nil {
			ctxlog.Errorf(ctx, "conn.Write: %v", err)
		}
		replica.bytesSent.Add(int32(len(bytes)))
	}
}
