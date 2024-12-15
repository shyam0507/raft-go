package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/wal"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type PeerInfo struct {
	HttpAddr string
	TcpAddr  string
}

type ReplReq struct {
	payload      AppendEntryPayload
	corelationId string
}

type ReplRes struct {
	response     AppendEntryResponse
	corelationId string
}

type peer struct {
	HttpAddr    string
	TcpAddr     string
	ReplReqChan chan ReplReq
}

type ReplChannel struct {
	success      bool
	corelationId string
}

type WriteQuorumCallBack struct {
	totalExpectedRes int
	totalSuccessResp int
	totalErrorResp   int
	isDone           bool
}

func NewWriteQuorumCallBack() *WriteQuorumCallBack {
	return &WriteQuorumCallBack{
		totalExpectedRes: 2,
		totalSuccessResp: 1,
		isDone:           false,
	}
}

func (w *WriteQuorumCallBack) AddSuccess() {
	slog.Info("Received a success response ********")
	w.totalSuccessResp++
	if w.totalSuccessResp == w.totalExpectedRes {
		w.isDone = true
		slog.Info("Write quorum reached ********")
	}
}

type server struct {
	currentTerm int
	votedFor    string
	log         *wal.Log

	commitIndex int
	lastApplied int

	//set for leader only
	nextIndex []struct {
		index    int
		serverId string
	}
	matchIndex []struct {
		index    int
		serverId string
	}

	stateMachine sync.Map //map[string]string
	// mu           sync.RWMutex

	tcpPort  string
	httpPort string
	serverId string

	client http.Client
	peers  []peer
	state  string

	//
	prevLogIndex uint64
	prevLogTerm  int

	lastHeartBeat     atomic.Int64
	singularQueueChan chan RequestResponse

	rwl   map[string]*WriteQuorumCallBack
	rwlCh chan string
}

type RequestResponse struct {
	cmd     KVCmd
	resChan chan bool
}

type Log struct {
	Command KVCmd
	Term    int
}

type RequestVotePayload struct {
	Term         int    `json:"term"`
	CandidateId  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntryPayload struct {
	Term         int     `json:"term"`
	LeaderId     string  `json:"leaderId"`
	PrevLogIndex uint64  `json:"prevLogIndex"`
	PrevLogTerm  int     `json:"prevLogTerm"`
	Entries      []KVCmd `json:"entries"`
	LeaderCommit int     `json:"leaderCommit"`
}

type AppendEntryResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func NewServer(tPort string, hPort string, serverId string, peers []PeerInfo) (*server, error) {
	currT := time.Now().Nanosecond()
	log, err := wal.Open(fmt.Sprintf("log/mylog_%s_%d", serverId, currT), nil)

	if err != nil {
		slog.Error("Error while creating wal")
	}

	if len(peers) == 0 {
		slog.Error("Error while creating server")
	}

	var p []peer
	for _, v := range peers {
		p = append(p, peer{
			HttpAddr:    v.HttpAddr,
			TcpAddr:     v.TcpAddr,
			ReplReqChan: make(chan ReplReq, 100),
		})
	}

	s := &server{
		currentTerm:  0,
		votedFor:     "",
		log:          log,
		commitIndex:  0,
		lastApplied:  0,
		stateMachine: sync.Map{},
		// mu:           sync.RWMutex{},
		tcpPort:           tPort,
		httpPort:          hPort,
		serverId:          serverId,
		client:            *http.DefaultClient,
		peers:             p,
		prevLogIndex:      0,
		prevLogTerm:       0,
		state:             Follower,
		singularQueueChan: make(chan RequestResponse, 100),
		rwl:               map[string]*WriteQuorumCallBack{},
		rwlCh:             make(chan string, 100),
	}

	return s, nil
}

func (s *server) Start() {
	go s.startSingularUpdateQueue()

	for i := 0; i < len(s.peers); i++ {
		go s.startReplicationToPeer(&s.peers[i])
	}

	go s.startQuorum()

	s.startTCP()
}

func (s *server) startReplicationToPeer(peer *peer) {
	for d := range peer.ReplReqChan {
		_, err := s.appendEntryRPC(&d.payload, peer)

		if err != nil {
			slog.Error("Error while making the request, need to retry", "err", err)
		} else {
			if len(d.payload.Entries) > 0 {
				s.rwlCh <- d.corelationId
			}
		}
	}
	slog.Info("Replication to peer stopped")
}

func (s *server) startQuorum() {
	for d := range s.rwlCh {
		cb, ok := s.rwl[d]

		if !ok {
			continue
		}

		cb.AddSuccess()
	}
	slog.Info("Replication to peer stopped")
}

func (s *server) startTCP() {
	l, err := net.Listen("tcp", ":"+s.tcpPort)

	if err != nil {
		slog.Error("Error while start the tcp Server")
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("Error while start the tcp Server")
		}
		go s.handleTCPData(conn)
	}
}

// SingularUpdateQueue starts a goroutine to update the key-value store
// in sequence. As of now it is a no-op.
func (s *server) startSingularUpdateQueue() {
	for req := range s.singularQueueChan {
		slog.Info("SingularUpdateQueue: received data", "cmd", req.cmd)
		//replicate the command to peers

		//Step-1 write the log
		m, err := json.Marshal(Log{
			Command: req.cmd,
			Term:    s.currentTerm,
		})
		if err != nil {
			slog.Error("error while parsing command", "err", err)
		}
		s.log.Write(uint64(s.prevLogIndex+1), m)

		//Step-2 replicate the log
		payload := AppendEntryPayload{
			Term:         s.currentTerm,
			LeaderId:     s.serverId,
			PrevLogIndex: s.prevLogIndex,
			PrevLogTerm:  s.prevLogTerm,
			Entries:      []KVCmd{req.cmd},
			LeaderCommit: s.commitIndex,
		}

		//callback
		quorumCallback := NewWriteQuorumCallBack()

		for _, v := range s.peers {
			cId := uuid.New().String()
			s.rwl[cId] = quorumCallback
			v.ReplReqChan <- ReplReq{
				payload:      payload,
				corelationId: cId,
			}
		}

		s.prevLogIndex++
		s.prevLogTerm = s.currentTerm

		// go s.replicateLog([]KVCmd{req.cmd})

		//Step-3 apply the log to the SM
		// s.mu.Lock()
		slog.Info("Updating state machine", "key", req.cmd.Array[1].Bulk, "value", req.cmd.Array[2].Bulk)
		s.stateMachine.Store(req.cmd.Array[1].Bulk, req.cmd.Array[2].Bulk)
		slog.Info("Updated state machine", "key", req.cmd.Array[1].Bulk, "value", req.cmd.Array[2].Bulk)
		s.commitIndex++
		// s.mu.Unlock()

		//Step-4 Send back confirmation to client
		req.resChan <- true
	}
}

func (s *server) StartElection() {

	//transition to candidate
	s.state = Candidate
	s.currentTerm++

	totalVotesRec := 1

	//TODO implement election timeout b/w 150-300 ms

	for _, v := range s.peers {

		s.prevLogIndex, _ = s.log.LastIndex()
		if s.prevLogIndex > 0 {
			d, _ := s.log.Read(s.prevLogIndex)
			var l Log
			json.Unmarshal(d, &l)
			s.prevLogTerm = l.Term
		} else {
			s.prevLogTerm = 0
		}

		voteResp, err := s.requestVote(&RequestVotePayload{
			Term:         s.currentTerm,
			CandidateId:  s.serverId,
			LastLogIndex: s.prevLogIndex,
			LastLogTerm:  s.prevLogTerm,
		}, v)

		if err != nil {
			slog.Error("Error while doing the RPC Request, should be retried", "err", err)
			continue
		}

		if voteResp.VoteGranted {
			totalVotesRec++
		}

		if voteResp.Term > s.currentTerm {
			s.currentTerm = voteResp.Term
			s.state = Follower
			break
		}
	}

	if totalVotesRec >= (len(s.peers)+2)/2 {
		slog.Info("Server is now leader")
		s.state = Leader

		//set the next index and match index

		//TODO start sending heartbeat
		go s.sendHearbeat()
	}
}

func (s *server) requestVote(p *RequestVotePayload, peer peer) (*RequestVoteResponse, error) {
	d, _ := json.Marshal(p)
	r, err := s.client.Post(fmt.Sprintf("%s/request-vote", peer.HttpAddr), "application/json", bytes.NewBuffer(d))
	if err != nil {
		slog.Error("Error while making the post request", "err", err)
		return nil, err
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// slog.Error("Error while reading the request vote resp", "err", err)
		return nil, err
	}

	var voteResp RequestVoteResponse
	err = json.Unmarshal(body, &voteResp)

	if err != nil {
		// slog.Error("Error while unmarshlling the resp", "err", err)
		return nil, err
	}

	return &voteResp, nil
}

// TODO Optimize send only if no log request was sent
// TODO send commit index and other things as part of heartbeat
func (s *server) sendHearbeat() {
	var counter int
	ticker := time.NewTicker(time.Millisecond * HEART_BEAT_INTERVAL_LEADER)
	for range ticker.C {
		counter++
		payload := AppendEntryPayload{
			Term:         s.currentTerm,
			LeaderId:     s.serverId,
			PrevLogIndex: s.prevLogIndex,
			PrevLogTerm:  s.prevLogTerm,
			Entries:      []KVCmd{},
			LeaderCommit: s.commitIndex,
		}

		slog.Info("Sending heartbeat", "prevLogIndex", s.prevLogIndex, "prevLogTerm", s.prevLogTerm, "counter", counter)
		for _, v := range s.peers {
			v.ReplReqChan <- ReplReq{
				payload:      payload,
				corelationId: "",
			}
		}
	}
}

func (s *server) handleTCPData(conn net.Conn) {
	defer conn.Close()
	// reader := bufio.NewReader((conn))
	for {
		// message, err := reader.ReadString('\n')
		// if err != nil {
		// 	conn.Close()
		// 	return
		// }
		// slog.Info("Message incoming", "Message", string(message))
		// conn.Write([]byte("+OK\r\n"))

		resp := NewResp(conn)
		input, err := resp.ProcessCMD()
		// slog.Error(err)

		if err != nil && err == io.EOF {
			slog.Info("TCP connection was closed by the client")
			break
		}

		// command := input.Array[0].Bulk
		command := strings.ToUpper(input.Array[0].Bulk)
		handler, ok := Handlers[command]

		if !ok {
			slog.Error("Invalid command: ", "command", command)
			v := KVCmd{Typ: "string", Str: ""}
			conn.Write(v.Marshal())
			continue
		}

		if command == "SET" || command == "HSET" {

			if s.state != Leader {
				v := KVCmd{Typ: "string", Str: "Leader not elected"}
				conn.Write(v.Marshal())
				continue
			}

			resChan := make(chan bool, 1)
			s.singularQueueChan <- RequestResponse{
				cmd:     input,
				resChan: resChan,
			}

			<-resChan
			v := KVCmd{Typ: "string", Str: "Ok"}
			conn.Write(v.Marshal())
		} else {
			// s.mu.RLock()
			result := handler(input.Array[1:], &s.stateMachine)
			// s.mu.RUnlock()
			conn.Write(result.Marshal())
		}
	}

}

func (s *server) appendEntryRPC(p *AppendEntryPayload, peer *peer) (*AppendEntryResponse, error) {
	d, _ := json.Marshal(p)

	r, err := s.client.Post(fmt.Sprintf("%s/append-entry", peer.HttpAddr), "application/json", bytes.NewBuffer(d))
	if err != nil {
		slog.Error("Retry the AppendEntry RPC")
		return nil, err
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Retry the AppendEntry RPC")
		return nil, err
	}

	var resp AppendEntryResponse
	err = json.Unmarshal(body, &resp)

	if err != nil {
		slog.Error("Error while unmarshlling the AppendEntry resp")
		return nil, err
	}

	return &resp, nil
}

func (s *server) heartbeatDetection(timeout int) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(timeout))

	for range ticker.C {
		//heartbeats not received from leader
		if s.state == Follower && time.Now().UnixMilli()-s.lastHeartBeat.Load() > int64(timeout) {
			slog.Info("Leader crashed detection, election will be started")
			s.StartElection()
		}
	}
}
