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
	"time"

	"github.com/tidwall/wal"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type Peer struct {
	HttpAddr string
	TcpAddr  string
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

	stateMachine map[string]string
	mu           sync.RWMutex

	tcpPort  string
	httpPort string
	serverId string

	client http.Client
	peers  []Peer
	state  string

	//
	prevLogIndex uint64
	prevLogTerm  int

	lastHeartBeat  int64
	requestChannel chan RequestResponse
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

func NewServer(tPort string, hPort string, serverId string, peers []Peer) (*server, error) {
	currT := time.Now().Nanosecond()
	log, err := wal.Open(fmt.Sprintf("log/mylog_%s_%d", serverId, currT), nil)

	if err != nil {
		slog.Error("Error while creating wal")
	}

	s := &server{
		currentTerm:    0,
		votedFor:       "",
		log:            log,
		commitIndex:    0,
		lastApplied:    0,
		stateMachine:   map[string]string{},
		mu:             sync.RWMutex{},
		tcpPort:        tPort,
		httpPort:       hPort,
		serverId:       serverId,
		client:         *http.DefaultClient,
		peers:          peers,
		prevLogIndex:   0,
		prevLogTerm:    0,
		state:          Follower,
		requestChannel: make(chan RequestResponse, 100),
	}

	return s, nil
}

func (s *server) Start() {
	go s.startSingularUpdateQueue()
	s.startTCP()
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
	for req := range s.requestChannel {
		slog.Info("SingularUpdateQueue: received data", "cmd", req.cmd)
		//replicate the command to peers
		s.replicateLog([]KVCmd{req.cmd})

		//apply the log to the SM
		s.mu.Lock()
		s.stateMachine[req.cmd.Array[1].Bulk] = req.cmd.Array[2].Bulk
		s.mu.Unlock()
		slog.Info("SM", "state", s.stateMachine)

		s.commitIndex++

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

func (s *server) requestVote(p *RequestVotePayload, peer Peer) (*RequestVoteResponse, error) {
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
	ticker := time.NewTicker(time.Millisecond * HEART_BEAT_INTERVAL_LEADER)
	for range ticker.C {
		s.replicateLog([]KVCmd{})
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

			m, err := json.Marshal(Log{
				Command: input,
				Term:    s.currentTerm,
			})

			if err != nil {
				slog.Error("error while parsing command", "err", err)
			}

			//TODO migrate this
			s.log.Write(uint64(s.prevLogIndex+1), m)

			resChan := make(chan bool, 1)
			s.requestChannel <- RequestResponse{
				cmd:     input,
				resChan: resChan,
			}

			slog.Info("Added data to request channel")
			<-resChan
			v := KVCmd{Typ: "string", Str: "Ok"}
			conn.Write(v.Marshal())
		} else {
			s.mu.RLock()
			result := handler(input.Array[1:], &s.stateMachine)
			s.mu.RUnlock()
			conn.Write(result.Marshal())
		}

	}

}

func (s *server) replicateLog(e []KVCmd) {

	for _, v := range s.peers {

		s.appendEntryRPC(&AppendEntryPayload{
			Term:         s.currentTerm,
			LeaderId:     s.serverId,
			PrevLogIndex: s.prevLogIndex,
			PrevLogTerm:  s.prevLogTerm,
			Entries:      e,
			LeaderCommit: s.commitIndex,
		}, v)

	}

	if len(e) > 0 {
		s.prevLogIndex++
		s.prevLogTerm = s.currentTerm
	}

}

func (s *server) appendEntryRPC(p *AppendEntryPayload, peer Peer) (*AppendEntryResponse, error) {
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
		if s.state == Follower && time.Now().UnixMilli()-s.lastHeartBeat > int64(timeout) {
			slog.Info("Leader crashed detection, election will be started")
			s.StartElection()
		}
	}
}
