package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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

type Server struct {
	currentTerm int
	votedFor    string
	log         *wal.Log

	commitIndex int
	lastApplied int

	//set for leader only
	nextIndex  []int
	matchIndex []int

	stateMachine map[string]string

	tcpPort  string
	httpPort string
	serverId string

	client http.Client
	peers  []Peer
	state  string

	//
	prevLogIndex int
	prevLogTerm  int

	lastHeartBeat int64
}

type Log struct {
	Command Value
	Term    int
}

type RequestVotePayload struct {
	Term         int    `json:"term"`
	CandidateId  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntryPayload struct {
	Term         int     `json:"term"`
	LeaderId     string  `json:"leaderId"`
	PrevLogIndex int     `json:"prevLogIndex"`
	PrevLogTerm  int     `json:"prevLogTerm"`
	Entries      []Value `json:"entries"`
	LeaderCommit int     `json:"leaderCommit"`
}

type AppendEntryResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func NewServer(tPort string, hPort string, serverId string, peers []Peer) (*Server, error) {
	currT := time.Now().Nanosecond()
	log, err := wal.Open(fmt.Sprintf("log/mylog_%s_%d", serverId, currT), nil)

	if err != nil {
		slog.Error("Error while creating wal")
	}

	s := &Server{
		currentTerm:  0,
		votedFor:     "",
		log:          log,
		commitIndex:  0,
		lastApplied:  0,
		stateMachine: map[string]string{},
		tcpPort:      tPort,
		httpPort:     hPort,
		serverId:     serverId,
		client:       *http.DefaultClient,
		peers:        peers,
		prevLogIndex: 0,
		prevLogTerm:  0,
		state:        Follower,
	}

	return s, nil
}

func (s *Server) StartTCP() {
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

func (s *Server) StartElection() {

	//transition to candidate
	s.state = Candidate
	s.currentTerm++

	totalVotesRec := 1

	//TODO implement election timeout b/w 150-300 ms

	for _, v := range s.peers {

		voteResp, err := s.RequestVote(&RequestVotePayload{
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
		//TODO start sending heartbeat
		go s.SendHearbeat()
	}
}

func (s *Server) RequestVote(p *RequestVotePayload, peer Peer) (*RequestVoteResponse, error) {
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
func (s *Server) SendHearbeat() {
	ticker := time.NewTicker(time.Millisecond * HEART_BEAT_INTERVAL_LEADER)
	for range ticker.C {
		s.ReplicateLog([]Value{})
	}
}

func (s *Server) handleTCPData(conn net.Conn) {
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

		command := input.Array[0].Bulk
		handler, ok := Handlers[command]

		if !ok {
			slog.Error("Invalid command: ", "command", command)
			v := Value{Typ: "string", Str: ""}
			conn.Write(v.Marshal())
			continue
		}

		if command == "SET" || command == "HSET" {

			if s.state != Leader {
				v := Value{Typ: "string", Str: "Leader not elected"}
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

			s.log.Write(uint64(s.prevLogIndex+1), m)

			s.ReplicateLog([]Value{input})

			//apply the log to the SM
			s.stateMachine[input.Array[1].Bulk] = input.Array[2].Bulk
			slog.Info("SM", "state", s.stateMachine)

			s.commitIndex++
		}

		result := handler(input.Array[1:], &s.stateMachine)
		conn.Write(result.Marshal())
	}
}

func (s *Server) ReplicateLog(e []Value) {

	for _, v := range s.peers {

		s.AppendEntryRPC(&AppendEntryPayload{
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
	}
	s.prevLogTerm = s.currentTerm

}

func (s *Server) AppendEntryRPC(p *AppendEntryPayload, peer Peer) (*AppendEntryResponse, error) {
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

func (s *Server) HeartbeatDetection(timeout int) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(timeout))

	for range ticker.C {
		//heartbeats not received from leader
		if s.state == Follower && time.Now().UnixMilli()-s.lastHeartBeat > int64(timeout) {
			slog.Info("Leader crashed detection, election will be started")
			s.StartElection()
		}
	}
}
