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

	for _, v := range s.peers {
		d, _ := json.Marshal(RequestVotePayload{
			Term:         s.currentTerm,
			CandidateId:  s.serverId,
			LastLogIndex: s.prevLogIndex,
			LastLogTerm:  s.prevLogTerm,
		})
		r, err := s.client.Post(fmt.Sprintf("%s/request-vote", v.HttpAddr), "application/json", bytes.NewBuffer(d))
		if err != nil {
			slog.Error("Retry the RPC")
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Retry the RPC")
		}

		var voteResp RequestVoteResponse
		err = json.Unmarshal(body, &voteResp)

		if err != nil {
			slog.Error("Error while unmarshlling the resp")
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
	}
}

func (s *Server) SendHearbeat() {

}

func (s *Server) handleTCPData(conn net.Conn) {
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
		input, _ := resp.ProcessCMD()
		fmt.Println(input)

		command := input.Array[0].Bulk
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			v := Value{Typ: "string", Str: ""}
			conn.Write(v.Marshal())
			continue
		}

		if command == "SET" || command == "HSET" {

			m, err := json.Marshal(Log{
				Command: input,
				Term:    s.currentTerm,
			})

			if err != nil {
				slog.Error("error while parsing", "err", err)
			}

			s.log.Write(uint64(s.prevLogIndex+1), m)

			s.SendAppendEntryRPC([]Value{input})

			//apply the log to the SM
			s.stateMachine[input.Array[1].Bulk] = input.Array[2].Bulk
			slog.Info("SM", "state", s.stateMachine)

			s.commitIndex++
		}

		result := handler(input.Array[1:], &s.stateMachine)
		conn.Write(result.Marshal())
	}
}

func (s *Server) SendAppendEntryRPC(e []Value) {

	for _, v := range s.peers {
		d, _ := json.Marshal(AppendEntryPayload{
			Term:         s.currentTerm,
			LeaderId:     s.serverId,
			PrevLogIndex: s.prevLogIndex,
			PrevLogTerm:  s.prevLogTerm,
			Entries:      e,
			LeaderCommit: s.commitIndex,
		})

		r, err := s.client.Post(fmt.Sprintf("%s/append-entry", v.HttpAddr), "application/json", bytes.NewBuffer(d))
		if err != nil {
			slog.Error("Retry the AppendEntry RPC")
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Retry the AppendEntry RPC")
		}

		var voteResp RequestVoteResponse
		err = json.Unmarshal(body, &voteResp)

		if err != nil {
			slog.Error("Error while unmarshlling the AppendEntry resp")
		}

	}

	s.prevLogIndex++
	s.prevLogTerm = s.currentTerm

}
