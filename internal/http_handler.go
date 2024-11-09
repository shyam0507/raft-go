package internal

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
)

type HttpServer struct {
	s *Server
}

func NewHTTPServer(s *Server) *HttpServer {
	h := HttpServer{s: s}
	go h.startHTTPServer()
	return &h
}

func (h *HttpServer) startHTTPServer() {
	http.HandleFunc("/request-vote", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Received Request Vote Request")
		var req RequestVotePayload
		b, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Error while parsing the request vote response")
		}
		json.Unmarshal(b, &req)

		slog.Info("Received Request Payload", "Payload", req)

		var resp RequestVoteResponse
		if req.Term > h.s.currentTerm && req.LastLogIndex >= h.s.prevLogIndex {
			resp.VoteGranted = true
			h.s.votedFor = req.CandidateId
			h.s.currentTerm = req.Term
		} else {
			resp.VoteGranted = false
			resp.Term = h.s.currentTerm
		}

		d, err := json.Marshal(resp)
		if err != nil {
			slog.Error("Error while parsing the request vote response")
		}

		w.Write(d)

	})

	http.HandleFunc("/append-entry", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Received Append Entry Request")
		var req AppendEntryPayload
		b, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Error while parsing the append entry response")
		}
		json.Unmarshal(b, &req)

		slog.Info("Received Append Entry Request Payload", "Payload", req)

		valid := h.validateAppendRequest(req)

		slog.Info("Received Append Entry Request Is", "Valid", valid)

		var resp AppendEntryResponse
		if valid {
			newLogIndex := req.PrevLogIndex + 1
			l := Log{
				Command: req.Entries[0],
				Term:    req.Term,
			}

			d, _ := json.Marshal(l)

			h.s.log.Write(uint64(newLogIndex), d)

			//Fix this
			if req.LeaderCommit > h.s.commitIndex {
				var l Log
				data, _ := h.s.log.Read(uint64(req.LeaderCommit))
				json.Unmarshal(data, &l)

				slog.Info("Received Data", "s", l)

				//apply the log to the SM
				h.s.stateMachine[l.Command.Array[1].Bulk] = l.Command.Array[2].Bulk
				slog.Info("SM", "state", h.s.stateMachine)
			}

			resp.Success = true
			resp.Term = h.s.currentTerm
		} else {
			resp.Success = false
			resp.Term = h.s.currentTerm
		}

		d, err := json.Marshal(resp)
		if err != nil {
			slog.Error("Error while parsing the append entry vote response")
		}

		w.Write(d)
	})

	http.ListenAndServe(":"+h.s.httpPort, nil)

}

// TBD Complete It
func (h *HttpServer) validateAppendRequest(req AppendEntryPayload) bool {

	if h.s.currentTerm > req.Term {
		return false
	}

	//checking if this is start server index will be 0
	firstIndex, _ := h.s.log.FirstIndex()
	if firstIndex == 0 && req.PrevLogIndex > 0 {
		return false
	}

	var prevLog Log
	if firstIndex > 0 {
		b, err := h.s.log.Read(uint64(req.PrevLogIndex))

		if err != nil {
			slog.Error("Could not find same log at previous index")
			return false
		}

		json.Unmarshal(b, &prevLog)
	}

	if prevLog.Term != req.PrevLogTerm {
		return false
	}

	lastI, _ := h.s.log.LastIndex()

	if lastI != uint64(req.PrevLogIndex) {
		//we need to clean the data
	}

	return true
}
