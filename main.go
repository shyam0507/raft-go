package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"github.com/shyam0507/raft-go/internal"
	server "github.com/shyam0507/raft-go/internal"
)

func main() {
	var serverId string
	var startElection bool

	flag.StringVar(&serverId, "serverId", "s1", "id of the server")
	flag.BoolVar(&startElection, "startElection", false, "start election for first time")
	flag.Parse()

	slog.Info("Params received", "serverId", serverId, "Start Election", startElection)

	//load the env
	err := godotenv.Load(fmt.Sprintf("%s.env", serverId))
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	tcpPort := os.Getenv("TCP_PORT")
	httpPort := os.Getenv("HTTP_PORT")
	peersHttp := strings.Split(os.Getenv("PEERS_HTTP"), ",")

	if !startElection {
		// var er error
		v, _ := strconv.ParseBool(os.Getenv("START_ELECTION"))
		startElection = v
	}

	var peers []server.Peer
	for _, v := range peersHttp {
		peers = append(peers, server.Peer{
			HttpAddr: v,
		})
	}

	s, _ := server.NewServer(tcpPort, httpPort, serverId, peers)

	internal.NewHTTPServer(s)

	slog.Info("HTTP server running")

	if startElection {
		s.StartElection()
	}

	s.Start()

}
