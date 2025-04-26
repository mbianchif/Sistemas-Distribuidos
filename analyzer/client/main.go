package main

import (
	"fmt"
	"os"

	"analyzer/client/config"
	"analyzer/client/protocol"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func configLog(logLevel logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{time:2006-01-02 15:04:05}	%{level:.4s}	%{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logLevel, "log")
	logging.SetBackend(backendLeveled)
}

func main() {
	con, err := config.Create()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Couldn't read envars:", err)
	}
	configLog(con.LogLevel)

	skt, err := protocol.NewConnection(con.GatewayHost, con.GatewayPort, log)
	if err != nil {
		skt.Close()
		log.Fatalf("Can't connect with gateway: %v", err)
	}
	defer skt.Close()
	log.Infof("Connected to gateway server")

	// Send files to gateway
	files := []string{"movies.csv", "credits.csv", "ratings.csv"}
	go protocol.SendFiles(skt, con, log, files)

	skt.RecvQueryResult(con.Storage, 5)
}
