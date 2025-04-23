package main

import (
	"fmt"
	"os"

	"client/config"
	"client/protocol"

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

	files := []string{"movies.csv", "credits.csv", "ratings.csv"}
	for id, filename := range files {
		path := con.DataPath + "/" + filename
		fp, err := os.Open(path)
		if err != nil {
			log.Fatalf("Can't open file %s: %v", path, err)
			break
		}
		defer fp.Close()

		log.Debugf("Sending %s", filename)
		if err = skt.SendFile(fp, uint8(id), con.BatchSize, con.Lines); err != nil {
			log.Criticalf("Couldn't send batch of file %s: %v", filename, err)
			break
		}
	}

	log.Infof("Waiting for results")
	skt.RecvQueryResult(con.Storage)
}
