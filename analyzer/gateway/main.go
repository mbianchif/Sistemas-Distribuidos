package main

import (
	"fmt"
	"os"

	"analyzer/gateway/config"
	"analyzer/gateway/protocol"

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

	sv, err := protocol.NewServer(con, log)
	if err != nil {
		log.Errorf("Couldn't start server: %v", err)
		return
	}

	sv.Run()
}
