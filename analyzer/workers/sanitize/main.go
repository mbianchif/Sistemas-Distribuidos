package main

import (
	"analyzer/workers/sanitize/config"
	impl "analyzer/workers/sanitize/impl"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	con, err := config.Create()
	if err != nil {
		log.Fatalf("failed config: %v", err)
	}
	log.Debug("successfull config")

	w, err := impl.New(con, log)
	if err != nil {
		w.Close()
		log.Fatalf("failed init: %v", err)
	}
	log.Debug("successfull init")
	defer w.Close()

	if err := w.Run(); err != nil {
		log.Criticalf("failed run: %v", err)
	}

	log.Infof("Terminating...")
}
