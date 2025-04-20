package main

import (
	"workers/explode/config"
	impl "workers/explode/impl"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	con, err := config.Create()
	if err != nil {
		log.Fatalf("failed config: %v", err)
	}
	log.Debug("successfull config")

	w, err := impl.New(con)
	if err != nil {
		w.Close()
		log.Fatalf("failed init: %v", err)
	}
	log.Debug("successfull init")

	if err := w.Run(con, log); err != nil {
		log.Criticalf("failed run: %v", err)
	}

	log.Infof("Terminating...")
	w.Close()
}
