package main

import (
	"analyzer/checker/config"
	"analyzer/checker/impl"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	con, err := config.Create()
	if err != nil {
		log.Fatalf("failed config: %v", err)
	}
	log.Debug("successfull config")

	c := impl.NewChecker(con, log)
	if err := c.Run(); err != nil {
		log.Criticalf("failed run: %v", err)
	}

	log.Infof("Terminating...")
}
