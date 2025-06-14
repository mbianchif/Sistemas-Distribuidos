package impl

import (
	"analyzer/checker/config"
	"net"

	"github.com/op/go-logging"
)

type Checker struct {
	config config.Config
	conn   *net.UDPConn
}

func New(con config.Config, log *logging.Logger) (Checker, error) {
	go spawnKeepAlive(con, log)
	return Checker{}, nil
}

func (w *Checker) Run() error {
	return nil
}

func (w *Checker) Close() error {
	return nil
}
