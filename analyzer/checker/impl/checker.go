package impl

import (
	"analyzer/checker/config"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

type Checker struct {
	con config.Config
	log *logging.Logger
}

func NewChecker(con config.Config, log *logging.Logger) Checker {
	return Checker{
		con: con,
		log: log,
	}
}

func (c Checker) Run() error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	defer close(sigs)

	a, err := SpawnAcker(c.con.HealthCheckPort, c.con.KeepAliveRetries, c.log)
	if err != nil {
		return err
	}
	defer a.Stop()

	nextName := fmt.Sprintf(c.con.HostFmt, (c.con.Id+1)%c.con.N)
	nextMonitor, err := SpawnMonitor(c.con, c.log, []string{nextName})
	if err != nil {
		return err
	}
	defer nextMonitor.Stop()

	nodeMonitor, err := SpawnMonitor(c.con, c.log, c.con.WatchNodes)
	if err != nil {
		return err
	}
	defer nodeMonitor.Stop()

	<-sigs
	return nil
}
