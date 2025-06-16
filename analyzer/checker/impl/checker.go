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

	ackerLog := logging.MustGetLogger("acker")
	a, err := SpawnAcker(c.con.HealthCheckPort, c.con.KeepAliveRetries, ackerLog)
	if err != nil {
		return err
	}
	defer a.Stop()

	nextLog := logging.MustGetLogger("next monitor")
	nextName := fmt.Sprintf(c.con.HostFmt, (c.con.Id+1)%c.con.N)
	nextMonitor, err := SpawnMonitor(c.con, nextLog, []string{nextName})
	if err != nil {
		return err
	}
	defer nextMonitor.Stop()

	nodeLog := logging.MustGetLogger("node monitor")
	nodeMonitor, err := SpawnMonitor(c.con, nodeLog, c.con.WatchNodes)
	if err != nil {
		return err
	}
	defer nodeMonitor.Stop()

	<-sigs
	return nil
}
