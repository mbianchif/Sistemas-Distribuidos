package impl

import (
	"analyzer/checker/config"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	acker, err := SpawnAcker(c.con.HealthCheckPort, c.con.KeepAliveRetries, c.log)
	if err != nil {
		return err
	}
	defer acker.Stop()

	time.Sleep(c.con.StartupGraceDuration)

	nextName := fmt.Sprintf("%s-%d", c.con.HostName, (c.con.Id+1)%c.con.N)
	monitor, err := SpawnMonitor(c.con, c.log, append(c.con.WatchNodes, nextName))
	if err != nil {
		return err
	}
	defer monitor.Stop()

	c.log.Infof("Running...")
	<-sigs
	return nil
}
