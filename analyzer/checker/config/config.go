package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"
)

type Config struct {
	// .env
	HealthCheckPort               uint16
	DefaultSleepDuration          time.Duration
	ReviveSleepDuration           time.Duration
	StartingKeepAliveWaitDuration time.Duration
	KeepAliveRetries              int
	ReviveRetries                 int

	// compose
	Id         int
	N          int
	HostFmt    string
	WatchNodes []string
}

func configLog(logLevel logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{time:2006-01-02 15:04:05}	%{level:.4s}	%{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logLevel, "log")
	logging.SetBackend(backendLeveled)
}

func Create() (Config, error) {
	// ID
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return Config{}, fmt.Errorf("the given id is invalid: %v", err)
	}

	// N
	n, err := strconv.Atoi(os.Getenv("N"))
	if err != nil {
		return Config{}, fmt.Errorf("the given n is invalid: %v", err)
	}

	// HOST_FMT
	hostFmt := os.Getenv("HOST_FMT")
	if len(hostFmt) == 0 {
		return Config{}, fmt.Errorf("the host fmt is empty")
	}

	// HEALTH_CHECK_PORT
	healthCheckPort, err := strconv.ParseUint(os.Getenv("HEALTH_CHECK_PORT"), 10, 16)
	if err != nil {
		return Config{}, fmt.Errorf("the provided health check port is invalid: %v", err)
	}

	// DEFAULT_SLEEP_DURATION
	defaultSleepDurationInt, err := strconv.Atoi(os.Getenv("DEFAULT_SLEEP_DURATION"))
	if err != nil {
		return Config{}, fmt.Errorf("the default sleep duration is invalid: %v", err)
	}
	if defaultSleepDurationInt < 0 {
		return Config{}, fmt.Errorf("the default sleep duration must be a postive number or zero")
	}
	defaultSleepDuration := time.Duration(defaultSleepDurationInt) * time.Second

	// REVIVE_SLEEP_DURATION
	reviveSleepDurationInt, err := strconv.Atoi(os.Getenv("REVIVE_SLEEP_DURATION"))
	if err != nil {
		return Config{}, fmt.Errorf("the revive sleep duration is invalid: %v", err)
	}
	if reviveSleepDurationInt < 0 {
		return Config{}, fmt.Errorf("the revive sleep duration must be a positive number or zero")
	}
	reviveSleepDuration := time.Duration(reviveSleepDurationInt) * time.Second

	// STARTING_KEEP_ALIVE_WAIT_DURATION
	startingKeepAliveWaitDurationInt, err := strconv.Atoi(os.Getenv("STARTING_KEEP_ALIVE_WAIT_DURATION"))
	if err != nil {
		return Config{}, fmt.Errorf("the starting keep alive wait duration is invalid: %v", err)
	}
	if startingKeepAliveWaitDurationInt < 0 {
		return Config{}, fmt.Errorf("the starting keep alive wait duration must be a postive number or zero")
	}
	startingKeepAliveWaitDuration := time.Duration(startingKeepAliveWaitDurationInt) * time.Second

	// KEEP_ALIVE_RETRIES
	keepAliveRetries, err := strconv.Atoi(os.Getenv("KEEP_ALIVE_RETRIES"))
	if err != nil {
		return Config{}, fmt.Errorf("the keep alive retries value is invalid: %v", err)
	}
	if keepAliveRetries < 0 {
		return Config{}, fmt.Errorf("the keep alive retries value must be a postive number or zero")
	}

	// REVIVE_RETRIES
	reviveRetries, err := strconv.Atoi(os.Getenv("REVIVE_RETRIES"))
	if err != nil {
		return Config{}, fmt.Errorf("the revive retries value is invalid: %v", err)
	}
	if reviveRetries < 0 {
		return Config{}, fmt.Errorf("the revive retries value must be a postive number or zero")
	}

	// WATCH_NODES
	watchNodesStr := os.Getenv("WATCH_NODES")
	watchNodes := strings.Split(watchNodesStr, ",")
	if len(watchNodes) == 0 {
		return Config{}, fmt.Errorf("the watch nodes field is empty")
	}

	// LOG_LEVEL
	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}
	configLog(logLevel)

	return Config{
		Id:                            id,
		N:                             n,
		HostFmt:                       hostFmt,
		HealthCheckPort:               uint16(healthCheckPort),
		DefaultSleepDuration:          defaultSleepDuration,
		ReviveSleepDuration:           reviveSleepDuration,
		StartingKeepAliveWaitDuration: startingKeepAliveWaitDuration,
		ReviveRetries:                 reviveRetries,
		WatchNodes:                    watchNodes,
		KeepAliveRetries:              keepAliveRetries,
	}, nil
}
