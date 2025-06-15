package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"
)

const CHECKER_COMPOSE_FILENAME = "compose.checkers.yaml"
const HOST_FMT = "health-checker-%d"

type Config struct {
	// .env
	HealthCheckPort               uint16
	DefaultSleepDuration          time.Duration
	RespawnSleepDuration          time.Duration
	StartingKeepAliveWaitDuration time.Duration
	KeepAliveRetries              int
	RespawnRetries                int

	// compose
	Id                     int
	N                      int
	ContainerNames         []string
	CheckerComposeFileName string
	HostFmt                string
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

	// CONTAINER_NAMES
	ContainerNamesStr := os.Getenv("CONTAINER_NAMES")
	ContainerNames := strings.Split(ContainerNamesStr, ",")
	if len(ContainerNames) == 0 {
		return Config{}, fmt.Errorf("the container names are empty")
	}

	// CHECKER_COMPOSE_FILE_NAME
	checkerComposeFileName := os.Getenv("CHECKER_COMPOSE_FILE_NAME")
	if len(checkerComposeFileName) == 0 {
		return Config{}, fmt.Errorf("the checker compose file name is empty")
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

	// RESPAWN_SLEEP_DURATION
	respawnSleepDurationInt, err := strconv.Atoi(os.Getenv("RESPAWN_SLEEP_DURATION"))
	if err != nil {
		return Config{}, fmt.Errorf("the respawn sleep duration is invalid: %v", err)
	}
	if respawnSleepDurationInt < 0 {
		return Config{}, fmt.Errorf("the respawn sleep duration must be a positive number or zero")
	}
	respawnSleepDuration := time.Duration(respawnSleepDurationInt) * time.Second

	// STARTING_KEEP_ALIVE_WAIT_DURATION
	startingKeepAliveWaitDurationInt, err := strconv.Atoi(os.Getenv("STARTING_KEEP_ALIVE_DURATION"))
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

	// RESPAWN_RETRIES
	respawnRetries, err := strconv.Atoi(os.Getenv("RESPAWN_RETRIES"))
	if err != nil {
		return Config{}, fmt.Errorf("the respawn retries value is invalid: %v", err)
	}
	if respawnRetries < 0 {
		return Config{}, fmt.Errorf("the respawn retries value must be a postive number or zero")
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
		ContainerNames:                ContainerNames,
		CheckerComposeFileName:        checkerComposeFileName,
		HostFmt:                       hostFmt,
		HealthCheckPort:               uint16(healthCheckPort),
		DefaultSleepDuration:          defaultSleepDuration,
		RespawnSleepDuration:          respawnSleepDuration,
		StartingKeepAliveWaitDuration: startingKeepAliveWaitDuration,
		RespawnRetries:                respawnRetries,
		KeepAliveRetries:              keepAliveRetries,
	}, nil
}
