package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	Id              int
	N               int
	KeepAlivePort   uint16
	HealthCheckPort uint16
	NodeDockerNames []string
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
	empty := Config{}

	// ID
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return empty, fmt.Errorf("the given id is invalid: %v", err)
	}

	// N
	n, err := strconv.Atoi(os.Getenv("N"))
	if err != nil {
		return empty, fmt.Errorf("the given n is invalid: %v", err)
	}

	// KEEP_ALIVE_PORT
	keepAlivePort, err := strconv.ParseUint(os.Getenv("KEEP_ALIVE_PORT"), 10, 16)
	if err != nil {
		return empty, fmt.Errorf("the provided keep alive port is invalid: %v", err)
	}

	// HEALTH_CHECK_PORT
	healthCheckPort, err := strconv.ParseUint(os.Getenv("HEALTH_CHECK_PORT"), 10, 16)
	if err != nil {
		return empty, fmt.Errorf("the provided health check port is invalid: %v", err)
	}

	// NODE_DOCKER_NAMES
	nodeDockerNamesStr := os.Getenv("NODE_DOCKER_NAMES")
	nodeDockerNames := strings.Split(nodeDockerNamesStr, ",")
	if len(nodeDockerNames) == 0 {
		return empty, fmt.Errorf("the node docker names are empty")
	}

	// LOG_LEVEL
	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}
	configLog(logLevel)

	return Config{
		Id:              id,
		N:               n,
		KeepAlivePort:   uint16(keepAlivePort),
		HealthCheckPort: uint16(healthCheckPort),
		NodeDockerNames: nodeDockerNames,
	}, nil
}
