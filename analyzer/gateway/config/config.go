package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	// .env
	Url                string
	Host               string
	Port               int
	Backlog            int
	InputExchangeNames []string
	InputQueueNames    []string
	OutputExchangeName string
	OutputQueueNames   []string
	HealthCheckPort    uint16
	LogLevel           logging.Level
	KeepAliveRetries   int

	// compose
	Id           int
	InputCopies  []int
	OutputCopies []int
}

func Create() (Config, error) {
	// ID
	id := 0 // There's only one gateway

	// RABBIT_URL
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return Config{}, fmt.Errorf("no rabbit url was provided")
	}

	// HOST
	host := os.Getenv("HOST")
	if len(host) == 0 {
		host = "0.0.0.0"
	}

	// PORT
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 9090
	}

	// BACKLOG
	backlog, err := strconv.Atoi(os.Getenv("BACKLOG"))
	if err != nil {
		return Config{}, fmt.Errorf("no backlog was provided")
	}

	// INPUT_EXCHANGE_NAMES
	inputExchangeNames := strings.Split(os.Getenv("INPUT_EXCHANGE_NAMES"), ",")
	if len(inputExchangeNames) == 0 {
		return Config{}, fmt.Errorf("the input exchange names were not provided")
	}

	// INPUT_QUEUE_NAMES
	inputQueueNames := strings.Split(os.Getenv("INPUT_QUEUE_NAMES"), ",")
	if len(inputQueueNames) == 0 {
		return Config{}, fmt.Errorf("the input queue names were not provided")
	}

	if len(inputExchangeNames) != len(inputQueueNames) {
		return Config{}, fmt.Errorf("the length of input exchange names and input queue names don't match (exch: %d, names: %d)", len(inputExchangeNames), len(inputQueueNames))
	}

	// INPUT_COPIES
	inputCopies := make([]int, 0)
	inputCopiesStrings := strings.Split(os.Getenv("INPUT_COPIES"), ",")
	for i, copies := range inputCopiesStrings {
		parsed, err := strconv.Atoi(copies)
		if err != nil {
			return Config{}, fmt.Errorf("the %d'th input copy value is invalid: %v", i, copies)
		}
		inputCopies = append(inputCopies, parsed)
	}

	// OUTPUT_EXCHANGE_NAME
	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return Config{}, fmt.Errorf("no output exchange name was provided")
	}

	// OUTPUT_QUEUE_NAMES
	outputQueueNames := strings.Split(os.Getenv("OUTPUT_QUEUE_NAMES"), ",")
	if len(outputQueueNames) == 0 {
		return Config{}, fmt.Errorf("no output queue names were provided")
	}

	// OUTPUT_COPIES
	outputCopies := make([]int, 0)
	outputCopiesString := strings.Split(os.Getenv("OUTPUT_COPIES"), ",")
	for i, copies := range outputCopiesString {
		parsed, err := strconv.Atoi(copies)
		if err != nil {
			return Config{}, fmt.Errorf("the %d'th input copy value is invalid: %v", i, copies)
		}
		outputCopies = append(outputCopies, parsed)
	}

	// HEALTH_CHECK_PORT
	healthCheckPort, err := strconv.ParseUint(os.Getenv("HEALTH_CHECK_PORT"), 10, 16)
	if err != nil {
		return Config{}, fmt.Errorf("the provided health check port is invalid: %v", err)
	}

	// KEEP_ALIVE_RETRIES
	keepAliveRetries, err := strconv.Atoi(os.Getenv("KEEP_ALIVE_RETRIES"))
	if err != nil {
		return Config{}, fmt.Errorf("the provided keep alive retries value is invalid: %v", err)
	}

	// LOG_LEVEL
	logLevelString := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelString)
	if err != nil {
		logLevel = logging.DEBUG
	}

	return Config{
		Url:                url,
		Host:               host,
		Port:               port,
		Backlog:            backlog,
		Id:                 id,
		InputExchangeNames: inputExchangeNames,
		InputQueueNames:    inputQueueNames,
		InputCopies:        inputCopies,
		OutputExchangeName: outputExchangeName,
		OutputQueueNames:   outputQueueNames,
		OutputCopies:       outputCopies,
		HealthCheckPort:    uint16(healthCheckPort),
		KeepAliveRetries:   keepAliveRetries,
		LogLevel:           logLevel,
	}, nil
}
