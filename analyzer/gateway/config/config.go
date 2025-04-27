package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	Url                string
	Host               string
	Port               int
	Backlog            int
	Id                 int
	InputExchangeName  string
	InputQueueName     string
	InputCopies        []int
	OutputExchangeName string
	OutputQueueNames   []string
	OutputCopies       []int
	LogLevel           logging.Level
}

func Create() (*Config, error) {
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return nil, fmt.Errorf("no rabbit url was provided")
	}

	host := os.Getenv("HOST")
	if len(host) == 0 {
		host = "0.0.0.0"
	}

	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 9090
	}

	backlog, err := strconv.Atoi(os.Getenv("BACKLOG"))
	if err != nil {
		return nil, fmt.Errorf("no backlog was provided")
	}

	// ID
	id := 0 // There's only one gateway

	// INPUT_EXCHANGE_NAME
	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if len(inputExchangeName) == 0 {
		return nil, fmt.Errorf("the input exchange name was not provided")
	}

	// INPUT_QUEUE_NAME
	inputQueueName := os.Getenv("INPUT_QUEUE_NAME")
	if len(inputQueueName) == 0 {
		return nil, fmt.Errorf("the input queue names were not provided")
	}

	// INPUT_COPIES
	inputCopies := make([]int, 0)
	inputCopiesStrings := strings.Split(os.Getenv("INPUT_COPIES"), ",")
	for i, copies := range inputCopiesStrings {
		parsed, err := strconv.Atoi(copies)
		if err != nil {
			return nil, fmt.Errorf("the %d'th input copy value is invalid: %v", i, copies)
		}
		inputCopies = append(inputCopies, parsed)
	}

	// OUTPUT_EXCHANGE_NAME
	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return nil, fmt.Errorf("no output exchange name was provided")
	}

	// OUTPUT_QUEUE_NAMES
	outputQueueNames := strings.Split(os.Getenv("OUTPUT_QUEUE_NAMES"), ",")
	if len(outputQueueNames) == 0 {
		return nil, fmt.Errorf("no output queue names were provided")
	}

	// OUTPUT_COPIES
	outputCopies := make([]int, 0)
	outputCopiesString := strings.Split(os.Getenv("OUTPUT_COPIES"), ",")
	for i, copies := range outputCopiesString {
		parsed, err := strconv.Atoi(copies)
		if err != nil {
			return nil, fmt.Errorf("the %d'th input copy value is invalid: %v", i, copies)
		}
		outputCopies = append(outputCopies, parsed)
	}

	logLevelString := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelString)
	if err != nil {
		logLevel = logging.DEBUG
	}

	return &Config{
		Url:                url,
		Host:               host,
		Port:               port,
		Backlog:            backlog,
		Id:                 id,
		InputExchangeName:  inputExchangeName,
		InputQueueName:     inputQueueName,
		InputCopies:        inputCopies,
		OutputExchangeName: outputExchangeName,
		OutputQueueNames:   outputQueueNames,
		OutputCopies:       outputCopies,
		LogLevel:           logLevel,
	}, nil
}
