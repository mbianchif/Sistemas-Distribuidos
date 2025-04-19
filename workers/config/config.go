package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	Url                string
	InputExchangeName  string
	InputExchangeType  string
	InputQueues        []string
	InputQueueKeys     []string
	OutputExchangeName string
	OutputExchangeType string
	OutputQueues       []string
	OutputQueueKeys    []string
	Select             map[string]struct{}
}

func configLog(logLevel logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{time:2006-01-02 15:04:05}	%{level:.4s}	%{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logging.SetLevel(logLevel, "log")
	logging.SetBackend(backendLeveled)
}

func Create() (*Config, error) {
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return nil, fmt.Errorf("the rabbitmq url was not provided")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if len(inputExchangeName) == 0 {
		return nil, fmt.Errorf("the input exchange name was not provided")
	}

	inputExchangeType := os.Getenv("INPUT_EXCHANGE_TYPE")
	if len(inputExchangeType) == 0 {
		return nil, fmt.Errorf("the input exchange name was not provided")
	}

	inputQueuesString := os.Getenv("INPUT_QUEUES")
	if len(inputQueuesString) == 0 {
		return nil, fmt.Errorf("the input queues were not provided")
	}
	inputQueues := strings.Split(inputQueuesString, ",")

	inputQueueKeysString := os.Getenv("INPUT_QUEUES_KEYS")
	inputQueueKeys := strings.Split(inputQueueKeysString, ",")

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return nil, fmt.Errorf("the output exchange name was not provided")
	}

	outputExchangeType := os.Getenv("OUTPUT_EXCHANGE_TYPE")
	if len(outputExchangeName) == 0 {
		return nil, fmt.Errorf("th output exchange type was not provided")
	}

	outputQueuesString := os.Getenv("OUTPUT_QUEUES")
	if len(outputQueuesString) == 0 {
		return nil, fmt.Errorf("the output queues were not provided")
	}
	outputQueues := strings.Split(outputQueuesString, ",")

	outputQueueKeysString := os.Getenv("OUTPUT_QUEUES_KEYS")
	outputQueueKeys := strings.Split(outputQueueKeysString, ",")

	selectString := os.Getenv("SELECT")
	if len(selectString) == 0 {
		return nil, fmt.Errorf("the select were not provided")
	}
	selectMap := make(map[string]struct{})
	for field := range strings.SplitSeq(selectString, ",") {
		selectMap[field] = struct{}{}
	}

	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}
	configLog(logLevel)

	return &Config{
		Url:                url,
		InputExchangeName:  inputExchangeName,
		InputExchangeType:  inputExchangeType,
		InputQueues:        inputQueues,
		InputQueueKeys:     inputQueueKeys,
		OutputExchangeName: outputExchangeName,
		OutputExchangeType: outputExchangeType,
		OutputQueues:       outputQueues,
		OutputQueueKeys:    outputQueueKeys,
		Select:             selectMap,
	}, nil
}
