package config

import (
	"fmt"
	"os"
	"slices"
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

	validExchangeTypes := []string{"direct", "fanout", "topic", "headers"}

	inputExchangeType := os.Getenv("INPUT_EXCHANGE_TYPE")
	if !slices.Contains(validExchangeTypes, inputExchangeType) {
		return nil, fmt.Errorf("the input exchange type is invalid: %v", inputExchangeType)
	}

	inputQueueNamesString := os.Getenv("INPUT_QUEUE_NAMES")
	if len(inputQueueNamesString) == 0 {
		return nil, fmt.Errorf("the input queues were not provided")
	}
	inputQueueNames := strings.Split(inputQueueNamesString, ",")

	inputQueueKeysString := os.Getenv("INPUT_QUEUE_KEYS")
	inputQueueKeys := strings.Split(inputQueueKeysString, ",")
	if len(inputQueueNames) != len(inputQueueKeys) {
		return nil, fmt.Errorf("length for input queue names and keys don't match (names: %v, keys: %v)", len(inputQueueNames), len(inputQueueKeys))
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return nil, fmt.Errorf("the output exchange name was not provided")
	}

	outputExchangeType := os.Getenv("OUTPUT_EXCHANGE_TYPE")
	if !slices.Contains(validExchangeTypes, outputExchangeType) {
		return nil, fmt.Errorf("the output exchange type is invalid: %v", outputExchangeType)
	}

	outputQueueNamesString := os.Getenv("OUTPUT_QUEUE_NAMES")
	if len(outputQueueNamesString) == 0 {
		return nil, fmt.Errorf("the output queues were not provided")
	}
	outputQueueNames := strings.Split(outputQueueNamesString, ",")

	outputQueueKeysString := os.Getenv("OUTPUT_QUEUE_KEYS")
	outputQueueKeys := strings.Split(outputQueueKeysString, ",")
	if len(outputQueueNames) != len(outputQueueKeys) {
		return nil, fmt.Errorf("length for output queue names and keys don't match (names: %v, keys: %v)", len(outputQueueNames), len(outputQueueKeys))
	}

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
		InputQueues:        inputQueueNames,
		InputQueueKeys:     inputQueueKeys,
		OutputExchangeName: outputExchangeName,
		OutputExchangeType: outputExchangeType,
		OutputQueues:       outputQueueNames,
		OutputQueueKeys:    outputQueueKeys,
		Select:             selectMap,
	}, nil
}
