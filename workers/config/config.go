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
	InputExchangeNames []string
	InputExchangeTypes []string
	InputQueueNames    []string
	InputQueueKeys     []string
	OutputExchangeName string
	OutputExchangeType string
	OutputQueueNames   []string
	OutputQueueKeys    []string
	Select             map[string]struct{}
}

func configLog(logLevel logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{time:2006-01-02 15:04:05}	%{level:.4s}	%{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logLevel, "log")
	logging.SetBackend(backendLeveled)
}

func Create() (*Config, error) {
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return nil, fmt.Errorf("the rabbitmq url was not provided")
	}

	inputExchangeNamesString := os.Getenv("INPUT_EXCHANGE_NAMES")
	inputExchangeNames := strings.Split(inputExchangeNamesString, ",")
	if len(inputExchangeNames) == 0 {
		return nil, fmt.Errorf("the input exchange names were not provided")
	}

	validExchangeTypes := []string{"direct", "fanout", "topic", "headers"}
	inputExchangeTypesString := os.Getenv("INPUT_EXCHANGE_TYPES")
	inputExchangeTypes := strings.Split(inputExchangeTypesString, ",")
	for _, t := range inputExchangeTypes {
		if !slices.Contains(validExchangeTypes, t) {
			return nil, fmt.Errorf("the input exchange types is invalid: %v", t)
		}
	}

	if len(inputExchangeNames) != len(inputExchangeTypes) {
		return nil, fmt.Errorf("the length of input exchange names and input exchange types don't match (names: %v, types: %v)", len(inputExchangeNames), len(inputExchangeTypes))
	}

	inputQueueNamesString := os.Getenv("INPUT_QUEUE_NAMES")
	inputQueueNames := strings.Split(inputQueueNamesString, ",")
	if len(inputQueueNames) == 0 {
		return nil, fmt.Errorf("the input queue names were not provided")
	}

	inputQueueKeysString := os.Getenv("INPUT_QUEUE_KEYS")
	inputQueueKeys := strings.Split(inputQueueKeysString, ",")
	if len(inputQueueNames) != len(inputQueueKeys) {
		return nil, fmt.Errorf("the length of input queue names and input queue keys don't match (names: %v, keys: %v)", len(inputQueueNames), len(inputQueueKeys))
	}

	if len(inputExchangeNames) != len(inputQueueNames) {
		return nil, fmt.Errorf("the length of input exchange names and input queue names don't match (exchange: %v, queues: %v)", len(inputExchangeNames), len(inputQueueNames))
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
		return nil, fmt.Errorf("the length of output queue names and ouput keys don't match (names: %v, keys: %v)", len(outputQueueNames), len(outputQueueKeys))
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
		InputExchangeNames: inputExchangeNames,
		InputExchangeTypes: inputExchangeTypes,
		InputQueueNames:    inputQueueNames,
		InputQueueKeys:     inputQueueKeys,
		OutputExchangeName: outputExchangeName,
		OutputExchangeType: outputExchangeType,
		OutputQueueNames:   outputQueueNames,
		OutputQueueKeys:    outputQueueKeys,
		Select:             selectMap,
	}, nil
}
