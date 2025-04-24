package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	Id                  int
	Url                 string
	InputExchangeNames  []string
	InputQueueNames     []string
	InputCopies         int
	OutputExchangeName  string
	OutputQueueNames    []string
	OutputCopies        []int
	OutputDeliveryTypes []string
	Select              map[string]struct{}
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
	// ID
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return nil, fmt.Errorf("the given id is invalid: %v", id)
	}

	// RABBIT_URL
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return nil, fmt.Errorf("the rabbitmq url was not provided")
	}

	// INPUT_EXCHANGE_NAMES
	inputExchangeNamesString := os.Getenv("INPUT_EXCHANGE_NAMES")
	inputExchangeNames := strings.Split(inputExchangeNamesString, ",")
	if len(inputExchangeNames) == 0 {
		return nil, fmt.Errorf("the input exchange names were not provided")
	}

	// INPUT_QUEUE_NAMES
	inputQueueNamesString := os.Getenv("INPUT_QUEUE_NAMES")
	inputQueueNames := strings.Split(inputQueueNamesString, ",")
	if len(inputQueueNames) == 0 {
		return nil, fmt.Errorf("the input queue names were not provided")
	}

	if len(inputExchangeNames) != len(inputQueueNames) {
		return nil, fmt.Errorf("the length of input exchange names and input queue names don't match (exchange: %v, queues: %v)", len(inputExchangeNames), len(inputQueueNames))
	}

	// INPUT_COPIES
	inputCopies, err := strconv.Atoi(os.Getenv("INPUT_COPIES"))
	if err != nil {
		return nil, fmt.Errorf("input copy quantity is not a number: %v", inputCopies)
	}

	// OUTPUT_EXCHANGE_NAME
	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return nil, fmt.Errorf("the output exchange name was not provided")
	}

	// OUTPUT_QUEUE_NAMES
	outputQueueNamesString := os.Getenv("OUTPUT_QUEUE_NAMES")
	if len(outputQueueNamesString) == 0 {
		return nil, fmt.Errorf("the output queues were not provided")
	}
	outputQueueNames := strings.Split(outputQueueNamesString, ",")

	// OUTPUT_DELIVERY_TYPES
	outputDeliveryTypes := strings.Split(os.Getenv("OUTPUT_DELIVERY_TYPES"), ",")
	for _, delType := range outputDeliveryTypes {
		if delType == "robin" {
			continue
		}

		shardParts := strings.Split(delType, ":")
		if len(shardParts) == 2 && shardParts[0] == "shard" && len(shardParts[1]) > 0 {
			continue
		}

		return nil, fmt.Errorf("inavlid delivery type %v", delType)
	}

	if len(outputDeliveryTypes) != len(outputQueueNames) {
		return nil, fmt.Errorf("the length of output queue names and output delivery types don't match (names: %v, types: %v)", len(outputQueueNames), len(outputDeliveryTypes))
	}

	// OUTPUT_COPIES
	outputCopiesPerQueueString := os.Getenv("OUTPUT_COPIES")
	outputCopiesPerQueueSlice := strings.Split(outputCopiesPerQueueString, ",")
	outputCopies := make([]int, 0, len(outputCopiesPerQueueSlice))
	for i, copies := range outputCopiesPerQueueSlice {
		n, err := strconv.Atoi(copies)
		if err != nil {
			return nil, fmt.Errorf("%v'nth copy quantity is not a number: %v", i, copies)
		}
		outputCopies = append(outputCopies, n)
	}

	if len(outputCopies) != len(outputQueueNames) {
		return nil, fmt.Errorf("the length of output queue names and output copies per queue don't match (names: %v, copies: %v)", len(outputQueueNames), len(outputCopiesPerQueueSlice))
	}

	// SELECT
	selectString := os.Getenv("SELECT")
	if len(selectString) == 0 {
		return nil, fmt.Errorf("the select were not provided")
	}
	selectMap := make(map[string]struct{})
	for field := range strings.SplitSeq(selectString, ",") {
		selectMap[field] = struct{}{}
	}

	// LOG_LEVEL
	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}
	configLog(logLevel)

	return &Config{
		Id:                  id,
		Url:                 url,
		InputExchangeNames:  inputExchangeNames,
		InputQueueNames:     inputQueueNames,
		InputCopies:         inputCopies,
		OutputExchangeName:  outputExchangeName,
		OutputQueueNames:    outputQueueNames,
		OutputCopies:        outputCopies,
		OutputDeliveryTypes: outputDeliveryTypes,
		Select:              selectMap,
	}, nil
}
