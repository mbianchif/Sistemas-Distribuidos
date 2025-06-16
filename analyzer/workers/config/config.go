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
	Url                   string
	InputExchangeNames    []string
	InputQueueNames       []string
	OutputExchangeName    string
	OutputQueueNames      []string
	OutputDeliveryTypes   []string
	RussianRouletteChance float64
	HealthCheckPort       uint16
	Select                map[string]struct{}
	KeepAliveRetries      int

	// compose
	Id           int
	InputCopies  []int
	OutputCopies []int
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

	// RABBIT_URL
	url := os.Getenv("RABBIT_URL")
	if len(url) == 0 {
		return Config{}, fmt.Errorf("the rabbitmq url was not provided")
	}

	// INPUT_EXCHANGE_NAMES
	inputExchangeNamesString := os.Getenv("INPUT_EXCHANGE_NAMES")
	inputExchangeNames := strings.Split(inputExchangeNamesString, ",")
	if len(inputExchangeNames) == 0 {
		return Config{}, fmt.Errorf("the input exchange names were not provided")
	}

	// INPUT_QUEUE_NAMES
	inputQueueNamesString := os.Getenv("INPUT_QUEUE_NAMES")
	inputQueueNames := strings.Split(inputQueueNamesString, ",")
	if len(inputQueueNames) == 0 {
		return Config{}, fmt.Errorf("the input queue names were not provided")
	}

	if len(inputExchangeNames) != len(inputQueueNames) {
		return Config{}, fmt.Errorf("the length of input exchange names and input queue names don't match (exchange: %v, queues: %v)", len(inputExchangeNames), len(inputQueueNames))
	}

	// INPUT_COPIES
	inputCopiesPerQueueString := os.Getenv("INPUT_COPIES")
	inputCopiesPerQueueSlice := strings.Split(inputCopiesPerQueueString, ",")
	inputCopies := make([]int, 0, len(inputCopiesPerQueueSlice))
	for i, copies := range inputCopiesPerQueueSlice {
		n, err := strconv.Atoi(copies)
		if err != nil {
			return Config{}, fmt.Errorf("%v'nth copy quantity is not a number: %v", i, copies)
		}
		inputCopies = append(inputCopies, n)
	}

	if len(inputCopies) != len(inputQueueNames) {
		return Config{}, fmt.Errorf("the length of input queue names and input copies per queue don't match (names: %v, copies: %v)", len(inputQueueNames), len(inputCopiesPerQueueSlice))
	}

	// OUTPUT_EXCHANGE_NAME
	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if len(outputExchangeName) == 0 {
		return Config{}, fmt.Errorf("the output exchange name was not provided")
	}

	// OUTPUT_QUEUE_NAMES
	outputQueueNamesString := os.Getenv("OUTPUT_QUEUE_NAMES")
	if len(outputQueueNamesString) == 0 {
		return Config{}, fmt.Errorf("the output queues were not provided")
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

		return Config{}, fmt.Errorf("inavlid delivery type %v", delType)
	}

	if len(outputDeliveryTypes) != len(outputQueueNames) {
		return Config{}, fmt.Errorf("the length of output queue names and output delivery types don't match (names: %v, types: %v)", len(outputQueueNames), len(outputDeliveryTypes))
	}

	// OUTPUT_COPIES
	outputCopiesPerQueueString := os.Getenv("OUTPUT_COPIES")
	outputCopiesPerQueueSlice := strings.Split(outputCopiesPerQueueString, ",")
	outputCopies := make([]int, 0, len(outputCopiesPerQueueSlice))
	for i, copies := range outputCopiesPerQueueSlice {
		n, err := strconv.Atoi(copies)
		if err != nil {
			return Config{}, fmt.Errorf("%v'nth copy quantity is not a number: %v", i, copies)
		}
		outputCopies = append(outputCopies, n)
	}

	if len(outputCopies) != len(outputQueueNames) {
		return Config{}, fmt.Errorf("the length of output queue names and output copies per queue don't match (names: %v, copies: %v)", len(outputQueueNames), len(outputCopiesPerQueueSlice))
	}

	// RUSSIAN ROULETTE CHANCE
	russianRouletteChance, err := strconv.ParseFloat(os.Getenv("RUSSIAN_ROULETTE_CHANCE"), 32)
	if err != nil {
		return Config{}, fmt.Errorf("the given russian roulette chance is invalid: %v", err)
	}
	if !(0 <= russianRouletteChance || russianRouletteChance <= 100) {
		return Config{}, fmt.Errorf("russian roulette chance must be between 0 and 100: %f", russianRouletteChance)
	}

	// SELECT
	selectString := os.Getenv("SELECT")
	if len(selectString) == 0 {
		return Config{}, fmt.Errorf("the select were not provided")
	}
	selectMap := make(map[string]struct{})
	for field := range strings.SplitSeq(selectString, ",") {
		selectMap[field] = struct{}{}
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
	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}
	configLog(logLevel)

	return Config{
		Id:                    id,
		Url:                   url,
		InputExchangeNames:    inputExchangeNames,
		InputQueueNames:       inputQueueNames,
		InputCopies:           inputCopies,
		OutputExchangeName:    outputExchangeName,
		OutputQueueNames:      outputQueueNames,
		OutputCopies:          outputCopies,
		OutputDeliveryTypes:   outputDeliveryTypes,
		RussianRouletteChance: russianRouletteChance,
		HealthCheckPort:       uint16(healthCheckPort),
		KeepAliveRetries:      keepAliveRetries,
		Select:                selectMap,
	}, nil
}
