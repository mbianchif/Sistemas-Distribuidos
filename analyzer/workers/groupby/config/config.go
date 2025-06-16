package config

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"analyzer/workers/config"
)

type GroupByConfig struct {
	config.Config
	GroupKeys  []string
	Aggregator string
	AggKey     string
	Storage    string
}

func Create() (*GroupByConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	groupKeysVar := os.Getenv("GROUP_KEY")
	if len(groupKeysVar) == 0 {
		return nil, fmt.Errorf("no group keys was specified")
	}
	groupKeys := strings.Split(groupKeysVar, ",")

	validAggregators := []string{"count", "sum", "mean"}
	aggregator := os.Getenv("AGGREGATOR")
	if !slices.Contains(validAggregators, aggregator) {
		return nil, fmt.Errorf("the given aggregator is invalid %v", aggregator)
	}

	aggregatorKey := os.Getenv("AGGREGATOR_KEY")
	if len(aggregatorKey) == 0 && aggregator != "count" {
		return nil, fmt.Errorf("no aggregator key was specified")
	}

	storage := os.Getenv("STORAGE")
	if len(storage) == 0 {
		storage = aggregator
	}

	return &GroupByConfig{con, groupKeys, aggregator, aggregatorKey, storage}, nil
}
