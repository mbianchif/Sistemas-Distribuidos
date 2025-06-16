package config

import (
	"fmt"
	"os"
	"strconv"

	"analyzer/workers/config"
)

type SinkConfig struct {
	config.Config
	Query int
}

func Create() (*SinkConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	query, err := strconv.Atoi(os.Getenv("QUERY"))
	if err != nil {
		return nil, fmt.Errorf("no query was provided")
	}

	return &SinkConfig{Config: con, Query: query}, nil
}
