package config

import (
	"fmt"
	"os"
	"workers/config"
)

type SinkConfig struct {
	*config.Config
	Query string
}

func Create() (*SinkConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	query := os.Getenv("QUERY")
	if len(query) == 0 {
		return nil, fmt.Errorf("no query was provided")
	}

	return &SinkConfig{Config: con, Query: query}, nil
}
