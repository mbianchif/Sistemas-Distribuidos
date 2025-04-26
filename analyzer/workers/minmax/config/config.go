package config

import (
	"fmt"
	"os"

	"analyzer/workers/config"
)

type MinMaxConfig struct {
	*config.Config
	Key string
}

func Create() (*MinMaxConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	key := os.Getenv("KEY")
	if len(key) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	return &MinMaxConfig{con, key}, nil
}
