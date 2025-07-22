package config

import (
	"analyzer/workers/config"
	"fmt"
	"os"
)

type ExplodeConfig struct {
	config.Config
	Key    string
	Rename string
}

func Create() (*ExplodeConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	key := os.Getenv("KEY")
	if len(key) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	rename := os.Getenv("RENAME")
	if len(rename) == 0 {
		rename = key
	}

	return &ExplodeConfig{con, key, rename}, nil
}
