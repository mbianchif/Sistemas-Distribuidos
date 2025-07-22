package config

import (
	"analyzer/workers/config"
)

type DividerConfig struct {
	config.Config
}

func Create() (*DividerConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}
	return &DividerConfig{con}, nil
}
