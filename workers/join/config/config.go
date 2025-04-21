package config

import (
	"workers/config"
)

type JoinConfig struct {
	*config.Config
}

func Create() (*JoinConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	return &JoinConfig{con}, nil
}
