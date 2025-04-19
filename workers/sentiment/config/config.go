package config

import (
	"workers/config"
)

type SentimentConfig struct {
	*config.Config
}

func Create() (*SentimentConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	return &SentimentConfig{Config: con}, nil
}
