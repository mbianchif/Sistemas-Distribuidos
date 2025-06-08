package config

import (
	"fmt"
	"os"

	"analyzer/workers/config"
)

type JoinConfig struct {
	*config.Config
	LeftKey  string
	RightKey string
}

func Create() (*JoinConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	leftKey := os.Getenv("LEFT_KEY")
	if len(leftKey) == 0 {
		return nil, fmt.Errorf("no left key was provided")
	}

	rightKey := os.Getenv("RIGHT_KEY")
	if len(rightKey) == 0 {
		return nil, fmt.Errorf("no right key was provided")
	}

	return &JoinConfig{con, leftKey, rightKey}, nil
}
