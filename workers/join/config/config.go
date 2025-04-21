package config

import (
	"fmt"
	"os"
	"strconv"
	"workers/config"
)

type JoinConfig struct {
	*config.Config
	LeftKey  string
	RightKey string
	NShards  int
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

	shards, err := strconv.Atoi(os.Getenv("SHARDS"))
	if err != nil {
		return nil, fmt.Errorf("invalid shard value was given")
	}

	return &JoinConfig{con, leftKey, rightKey, shards}, nil
}
