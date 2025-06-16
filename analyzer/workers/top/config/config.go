package config

import (
	"fmt"
	"os"
	"strconv"

	"analyzer/workers/config"
)

type TopConfig struct {
	config.Config
	Key    string
	Amount int
}

func Create() (*TopConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	key := os.Getenv("KEY")
	if len(key) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	amount := os.Getenv("AMOUNT")
	if len(key) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}
	amountInt, err := strconv.Atoi(amount)
	if err != nil || amountInt <= 0 {
		return nil, fmt.Errorf("invalid amount: %v", err)
	}

	return &TopConfig{con, key, amountInt}, nil
}
