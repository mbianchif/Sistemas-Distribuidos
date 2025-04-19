package config

import (
	"fmt"
	"os"
	"workers/config"
)

type DividerConfig struct {
	*config.Config
	Handler string
}

func Create() (*DividerConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	handler := os.Getenv("HANDLER")
	if len(handler) == 0 {
		return nil, fmt.Errorf("no handler was provided")
	}

	return &DividerConfig{Config: con, Handler: handler}, nil
}
