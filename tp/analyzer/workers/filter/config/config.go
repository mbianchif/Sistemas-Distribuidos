package config

import (
	"fmt"
	"os"
	"slices"

	"analyzer/workers/config"
)

type FilterConfig struct {
	config.Config
	Handler string
	Key     string
	Value   string
}

func Create() (*FilterConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	validFilterHandlers := []string{"length", "range", "contains"}
	handler := os.Getenv("HANDLER")
	if !slices.Contains(validFilterHandlers, handler) {
		return nil, fmt.Errorf("no filter handler was provided")
	}

	key := os.Getenv("KEY")
	if len(key) == 0 {
		return nil, fmt.Errorf("no filter key was provided")
	}

	value := os.Getenv("VALUE")
	if len(value) == 0 {
		return nil, fmt.Errorf("no filter value was provided")
	}

	return &FilterConfig{Config: con, Handler: handler, Key: key, Value: value}, nil
}
