package config

import (
	"fmt"
	"os"
	"workers/config"
)

type FilterConfig struct {
	*config.Config
	FilterType  string
	FilterKey   string
	FilterValue string
}

func Create() (*FilterConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	filterType := os.Getenv("FILTER_TYPE")
	if len(filterType) == 0 {
		return nil, fmt.Errorf("no filterType was provided")
	}

	filterKey := os.Getenv("FILTER_TYPE")
	if len(filterKey) == 0 {
		return nil, fmt.Errorf("no filterType was provided")
	}

	filterValue := os.Getenv("FILTER_TYPE")
	if len(filterValue) == 0 {
		return nil, fmt.Errorf("no filterType was provided")
	}
	return &FilterConfig{Config: con, FilterType: filterType, FilterKey: filterKey, FilterValue: filterValue}, nil
}
