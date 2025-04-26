package config

import (
	"fmt"
	"os"
	"slices"

	"analyzer/workers/config"
)

type SanitizeConfig struct {
	*config.Config
	Handler string
}


func Create() (*SanitizeConfig, error) {
	con, err := config.Create()
	if err != nil {
		return nil, err
	}

	handler := os.Getenv("HANDLER")
	if len(handler) == 0 {
		return nil, fmt.Errorf("no handler was provided")
	}

	if !slices.Contains([]string{"movies", "ratings", "credits"}, handler) {
		return nil, fmt.Errorf("invalid handler type")
	}

	return &SanitizeConfig{Config: con, Handler: handler}, nil
}
