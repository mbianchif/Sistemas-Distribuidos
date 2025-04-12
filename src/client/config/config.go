package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	BatchSize   int
	GatewayHost string
	GatewayPort uint16
	DataPath    string
	Storage     string
}

func Create() (*Config, error) {
	batchSize, err := strconv.Atoi(os.Getenv("BATCHSIZE"))
	if err != nil {
		return nil, err
	}

	gatewayHost := os.Getenv("GATEWAY")
	if len(gatewayHost) == 0 {
		return nil, fmt.Errorf("the gateway host was not provided")
	}

	gatewayPort, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		return nil, err
	}

	dataPath := os.Getenv("DATA_PATH")
	if len(dataPath) == 0 {
		return nil, fmt.Errorf("no data path was provided")
	}

	storage := os.Getenv("STORAGE")
	if len(storage) == 0 {
		return nil, fmt.Errorf("no storage path was proviced")
	}

	return &Config{
		BatchSize:   batchSize,
		GatewayHost: gatewayHost,
		GatewayPort: uint16(gatewayPort),
		DataPath:    dataPath,
		Storage:     storage,
	}, nil
}
