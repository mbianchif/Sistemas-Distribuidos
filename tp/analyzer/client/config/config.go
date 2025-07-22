package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type Config struct {
	// .env
	BatchSize   int
	GatewayHost string
	GatewayPort uint16
	DataPath    string
	Storage     string
	LogLevel    logging.Level
}

func Create() (Config, error) {
	batchSize, err := strconv.Atoi(os.Getenv("BATCHSIZE"))
	if err != nil {
		return Config{}, err
	}

	gatewayHost := os.Getenv("GATEWAY")
	if len(gatewayHost) == 0 {
		return Config{}, fmt.Errorf("the gateway host was not provided")
	}

	gatewayPort, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		return Config{}, err
	}

	dataPath := os.Getenv("DATA_PATH")
	if len(dataPath) == 0 {
		return Config{}, fmt.Errorf("no data path was provided")
	}

	storage := os.Getenv("STORAGE")
	if len(storage) == 0 {
		return Config{}, fmt.Errorf("no storage path was proviced")
	}

	logLevelVar := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	logLevel, err := logging.LogLevel(logLevelVar)
	if err != nil {
		logLevel = logging.DEBUG
	}

	return Config{
		BatchSize:   batchSize,
		GatewayHost: gatewayHost,
		GatewayPort: uint16(gatewayPort),
		DataPath:    dataPath,
		Storage:     storage,
		LogLevel:    logLevel,
	}, nil
}
