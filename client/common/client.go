package common

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
}

type Client struct {
	config ClientConfig
	conn   *BetSockStream
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

func (c *Client) createClientSocket() error {
	conn, err := BetSockConnect(c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClientLoop() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

    c.createClientSocket()
    defer c.conn.Close()

    select {
    case _ = <-sigs:
        return
    default:
        c.createClientSocket()
    }

    msg := Bet{
        Agency:    c.config.ID,
        Name:      os.Getenv("NOMBRE"),
        Surname:   os.Getenv("APELLIDO"),
        Id:        os.Getenv("DOCUMENTO"),
        Birthdate: os.Getenv("NACIMIENTO"),
        Number:    os.Getenv("NUMERO"),
    }

    if err := c.conn.Send(msg); err == nil {
        log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", msg.Id, msg.Number)
    }
}
