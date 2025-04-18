package main

import (
	"encoding/csv"
	"strings"

	"workers"
	"workers/sanitize/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Sanitize struct {
	*workers.Worker
}

func New(con *config.SanitizeConfig) (*Sanitize, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Sanitize{base}, nil
}

func (w *Sanitize) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[string]func(*Sanitize, amqp.Delivery) error{
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}

	for msg := range recvChan {
		handlers[inputQueue.Name](w, msg)
	}

	return nil
}

func handleMovie(w *Sanitize, del amqp.Delivery) error {
	reader := csv.NewReader(strings.NewReader(string(del.Body)))
	line, err := reader.Read()
	if err != nil {
		return err
	}

	baseFields := map[string]string{
		"id":          line[5],
		"title":       line[20],
		"releaseDate": line[14],
		"overview":    line[9],
		"budget":      line[2],
		"revenue":     line[15],
	}

	genres := line[3]
	productionCountries = line[13]
	spokenLanguages := line[17]

	return nil
}

func handleRating(w *Sanitize, del amqp.Delivery) error {
	return nil
}

func handleCredit(w *Sanitize, del amqp.Delivery) error {
	return nil
}
