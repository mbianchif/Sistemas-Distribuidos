package impl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"workers"
	"workers/protocol"
	"workers/sanitize/config"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/op/go-logging"
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

func (w *Sanitize) Run(con *config.SanitizeConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handler := map[string]func(*Sanitize, amqp.Delivery) (map[string]string, error){
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}[con.Handler]

	log.Infof("Running with handler: %v", con.Handler)
	for msg := range recvChan {
		responseFieldMap, err := handler(w, msg) 
		if err != nil {
			log.Errorf("failed to handle message: %v", err)
			continue
		}

		if responseFieldMap != nil {
			body := protocol.Encode(responseFieldMap, con.Select)
			outQKey := con.OutputQueueKeys[0]
			if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
				log.Errorf("failed to publish message: %v", err)
			}
		}
	}

	return nil
}

func parseNamesFromJson(field string) ([]string, error) {
	type Named struct {
		Name string `json:"name"`
	}

	field = strings.ReplaceAll(field, "'", "\"")

	var values []Named
	if err := json.Unmarshal([]byte(field), &values); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(values))
	for _, named := range values {
		names = append(names, named.Name)
	}

	return names, nil
}

func isValidRow(fields map[string]string) bool {
	for _, value := range fields {
		if len(value) == 0 {
			return false 
		}
	}
	return true 
}

func handleMovie(w *Sanitize, del amqp.Delivery) (map[string]string, error) {
	reader := csv.NewReader(strings.NewReader(string(del.Body)))
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}

	genres, err := parseNamesFromJson(line[3])
	if err != nil {
		return nil, err
	}
	prodCountries, err := parseNamesFromJson(line[13])
	if err != nil {
		return nil, err
	}
	spokLangs, err := parseNamesFromJson(line[17])
	if err != nil {
		return nil, err
	}

	fields := map[string]string{
		"id":                   strings.TrimSpace(line[5]),
		"title":                strings.TrimSpace(line[20]),
		"release_date":         strings.TrimSpace(line[14]),
		"overview":             strings.TrimSpace(line[9]),
		"budget":               strings.TrimSpace(line[2]),
		"revenue":              strings.TrimSpace(line[15]),
		"genres":               strings.Join(genres, ","),
		"production_countries": strings.Join(prodCountries, ","),
		"spoken_languages":     strings.Join(spokLangs, ","),
	}

	if !isValidRow(fields) {
		return nil, nil
	}

	return fields, nil
}

func handleRating(w *Sanitize, del amqp.Delivery) (map[string]string, error) {
	return nil, nil
}

func handleCredit(w *Sanitize, del amqp.Delivery) (map[string]string, error) {
	return nil, nil
}
