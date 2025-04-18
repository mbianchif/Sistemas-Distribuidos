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

func (w *Sanitize) Run(con *config.SanitizeConfig) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[string]func(*Sanitize, amqp.Delivery) (map[string]string, error){
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}

	for msg := range recvChan {
		responseFieldMap, err := handlers[con.Handler](w, msg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		body := protocol.Encode(responseFieldMap, con.Select)
		outQKey := con.OutputQueueKeys[0] // fanout
		if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
			// log
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

func hasEmptyValues(fields map[string]string) bool {
	for _, value := range fields {
		if value == "" {
			return true
		}
	}
	return false
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

	if hasEmptyValues(fields) {
		return nil, fmt.Errorf("la linea tiene campos vacios")
	}

	return fields, nil
}

func handleRating(w *Sanitize, del amqp.Delivery) (map[string]string, error) {
	return nil, nil
}

func handleCredit(w *Sanitize, del amqp.Delivery) (map[string]string, error) {
	return nil, nil
}
