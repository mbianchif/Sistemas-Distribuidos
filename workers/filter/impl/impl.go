package impl

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"workers"
	"workers/filter/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type Filter struct {
	*workers.Worker
}

func New(con *config.FilterConfig) (*Filter, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Filter{base}, nil
}

func (w *Filter) Run(con *config.FilterConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	log.Infof("Running with handler: %v", con.Handler)
	handler := map[string]func(*Filter, map[string]string, *config.FilterConfig) (map[string]string, error){
		"range":    handleRange,
		"contains": handleContains,
		"length":   handleLength,
	}[con.Handler]

	for msg := range recvChan {
		fieldMap, err := protocol.Decode(msg.Body)
		if err != nil {
			log.Errorf("failed to decode message: %v", err)
			msg.Nack(false, false)
			continue
		}

		responseFieldMap, err := handler(w, fieldMap, con)
		if err != nil {
			log.Errorf("failed to handle message: %v", err)
			msg.Nack(false, false)
			continue
		}

		if responseFieldMap != nil {
			log.Debugf("fieldMap: %v", fieldMap)
			body := protocol.Encode(responseFieldMap, con.Select)
			outQKey := con.OutputQueueKeys[0]
			if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
				log.Errorf("failed to publish message: %v", err)
			}
		}

		msg.Ack(false)
	}

	log.Info("recv channel was closed")
	return nil
}

func handleRange(w *Filter, msg map[string]string, con *config.FilterConfig) (map[string]string, error) {
	yearRange, err := parseMathRange(con.Value)
	if err != nil {
		return nil, err
	}

	date, ok := msg[con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", con.Key)
	}

	year, err := strconv.Atoi(strings.Split(date, "-")[0])
	if err != nil {
		return nil, fmt.Errorf("given year is not a number")
	}

	if !yearRange.Contains(year) {
		return nil, nil
	}

	return msg, nil
}

func handleLength(w *Filter, msg map[string]string, con *config.FilterConfig) (map[string]string, error) {
	length, err := strconv.Atoi(con.Value)
	if err != nil {
		return nil, fmt.Errorf("given length is not a number")
	}
	countries, ok := msg[con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", con.Key)
	}
	countryList := strings.Split(countries, ",")
	if len(countryList) != length {
		return nil, nil
	}

	return msg, nil
}

func handleContains(w *Filter, msg map[string]string, con *config.FilterConfig) (map[string]string, error) {
	countries, ok := msg[con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", con.Key)
	}
	countryList := strings.Split(countries, ",")
	countryValueList := strings.Split(con.Value, ",")

	if len(countryValueList) == 0 {
		return nil, fmt.Errorf("value %v is not a list", con.Value)
	}

	for _, country := range countryValueList {
		if !slices.Contains(countryList, country) {
			fmt.Printf("country %v is not in the list %v", country, countryList)
			return nil, nil
		}
	}

	return msg, nil
}
