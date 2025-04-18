package workers

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	inputQueues []string
}

func New(con Config) *Worker {
	return &Worker{
		inputQueues: con.inputQueues,
	}
}

// ...

type Filter struct {
	Worker
}

func FilterNew() Filter {
	return Filter{}
}

func f() {
	filter := FilterNew()

	filter.inputQueues
}

func main() {
	fmt.Println("RabbitMQ in Golang: Getting started tutorial")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	fmt.Println("Successfully connected to RabbitMQ instance")

	ch, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	ch.Close()

	// declaring queue with its properties over the the channel opened
	queue, err := ch.QueueDeclare(
		"testing", // name
		false,     // durable
		false,     // auto delete
		false,     // exclusive
		false,     // no wait
		nil,       // args
	)
	if err != nil {
		panic(err)
	}

	// publishing a message
	err = ch.Publish(
		"",        // exchange
		"testing", // key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test Message"),
		},
	)
	if err != nil {
		panic(err)
	}

	deliveries, err := ch.Consume()
}
