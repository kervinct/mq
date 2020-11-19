package main

import (
	"github.com/zvnlanx/mq/infrastructure/mq"
)

var (
	handler mq.MQHandler
)

func main() {
	handler.Object = mq.NewRabbitMQSimple("test")

	handler.Object.PublishSimple("hello world")

	handler.Object.ConsumeSimple()
}
